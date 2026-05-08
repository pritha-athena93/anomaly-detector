"""
Agent tools — observe + act on the ML platform, plus RAG-based triage.

All service tools use in-cluster DNS (k8s API, Prometheus, MLflow, KServe).
RAG triage tool uses boto3 directly: CloudTrail for change retrieval,
Bedrock Titan for embeddings, FAISS for similarity search.
"""

import json
import os
from datetime import datetime, timezone

import boto3
import faiss
import mlflow
import numpy as np
import requests
from langchain_core.tools import tool
from kubernetes import client as k8s_client, config as k8s_config
from mlflow import MlflowClient

MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc:5000")
PROMETHEUS_URL = os.environ.get(
    "PROMETHEUS_URL",
    "http://kube-prometheus-stack-prometheus.monitoring.svc:9090",
)
KSERVE_HOST = os.environ.get("KSERVE_HOST", "http://anomaly-detector.kserve.svc")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# In-cluster k8s config; falls back to ~/.kube/config for local dev
try:
    k8s_config.load_incluster_config()
except k8s_config.ConfigException:
    k8s_config.load_kube_config()

_core = k8s_client.CoreV1Api()
_custom = k8s_client.CustomObjectsApi()
mlflow.set_tracking_uri(MLFLOW_URI)
_mlflow = MlflowClient()

# CloudTrail event names that indicate infra changes
_INFRA_EVENT_NAMES = frozenset({
    "CreateStack", "UpdateStack", "DeleteStack",
    "RunInstances", "TerminateInstances", "StopInstances", "StartInstances",
    "CreateSecurityGroup", "DeleteSecurityGroup",
    "AuthorizeSecurityGroupIngress", "RevokeSecurityGroupIngress",
    "CreateLoadBalancer", "DeleteLoadBalancer",
    "CreateDBInstance", "DeleteDBInstance", "ModifyDBInstance",
    "CreateBucket", "DeleteBucket", "PutBucketPolicy", "DeleteBucketPolicy",
    "CreateFunction", "UpdateFunctionCode", "DeleteFunction",
    "CreateCluster", "DeleteCluster",        # EKS
    "UpdateNodegroupConfig", "CreateNodegroup", "DeleteNodegroup",
    "CreateVpc", "DeleteVpc", "CreateSubnet", "DeleteSubnet",
    "CreateInternetGateway", "DeleteInternetGateway",
})


def _prom_query(promql: str) -> dict:
    resp = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": promql},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# ── observe tools ─────────────────────────────────────────

@tool
def get_pod_health(namespace: str) -> str:
    """Return pod name, phase, and ready status for every pod in a namespace."""
    pods = _core.list_namespaced_pod(namespace)
    lines = []
    for pod in pods.items:
        phase = pod.status.phase or "Unknown"
        ready = all(cs.ready for cs in (pod.status.container_statuses or []))
        lines.append(f"{pod.metadata.name}  phase={phase}  ready={ready}")
    return "\n".join(lines) if lines else f"No pods in namespace {namespace!r}"


@tool
def get_cluster_health() -> str:
    """Return current CPU and memory utilisation per node from Prometheus."""
    cpu_data = _prom_query(
        'sum by (node) (rate(node_cpu_seconds_total{mode!="idle"}[5m])) / '
        'sum by (node) (rate(node_cpu_seconds_total[5m]))'
    )
    mem_data = _prom_query(
        '1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)'
    )
    lines = ["CPU utilisation:"]
    for r in cpu_data.get("data", {}).get("result", []):
        lines.append(f"  {r['metric'].get('node', '?')}: {float(r['value'][1]):.1%}")
    lines.append("Memory utilisation:")
    for r in mem_data.get("data", {}).get("result", []):
        lines.append(f"  {r['metric'].get('instance', '?')}: {float(r['value'][1]):.1%}")
    return "\n".join(lines)


@tool
def get_mlflow_experiments(n: int = 5) -> str:
    """Return the last N MLflow runs with their metrics (f1, precision, recall)."""
    runs = _mlflow.search_runs(
        experiment_names=["federated-anomaly-detection"],
        order_by=["start_time DESC"],
        max_results=n,
    )
    if runs.empty:
        return "No runs found in federated-anomaly-detection experiment"
    cols = [c for c in ["run_id", "status", "metrics.f1", "metrics.precision", "metrics.recall"]
            if c in runs.columns]
    return runs[cols].to_string(index=False)


@tool
def get_model_versions() -> str:
    """Return all registered versions of the anomaly-detector model with their stages."""
    try:
        versions = _mlflow.search_model_versions("name='anomaly-detector'")
        if not versions:
            return "No versions registered for anomaly-detector"
        lines = [f"version={v.version}  stage={v.current_stage}  run_id={v.run_id}"
                 for v in versions]
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


@tool
def get_model_latency() -> str:
    """Return KServe inference latency percentiles (P50/P95/P99) from Prometheus."""
    lines = []
    for quantile, label in [(0.5, "P50"), (0.95, "P95"), (0.99, "P99")]:
        data = _prom_query(
            f'histogram_quantile({quantile}, sum by (le) '
            f'(rate(revision_request_latencies_bucket{{namespace="kserve"}}[5m])))'
        )
        results = data.get("data", {}).get("result", [])
        val = float(results[0]["value"][1]) if results else float("nan")
        lines.append(f"  {label}: {val:.1f}ms")
    return "\n".join(lines)


@tool
def query_anomaly_model(features: list[float]) -> str:
    """Run inference on the KServe anomaly-detector endpoint. Pass 18 numeric features as a list."""
    payload = {"instances": [features]}
    resp = requests.post(
        f"{KSERVE_HOST}/v1/models/anomaly-detector:predict",
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    predictions = resp.json().get("predictions", [])
    label = "ANOMALY" if predictions and predictions[0] == -1 else "NORMAL"
    return f"Prediction: {label} (raw={predictions})"


# ── act tools ─────────────────────────────────────────────

@tool
def trigger_training_pipeline(pipeline_name: str = "federated-training") -> str:
    """Trigger a Kubeflow Pipeline run for federated training."""
    import kfp
    client = kfp.Client(host="http://ml-pipeline.kubeflow.svc:8888")
    pipelines = client.list_pipelines().pipelines or []
    match = next((p for p in pipelines if pipeline_name in p.display_name), None)
    if not match:
        return f"Pipeline {pipeline_name!r} not found. Available: {[p.display_name for p in pipelines]}"
    run = client.create_run_from_pipeline_id(
        pipeline_id=match.pipeline_id,
        run_name=f"agent-triggered-{pipeline_name}",
    )
    return f"Triggered pipeline run: {run.run_id}"


@tool
def trigger_katib_hpo() -> str:
    """Submit a new Katib HPO experiment by applying the experiment YAML."""
    import subprocess
    result = subprocess.run(
        ["kubectl", "apply", "-f", "/app/k8s/manifests/katib/experiment.yaml"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return f"Error: {result.stderr}"
    return f"Katib experiment applied: {result.stdout.strip()}"


@tool
def promote_model(version: str, stage: str) -> str:
    """Promote a registered anomaly-detector model version to a stage (Staging or Production)."""
    valid_stages = {"Staging", "Production", "Archived"}
    if stage not in valid_stages:
        return f"Invalid stage {stage!r}. Must be one of {valid_stages}"
    _mlflow.transition_model_version_stage(
        name="anomaly-detector", version=version, stage=stage,
    )
    return f"anomaly-detector v{version} promoted to {stage}"


# ── triage RAG tool ───────────────────────────────────────

def _get_last_training_timestamp() -> datetime:
    """Return UTC datetime of the most recent federated training run."""
    runs = _mlflow.search_runs(
        experiment_names=["federated-anomaly-detection"],
        order_by=["start_time DESC"],
        max_results=1,
    )
    if runs.empty:
        raise ValueError("No training runs in MLflow — cannot anchor infra change window")
    ts_ms = runs.iloc[0]["start_time"]
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def _embed(bedrock_rt: object, text: str) -> list[float]:
    """Embed text with Bedrock Titan Text Embeddings v2 (256-dim, normalised)."""
    resp = bedrock_rt.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": text[:8000], "dimensions": 256, "normalize": True}),
    )
    return json.loads(resp["body"].read())["embedding"]


@tool
def get_infra_changes_since_training(event_source: str = "") -> str:
    """
    Retrieve the most relevant infrastructure changes from CloudTrail
    since the model's last training run, using RAG (FAISS + Bedrock embeddings).

    event_source: CloudTrail eventSource to focus the RAG query on
                  (e.g. 'ec2.amazonaws.com'). Empty string = broad query.

    Returns top-3 relevant infra-changing events as text.
    """
    # 1. Anchor: last training timestamp
    try:
        since_dt = _get_last_training_timestamp()
    except ValueError as e:
        return str(e)

    # 2. Pull write-only CloudTrail events since training
    ct = boto3.client("cloudtrail", region_name=AWS_REGION)
    events = []
    paginator = ct.get_paginator("lookup_events")

    try:
        for page in paginator.paginate(
            StartTime=since_dt,
            LookupAttributes=[{"AttributeKey": "ReadOnly", "AttributeValue": "false"}],
            PaginationConfig={"MaxItems": 500},
        ):
            for ev in page["Events"]:
                if ev.get("EventName") not in _INFRA_EVENT_NAMES:
                    continue
                raw = json.loads(ev.get("CloudTrailEvent", "{}"))
                events.append({
                    "name": ev["EventName"],
                    "time": ev["EventTime"].isoformat(),
                    "source": ev.get("EventSource", ""),
                    "user": ev.get("Username", "unknown"),
                    "region": raw.get("awsRegion", ""),
                    "params": json.dumps(raw.get("requestParameters") or {})[:300],
                })
    except Exception as e:
        return f"CloudTrail query failed: {e}"

    if not events:
        return f"No infra-changing events found since last training ({since_dt.date()})"

    # 3. Embed each event summary with Bedrock Titan
    bedrock_rt = boto3.client("bedrock-runtime", region_name=AWS_REGION)

    docs = [
        f"{e['name']} at {e['time']} by {e['user']} "
        f"via {e['source']} in {e['region']}: {e['params']}"
        for e in events
    ]

    try:
        embeddings = np.array(
            [_embed(bedrock_rt, doc) for doc in docs], dtype="float32"
        )
    except Exception as e:
        # Embedding failed — return raw list (degraded but useful)
        return (
            f"Embedding unavailable ({e}). Raw infra changes since {since_dt.date()}:\n" +
            "\n".join(f"  [{i+1}] {d}" for i, d in enumerate(docs[:10]))
        )

    # 4. FAISS inner-product search (vectors already normalised → cosine similarity)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)

    query_text = (
        f"infrastructure changes affecting {event_source}"
        if event_source else
        "infrastructure changes deployed to cloud environment"
    )
    query_vec = np.array([_embed(bedrock_rt, query_text)], dtype="float32")
    k = min(3, len(docs))
    _, indices = index.search(query_vec, k)

    top_docs = [docs[i] for i in indices[0] if i < len(docs)]

    return (
        f"Top {k} infra changes since last training ({since_dt.date()}):\n" +
        "\n".join(f"  [{i+1}] {d}" for i, d in enumerate(top_docs))
    )


all_tools = [
    get_pod_health,
    get_cluster_health,
    get_mlflow_experiments,
    get_model_versions,
    get_model_latency,
    query_anomaly_model,
    trigger_training_pipeline,
    trigger_katib_hpo,
    promote_model,
    get_infra_changes_since_training,
]
