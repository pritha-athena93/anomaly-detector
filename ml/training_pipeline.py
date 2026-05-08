"""
KFP v2 monthly training pipeline — 8 steps, strictly ordered.

Gap-2 fix (last_train_ts atomicity):
    write_train_ts is the LAST step and depends on index_rag (which depends on
    update_kserve). If update_kserve fails, KFP marks it failed and does NOT
    execute index_rag or write_train_ts. SSM /anomaly/last_train_ts is only
    updated when ALL prior steps succeed.

    Dependency chain enforced via .after():
        fetch → features → hpo → train → evaluate → register → update_kserve
            → index_rag → write_train_ts

    If any step raises an exception, KFP sets the run to Failed and stops.
    No downstream step with .after() runs on a failed predecessor.
"""

from kfp import dsl
from kfp.dsl import (
    Dataset, Input, Metrics, Model, Output,
    component, pipeline,
)


# ── Components ────────────────────────────────────────────

@component(
    base_image="python:3.11-slim",
    packages_to_install=["boto3==1.34.144", "pandas==2.2.2", "pyarrow==16.1.0"],
)
def fetch_logs(
    cloudtrail_bucket: str,
    last_train_ts: str,
    output: Output[Dataset],
):
    """Download CloudTrail logs from S3 since last_train_ts into parquet."""
    import boto3, gzip, json, io, pandas as pd
    from datetime import datetime, timezone

    s3 = boto3.client("s3")
    since = datetime.fromisoformat(last_train_ts.replace("Z", "+00:00"))
    records = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=cloudtrail_bucket, Prefix="AWSLogs/"):
        for obj in page.get("Contents", []):
            if obj["LastModified"].replace(tzinfo=timezone.utc) < since:
                continue
            raw = s3.get_object(Bucket=cloudtrail_bucket, Key=obj["Key"])["Body"].read()
            if obj["Key"].endswith(".gz"):
                raw = gzip.decompress(raw)
            records.extend(json.loads(raw).get("Records", []))

    if not records:
        raise ValueError(
            f"No CloudTrail logs found since {last_train_ts}. "
            "Pipeline aborted — cannot train on empty dataset."
        )

    pd.DataFrame(records).to_parquet(output.path, index=False)
    print(f"Fetched {len(records)} CloudTrail events since {last_train_ts}")


@component(
    base_image="python:3.11-slim",
    packages_to_install=["pandas==2.2.2", "pyarrow==16.1.0", "scikit-learn==1.5.1"],
)
def engineer_features(
    raw: Input[Dataset],
    features: Output[Dataset],
):
    """Produce the 18-feature parquet used by trainer + poller. Must stay in sync."""
    import hashlib, json, pandas as pd, numpy as np

    FEATURE_COLS = [
        "is_error", "is_root", "hour", "day_of_week",
        "is_mgmt_event", "is_read_only",
        "user_agent_len", "req_params_len", "resp_elements_len",
        "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
        "mfa_auth", "is_console", "access_key_len",
        "src_ip_private", "event_version_major",
    ]

    df = pd.read_parquet(raw.path)

    def _hash_enc(series):
        return series.fillna("").apply(
            lambda v: int(hashlib.md5(v.encode()).hexdigest(), 16) % 1000
        )

    identity = df["userIdentity"].apply(lambda x: x if isinstance(x, dict) else {})
    out = pd.DataFrame()
    out["is_error"]          = df.get("errorCode", pd.Series([""] * len(df))).notna().astype(int)
    out["is_root"]           = identity.apply(lambda x: 1 if x.get("type") == "Root" else 0)
    out["hour"]              = pd.to_datetime(df["eventTime"], errors="coerce").dt.hour.fillna(0).astype(int)
    out["day_of_week"]       = pd.to_datetime(df["eventTime"], errors="coerce").dt.dayofweek.fillna(0).astype(int)
    out["is_mgmt_event"]     = df.get("managementEvent", False).astype(int)
    out["is_read_only"]      = df.get("readOnly", False).astype(int)
    out["user_agent_len"]    = df.get("userAgent", "").fillna("").str.len().clip(upper=500)
    out["req_params_len"]    = df.get("requestParameters", "").apply(
                                   lambda x: min(len(json.dumps(x) if x else ""), 5000))
    out["resp_elements_len"] = df.get("responseElements", "").apply(
                                   lambda x: min(len(json.dumps(x) if x else ""), 5000))
    out["event_source_enc"]  = _hash_enc(df.get("eventSource", pd.Series([""] * len(df))))
    out["event_name_enc"]    = _hash_enc(df.get("eventName", pd.Series([""] * len(df))))
    out["region_enc"]        = _hash_enc(df.get("awsRegion", pd.Series([""] * len(df))))
    out["identity_type_enc"] = _hash_enc(identity.apply(lambda x: x.get("type", "")))
    out["mfa_auth"]          = identity.apply(
                                   lambda x: 1 if x.get("sessionContext", {}).get(
                                       "attributes", {}).get("mfaAuthenticated") == "true" else 0)
    out["is_console"]        = df.get("userAgent", "").fillna("").str.contains(
                                   "signin.amazonaws.com").astype(int)
    out["access_key_len"]    = identity.apply(lambda x: min(len(x.get("accessKeyId", "")), 100))
    src_ip = df.get("sourceIPAddress", "").fillna("")
    out["src_ip_private"]    = src_ip.apply(
                                   lambda ip: 1 if (ip.startswith("10.") or
                                       ip.startswith("172.") or ip.startswith("192.168.")) else 0)
    out["event_version_major"] = df.get("eventVersion", "1.0").fillna("1.0").apply(
                                     lambda v: int(str(v).split(".")[0]))

    # Synthetic label: errorCode present = anomaly (1), else normal (0)
    out["label"] = out["is_error"]

    assert list(out.columns[:18]) == FEATURE_COLS, "Feature column mismatch — update FEATURE_COLS"
    out.to_parquet(features.path, index=False)
    print(f"Engineered {len(out)} rows × {len(FEATURE_COLS)} features")


@component(
    base_image="python:3.11-slim",
    packages_to_install=["kubeflow-katib==0.17.0", "scikit-learn==1.5.1"],
)
def run_katib_hpo(
    features: Input[Dataset],
    katib_namespace: str,
    best_params: Output[Dataset],
):
    """Submit Katib RandomSearch experiment; block until Succeeded; return best params."""
    import json, time
    from kubeflow.katib import KatibClient

    client = KatibClient(namespace=katib_namespace)

    experiment_name = "anomaly-hpo-pipeline"
    spec = {
        "algorithm": {"algorithmName": "random"},
        "objective": {
            "type": "maximize",
            "goal": 0.95,
            "objectiveMetricName": "f1",
        },
        "maxTrialCount": 15,
        "parallelTrialCount": 3,
        "maxFailedTrialCount": 5,
        "parameters": [
            {"name": "contamination", "parameterType": "double",
             "feasibleSpace": {"min": "0.01", "max": "0.15"}},
            {"name": "n_estimators", "parameterType": "int",
             "feasibleSpace": {"min": "50", "max": "300"}},
            {"name": "max_samples", "parameterType": "categorical",
             "feasibleSpace": {"list": ["auto", "0.5", "0.8"]}},
        ],
        "trialTemplate": {
            "primaryContainerName": "training-container",
            "trialParameters": [
                {"name": "contamination", "reference": "contamination"},
                {"name": "n_estimators", "reference": "n_estimators"},
                {"name": "max_samples", "reference": "max_samples"},
            ],
            "trialSpec": {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{
                                "name": "training-container",
                                "image": "anomaly-trainer:latest",
                                "command": ["python", "-m", "ml.trainer"],
                                "env": [
                                    {"name": "N_ESTIMATORS", "value": "${trialParameters.n_estimators}"},
                                    {"name": "CONTAMINATION",  "value": "${trialParameters.contamination}"},
                                    {"name": "MAX_SAMPLES",    "value": "${trialParameters.max_samples}"},
                                ],
                            }],
                            "restartPolicy": "Never",
                        }
                    }
                }
            }
        }
    }

    client.create_experiment(experiment_spec=spec, experiment_name=experiment_name)

    for _ in range(120):  # wait up to 60 min
        time.sleep(30)
        exp = client.get_experiment(name=experiment_name, namespace=katib_namespace)
        status = exp.get("status", {}).get("conditions", [])
        state = next((c["type"] for c in reversed(status) if c.get("status") == "True"), "")
        if state == "Succeeded":
            break
        if state == "Failed":
            raise RuntimeError(f"Katib experiment {experiment_name} failed")
    else:
        raise TimeoutError("Katib experiment did not complete within 60 minutes")

    trial = exp.get("status", {}).get("currentOptimalTrial")
    if not trial:
        raise ValueError("Katib succeeded but currentOptimalTrial is absent — cannot proceed")

    params = {p["name"]: p["value"] for p in trial["parameterAssignments"]}
    import pandas as pd
    pd.DataFrame([params]).to_parquet(best_params.path, index=False)
    print(f"Best params: {params}")


@component(
    base_image="python:3.11-slim",
    packages_to_install=["scikit-learn==1.5.1", "pandas==2.2.2", "pyarrow==16.1.0", "numpy==1.26.4"],
)
def train_and_evaluate(
    features: Input[Dataset],
    params: Input[Dataset],
    model_artifact: Output[Model],
    metrics: Output[Metrics],
):
    """Train IsolationForest with best Katib params; evaluate; serialize pipeline."""
    import numpy as np, pandas as pd, pickle
    from sklearn.ensemble import IsolationForest
    from sklearn.metrics import f1_score, precision_score, recall_score
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import FunctionTransformer

    feat_df  = pd.read_parquet(features.path)
    param_df = pd.read_parquet(params.path)
    p = param_df.iloc[0].to_dict()

    FEATURE_COLS = [
        "is_error", "is_root", "hour", "day_of_week",
        "is_mgmt_event", "is_read_only",
        "user_agent_len", "req_params_len", "resp_elements_len",
        "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
        "mfa_auth", "is_console", "access_key_len",
        "src_ip_private", "event_version_major",
    ]

    X = feat_df[FEATURE_COLS].values.astype(np.float32)
    y = feat_df["label"].values
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    max_samples = p.get("max_samples", "auto")
    if max_samples != "auto":
        max_samples = float(max_samples)

    clf = IsolationForest(
        n_estimators=int(p.get("n_estimators", 100)),
        contamination=float(p.get("contamination", 0.08)),
        max_samples=max_samples,
        random_state=42,
    )
    clf.fit(X_train)

    preds = (clf.predict(X_val) == -1).astype(int)
    f1 = f1_score(y_val, preds, zero_division=0)
    precision = precision_score(y_val, preds, zero_division=0)
    recall = recall_score(y_val, preds, zero_division=0)

    print(f"f1={f1:.4f} precision={precision:.4f} recall={recall:.4f}")
    metrics.log_metric("f1", f1)
    metrics.log_metric("precision", precision)
    metrics.log_metric("recall", recall)

    with open(model_artifact.path, "wb") as fh:
        pickle.dump(clf, fh)


@component(
    base_image="python:3.11-slim",
    packages_to_install=["boto3==1.34.144", "pandas==2.2.2", "pyarrow==16.1.0"],
)
def register_model(
    model: Input[Model],
    metrics: Input[Metrics],
    s3_bucket: str,
    version: str,
):
    """Upload model.pkl + feature_schema.json to versioned + latest S3 paths."""
    import boto3, hashlib, json, pickle

    FEATURE_COLS = [
        "is_error", "is_root", "hour", "day_of_week",
        "is_mgmt_event", "is_read_only",
        "user_agent_len", "req_params_len", "resp_elements_len",
        "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
        "mfa_auth", "is_console", "access_key_len",
        "src_ip_private", "event_version_major",
    ]

    s3 = boto3.client("s3")
    with open(model.path, "rb") as fh:
        model_bytes = fh.read()

    schema = {
        "version": version,
        "features": FEATURE_COLS,
        "count": len(FEATURE_COLS),
        "hash": hashlib.sha256(json.dumps(FEATURE_COLS).encode()).hexdigest(),
    }

    for prefix in [f"anomaly-detector/{version}", "anomaly-detector/latest"]:
        s3.put_object(Bucket=s3_bucket, Key=f"{prefix}/model.pkl",
                      Body=model_bytes, ContentType="application/octet-stream")
        s3.put_object(Bucket=s3_bucket, Key=f"{prefix}/feature_schema.json",
                      Body=json.dumps(schema, indent=2), ContentType="application/json")
        s3.put_object(Bucket=s3_bucket, Key=f"{prefix}/metadata.json",
                      Body=json.dumps({
                          "version": version,
                          "train_date": __import__("datetime").datetime.utcnow().isoformat(),
                          "f1": metrics.metadata.get("f1"),
                      }), ContentType="application/json")

    print(f"Registered model version {version} to s3://{s3_bucket}/anomaly-detector/{version}/")


@component(
    base_image="python:3.11-slim",
    packages_to_install=["kubernetes==30.1.0"],
)
def update_kserve(
    s3_bucket: str,
    version: str,
):
    """Patch InferenceService storageUri to new version path."""
    from kubernetes import client as k8s, config as k8s_config

    k8s_config.load_incluster_config()
    custom = k8s.CustomObjectsApi()

    storage_uri = f"s3://{s3_bucket}/anomaly-detector/{version}"
    patch = {"spec": {"predictor": {"sklearn": {"storageUri": storage_uri}}}}

    custom.patch_namespaced_custom_object(
        group="serving.kserve.io",
        version="v1beta1",
        namespace="kserve",
        plural="inferenceservices",
        name="anomaly-detector",
        body=patch,
    )
    print(f"KServe storageUri updated to {storage_uri}")


@component(
    base_image="python:3.11-slim",
    packages_to_install=["boto3==1.34.144", "psycopg2-binary==2.9.9"],
)
def index_rag_chunks(
    cloudtrail_bucket: str,
    last_train_ts: str,
):
    """Embed infra-change CloudTrail events since last_train_ts → pgvector upsert."""
    # Imported inline to keep component self-contained
    import boto3, gzip, hashlib, json, psycopg2
    from datetime import datetime, timezone

    bedrock = boto3.client("bedrock-runtime")
    s3 = boto3.client("s3")
    ssm = boto3.client("ssm")

    dsn = json.loads(boto3.client("secretsmanager").get_secret_value(
        SecretId="anomaly/rds-password")["SecretString"])["dsn"]

    INFRA_EVENT_NAMES = {
        "CreateStack", "UpdateStack", "DeleteStack",
        "PutRule", "DeleteRule", "PutTargets",
        "CreateFunction", "UpdateFunctionCode", "DeleteFunction",
        "ModifyDBInstance", "CreateDBInstance", "DeleteDBInstance",
        "PutBucketPolicy", "CreateBucket", "DeleteBucket",
        "RunInstances", "TerminateInstances",
        "CreateSecurityGroup", "AuthorizeSecurityGroupIngress",
        "CreateRole", "AttachRolePolicy", "PutRolePolicy",
    }

    since = datetime.fromisoformat(last_train_ts.replace("Z", "+00:00"))

    paginator = s3.get_paginator("list_objects_v2")
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    for page in paginator.paginate(Bucket=cloudtrail_bucket, Prefix="AWSLogs/"):
        for obj in page.get("Contents", []):
            if obj["LastModified"].replace(tzinfo=timezone.utc) < since:
                continue
            raw = s3.get_object(Bucket=cloudtrail_bucket, Key=obj["Key"])["Body"].read()
            if obj["Key"].endswith(".gz"):
                raw = gzip.decompress(raw)
            for event in json.loads(raw).get("Records", []):
                if event.get("eventName") not in INFRA_EVENT_NAMES:
                    continue
                identity = event.get("userIdentity", {})
                text = (
                    f"{event.get('eventName')} on "
                    f"{json.dumps(event.get('requestParameters', {}))[:200]} "
                    f"by {identity.get('arn','unknown')} at {event.get('eventTime')}"
                )
                try:
                    resp = bedrock.invoke_model(
                        modelId="amazon.titan-embed-text-v2:0",
                        body=json.dumps({"inputText": text, "dimensions": 1536, "normalize": True}),
                    )
                    embedding = json.loads(resp["body"].read())["embedding"]
                except Exception as e:
                    print(f"[WARN] Embed failed for {event.get('eventID')}: {e}")
                    continue

                cur.execute("""
                    INSERT INTO rag_chunks
                      (event_id, event_time, event_name, resource, principal, raw_text, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::vector)
                    ON CONFLICT (event_id) DO NOTHING
                """, (
                    event.get("eventID"),
                    event.get("eventTime"),
                    event.get("eventName"),
                    json.dumps(event.get("requestParameters", {}))[:500],
                    identity.get("arn", ""),
                    text,
                    json.dumps(embedding),
                ))

    conn.commit()
    cur.close()
    conn.close()
    print("RAG index updated")


@component(
    base_image="python:3.11-slim",
    packages_to_install=["boto3==1.34.144"],
)
def write_train_ts():
    """
    Gap-2 fix: write SSM /anomaly/last_train_ts to NOW().

    This is the LAST step and runs ONLY if all prior steps succeeded.
    If update_kserve or index_rag fails, KFP stops the run and this
    component never executes — SSM retains the previous timestamp.
    """
    import boto3
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    boto3.client("ssm").put_parameter(
        Name="/anomaly/last_train_ts",
        Value=ts,
        Type="String",
        Overwrite=True,
    )
    print(f"last_train_ts updated to {ts}")


# ── Pipeline ──────────────────────────────────────────────

@pipeline(
    name="anomaly-detector-training",
    description="Monthly IsolationForest retraining — gap-2: write_ts gated on all steps",
)
def training_pipeline(
    cloudtrail_bucket: str = "anomaly-detector-cloudtrail",
    model_registry_bucket: str = "anomaly-detector-models",
    katib_namespace: str = "kubeflow",
    last_train_ts: str = "1970-01-01T00:00:00Z",  # overridden at runtime from SSM
    version: str = "2026-05",                       # overridden at runtime
):
    fetch    = fetch_logs(
                   cloudtrail_bucket=cloudtrail_bucket,
                   last_train_ts=last_train_ts)

    features = engineer_features(raw=fetch.output)
    features.after(fetch)

    hpo      = run_katib_hpo(
                   features=features.output,
                   katib_namespace=katib_namespace)
    hpo.after(features)

    trained  = train_and_evaluate(
                   features=features.output,
                   params=hpo.output)
    trained.after(hpo)

    reg      = register_model(
                   model=trained.outputs["model_artifact"],
                   metrics=trained.outputs["metrics"],
                   s3_bucket=model_registry_bucket,
                   version=version)
    reg.after(trained)

    kserve   = update_kserve(s3_bucket=model_registry_bucket, version=version)
    kserve.after(reg)                     # ← gap-2: kserve must succeed before rag

    rag      = index_rag_chunks(
                   cloudtrail_bucket=cloudtrail_bucket,
                   last_train_ts=last_train_ts)
    rag.after(kserve)                     # ← gap-2: rag runs only if kserve patched

    ts       = write_train_ts()
    ts.after(rag)                         # ← gap-2: SSM write only if everything succeeded
