"""
Phase 7: Katib trial job — IsolationForest hyperparameter trainer.

Katib passes hyperparameters as env vars and parses metric from stdout.
Metric line format must match the metricCollector regex in the Experiment YAML.

Env vars (set by Katib):
  N_ESTIMATORS    int   50–300
  CONTAMINATION   float 0.01–0.15
  MAX_SAMPLES     str   auto | 0.5 | 0.8

Env vars (set manually / from ConfigMap):
  DATA_PATH           path to full processed parquet (default: /data/processed/features.parquet)
  MLFLOW_TRACKING_URI MLflow URL (default: http://mlflow.mlflow.svc:5000)
"""

import hashlib
import json
import os
import sys

import boto3
import mlflow
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

DATA_PATH          = os.environ.get("DATA_PATH", "/data/processed/features.parquet")
MLFLOW_URI         = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc:5000")
MODEL_REGISTRY_BUCKET = os.environ.get("MODEL_REGISTRY_BUCKET", "")
MODEL_VERSION      = os.environ.get("MODEL_VERSION", "")  # set by KFP pipeline step

N_ESTIMATORS = int(os.environ.get("N_ESTIMATORS", 100))
CONTAMINATION = float(os.environ.get("CONTAMINATION", 0.08))
MAX_SAMPLES_RAW = os.environ.get("MAX_SAMPLES", "auto")
MAX_SAMPLES = MAX_SAMPLES_RAW if MAX_SAMPLES_RAW == "auto" else float(MAX_SAMPLES_RAW)

FEATURE_COLS = [
    "is_error", "is_root", "hour", "day_of_week",
    "is_mgmt_event", "is_read_only",
    "user_agent_len", "req_params_len", "resp_elements_len",
    "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
    "mfa_auth", "is_console", "access_key_len",
    "src_ip_private", "event_version_major",
]


def main():
    df = pd.read_parquet(DATA_PATH)
    X = df[FEATURE_COLS].values.astype(np.float32)
    y = df["label"].values

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        max_samples=MAX_SAMPLES,
        random_state=42,
    )
    model.fit(X_train)

    preds = (model.predict(X_val) == -1).astype(int)
    f1 = f1_score(y_val, preds, zero_division=0)
    precision = precision_score(y_val, preds, zero_division=0)
    recall = recall_score(y_val, preds, zero_division=0)

    # Katib parses this line — format must match metricCollector regex
    print(f"f1={f1:.4f}")

    # Log to MLflow
    mlflow.set_tracking_uri(MLFLOW_URI)
    with mlflow.start_run(run_name=f"katib-n{N_ESTIMATORS}-c{CONTAMINATION}-m{MAX_SAMPLES_RAW}"):
        mlflow.log_params(
            {
                "n_estimators": N_ESTIMATORS,
                "contamination": CONTAMINATION,
                "max_samples": MAX_SAMPLES_RAW,
            }
        )
        mlflow.log_metrics({"f1": f1, "precision": precision, "recall": recall})
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name="anomaly-detector",
        )

    # Gap-6 fix (feature schema drift): serialize the canonical feature list
    # as feature_schema.json alongside the model artifact in S3.
    # The poller validates its own feature list against this at startup.
    _write_feature_schema()


def _write_feature_schema() -> None:
    """
    Serialize FEATURE_COLS + hash to feature_schema.json.

    If MODEL_REGISTRY_BUCKET and MODEL_VERSION are set (KFP context),
    upload to s3://<bucket>/anomaly-detector/<version>/feature_schema.json
    and also to latest/ so the poller always gets the current schema.
    Falls back to writing a local file if S3 env vars are absent.
    """
    schema = {
        "version": MODEL_VERSION or "local",
        "features": FEATURE_COLS,
        "count": len(FEATURE_COLS),
        # Hash lets poller detect drift with a single string comparison
        "hash": hashlib.sha256(json.dumps(FEATURE_COLS).encode()).hexdigest(),
    }
    payload = json.dumps(schema, indent=2)

    if MODEL_REGISTRY_BUCKET and MODEL_VERSION:
        s3 = boto3.client("s3")
        for prefix in [f"anomaly-detector/{MODEL_VERSION}", "anomaly-detector/latest"]:
            s3.put_object(
                Bucket=MODEL_REGISTRY_BUCKET,
                Key=f"{prefix}/feature_schema.json",
                Body=payload,
                ContentType="application/json",
            )
        print(f"feature_schema.json uploaded to s3://{MODEL_REGISTRY_BUCKET}")
    else:
        local_path = os.path.join(os.path.dirname(DATA_PATH), "feature_schema.json")
        with open(local_path, "w") as fh:
            fh.write(payload)
        print(f"feature_schema.json written to {local_path}")


if __name__ == "__main__":
    main()
