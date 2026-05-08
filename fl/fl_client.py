"""
Phase 6: Federated learning client using Flower + IsolationForest.

Federates the IsolationForest decision threshold (offset_) — the per-node threshold
at which a sample is classified as anomaly. FedAvg averages thresholds across nodes.
Each client trains a full IsolationForest locally; only the threshold is shared.

Run:
  python fl/fl_client.py

Env vars:
  DATA_PATH           path to node's parquet file (required)
  NODE_ID             node identifier for MLflow tagging (default: 0)
  FL_SERVER           Flower server address (default: fl-server.federated-learning.svc:8080)
  MLFLOW_TRACKING_URI MLflow server URL (default: http://mlflow.mlflow.svc:5000)
  N_ESTIMATORS        IsolationForest n_estimators (default: 100)
  CONTAMINATION       IsolationForest contamination (default: 0.08)
"""

import os
import numpy as np
import pandas as pd
import mlflow
import flwr as fl
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score, recall_score

DATA_PATH = os.environ.get("DATA_PATH", "")
NODE_ID = int(os.environ.get("NODE_ID", 0))
FL_SERVER = os.environ.get("FL_SERVER", "fl-server.federated-learning.svc:8080")
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc:5000")
N_ESTIMATORS = int(os.environ.get("N_ESTIMATORS", 100))
CONTAMINATION = float(os.environ.get("CONTAMINATION", 0.08))

FEATURE_COLS = [
    "is_error", "is_root", "hour", "day_of_week",
    "is_mgmt_event", "is_read_only",
    "user_agent_len", "req_params_len", "resp_elements_len",
    "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
    "mfa_auth", "is_console", "access_key_len",
    "src_ip_private", "event_version_major",
]


class IsolationForestClient(fl.client.NumPyClient):
    def __init__(self):
        df = pd.read_parquet(os.environ["DATA_PATH"])
        self.X = df[FEATURE_COLS].values.astype(np.float32)
        self.y = df["label"].values
        self.model = IsolationForest(
            n_estimators=N_ESTIMATORS,
            contamination=CONTAMINATION,
            random_state=42,
        )
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment("federated-anomaly-detection")
        print(f"[node-{NODE_ID}] loaded {len(self.X):,} samples from {DATA_PATH}")

    def get_parameters(self, config):
        # Share the decision threshold. Default offset_ before fit is 0.0.
        offset = float(getattr(self.model, "offset_", 0.0))
        return [np.array([offset], dtype=np.float64)]

    def fit(self, parameters, config):
        round_num = int(config.get("round", 1))

        # Apply global threshold from server before retraining (first round: no-op)
        if parameters and hasattr(self.model, "estimators_"):
            self.model.offset_ = float(parameters[0][0])

        self.model.fit(self.X)

        preds = (self.model.predict(self.X) == -1).astype(int)
        f1 = f1_score(self.y, preds, zero_division=0)

        with mlflow.start_run(
            run_name=f"node{NODE_ID}-round{round_num}"
        ):
            mlflow.log_params({"node_id": NODE_ID, "round": round_num})
            mlflow.log_metrics({"f1": f1, "n_samples": len(self.X)})

        print(f"[node-{NODE_ID}] round {round_num} fit — f1={f1:.4f}")
        return self.get_parameters(config), len(self.X), {"f1": f1}

    def evaluate(self, parameters, config):
        if parameters and hasattr(self.model, "estimators_"):
            self.model.offset_ = float(parameters[0][0])

        preds = (self.model.predict(self.X) == -1).astype(int)
        f1 = f1_score(self.y, preds, zero_division=0)
        precision = precision_score(self.y, preds, zero_division=0)
        recall = recall_score(self.y, preds, zero_division=0)

        # 1 - F1 as "loss" (lower is better, consistent with Flower convention)
        loss = float(1.0 - f1)
        return loss, len(self.X), {"f1": f1, "precision": precision, "recall": recall}


def main():
    client = IsolationForestClient()
    fl.client.start_numpy_client(server_address=FL_SERVER, client=client)


if __name__ == "__main__":
    main()
