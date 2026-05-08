"""
Phase 6: Federated learning server using Flower.

Strategy: FedAvg over the IsolationForest decision threshold (offset_).
Each round:
  1. Broadcast current global threshold to all clients
  2. Each client trains locally, returns updated threshold + local F1
  3. Server averages thresholds, logs per-round metrics to MLflow

Run:
  python fl/fl_server.py

Env vars:
  FL_ROUNDS           number of federated rounds (default: 10)
  FL_MIN_CLIENTS      minimum clients per round (default: 2)
  MLFLOW_TRACKING_URI MLflow server URL (default: http://mlflow.mlflow.svc:5000)
"""

import os
from typing import Optional
import flwr as fl
from flwr.common import Metrics, Parameters, FitIns, FitRes, EvaluateIns, EvaluateRes, Scalar
from flwr.server.client_proxy import ClientProxy
import numpy as np
import mlflow

FL_ROUNDS = int(os.environ.get("FL_ROUNDS", 10))
FL_MIN_CLIENTS = int(os.environ.get("FL_MIN_CLIENTS", 2))
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc:5000")


def weighted_average(metrics: list[tuple[int, Metrics]]) -> Metrics:
    """Aggregate per-client metrics weighted by dataset size."""
    total = sum(n for n, _ in metrics)
    f1 = sum(n * m.get("f1", 0.0) for n, m in metrics) / total if total else 0.0
    precision = sum(n * m.get("precision", 0.0) for n, m in metrics) / total if total else 0.0
    recall = sum(n * m.get("recall", 0.0) for n, m in metrics) / total if total else 0.0
    return {"f1": f1, "precision": precision, "recall": recall}


class AnomalyFedAvg(fl.server.strategy.FedAvg):
    """FedAvg with MLflow logging per round."""

    def __init__(self, mlflow_run_id: str, **kwargs):
        super().__init__(
            evaluate_metrics_aggregation_fn=weighted_average,
            fit_metrics_aggregation_fn=weighted_average,
            **kwargs,
        )
        self._run_id = mlflow_run_id

    def aggregate_fit(
        self,
        server_round: int,
        results: list[tuple[ClientProxy, FitRes]],
        failures: list,
    ):
        aggregated = super().aggregate_fit(server_round, results, failures)
        if aggregated is None:
            return None

        parameters, metrics = aggregated
        with mlflow.start_run(run_id=self._run_id, nested=True):
            mlflow.log_metrics(
                {
                    "train_f1": metrics.get("f1", 0.0),
                    "num_clients": len(results),
                },
                step=server_round,
            )
        print(f"[round {server_round}] fit  — f1={metrics.get('f1', 0):.4f}  clients={len(results)}")
        return aggregated

    def aggregate_evaluate(
        self,
        server_round: int,
        results: list[tuple[ClientProxy, EvaluateRes]],
        failures: list,
    ):
        aggregated = super().aggregate_evaluate(server_round, results, failures)
        if aggregated is None:
            return None

        loss, metrics = aggregated
        with mlflow.start_run(run_id=self._run_id, nested=True):
            mlflow.log_metrics(
                {
                    "eval_f1": metrics.get("f1", 0.0),
                    "eval_precision": metrics.get("precision", 0.0),
                    "eval_recall": metrics.get("recall", 0.0),
                    "eval_loss": loss,
                },
                step=server_round,
            )
        print(
            f"[round {server_round}] eval — f1={metrics.get('f1', 0):.4f}  "
            f"precision={metrics.get('precision', 0):.4f}  recall={metrics.get('recall', 0):.4f}"
        )
        return aggregated


def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("federated-anomaly-detection")

    with mlflow.start_run(run_name="fl-server") as run:
        mlflow.log_params(
            {"rounds": FL_ROUNDS, "min_clients": FL_MIN_CLIENTS, "strategy": "FedAvg"}
        )
        run_id = run.info.run_id

        strategy = AnomalyFedAvg(
            mlflow_run_id=run_id,
            min_fit_clients=FL_MIN_CLIENTS,
            min_evaluate_clients=FL_MIN_CLIENTS,
            min_available_clients=FL_MIN_CLIENTS,
        )

        fl.server.start_server(
            server_address="0.0.0.0:8080",
            config=fl.server.ServerConfig(num_rounds=FL_ROUNDS),
            strategy=strategy,
        )


if __name__ == "__main__":
    main()
