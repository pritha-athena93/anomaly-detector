"""
Unit tests for ml/trainer.py — IsolationForest hyperparameter trainer.
Covers: FEATURE_COLS definition, model training logic, metric output format,
        env var parsing, and edge cases around Katib metric collector.
"""

import io
import os
import sys
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock


EXPECTED_FEATURE_COLS = [
    "is_error", "is_root", "hour", "day_of_week",
    "is_mgmt_event", "is_read_only",
    "user_agent_len", "req_params_len", "resp_elements_len",
    "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
    "mfa_auth", "is_console", "access_key_len",
    "src_ip_private", "event_version_major",
]


@pytest.fixture
def sample_parquet(tmp_path):
    """Minimal parquet with all 18 feature cols + label."""
    n = 200
    data = {col: np.random.rand(n).astype(np.float32) for col in EXPECTED_FEATURE_COLS}
    data["label"] = np.random.randint(0, 2, size=n)
    df = pd.DataFrame(data)
    path = tmp_path / "features.parquet"
    df.to_parquet(path)
    return str(path)


class TestFeatureCols:

    def test_exactly_18_features(self):
        """TS-8-04: feature schema has exactly 18 columns."""
        from ml.trainer import FEATURE_COLS
        assert len(FEATURE_COLS) == 18

    def test_feature_cols_match_spec(self):
        """Feature column names match PRODUCTION_PLAN spec."""
        from ml.trainer import FEATURE_COLS
        assert FEATURE_COLS == EXPECTED_FEATURE_COLS

    def test_no_duplicate_feature_cols(self):
        from ml.trainer import FEATURE_COLS
        assert len(FEATURE_COLS) == len(set(FEATURE_COLS))


class TestEnvVarParsing:

    def test_n_estimators_default(self, monkeypatch):
        monkeypatch.delenv("N_ESTIMATORS", raising=False)
        # Re-import to pick up env
        import importlib
        import ml.trainer as trainer
        importlib.reload(trainer)
        assert trainer.N_ESTIMATORS == 100

    def test_contamination_default(self, monkeypatch):
        monkeypatch.delenv("CONTAMINATION", raising=False)
        import importlib
        import ml.trainer as trainer
        importlib.reload(trainer)
        assert trainer.CONTAMINATION == 0.08

    def test_max_samples_auto_stays_string(self, monkeypatch):
        monkeypatch.setenv("MAX_SAMPLES", "auto")
        import importlib
        import ml.trainer as trainer
        importlib.reload(trainer)
        assert trainer.MAX_SAMPLES == "auto"

    def test_max_samples_float_parsed(self, monkeypatch):
        monkeypatch.setenv("MAX_SAMPLES", "0.5")
        import importlib
        import ml.trainer as trainer
        importlib.reload(trainer)
        assert trainer.MAX_SAMPLES == 0.5


class TestMetricOutput:

    def test_katib_f1_line_printed_to_stdout(self, sample_parquet, monkeypatch, capsys):
        """TS-8-04: Katib metric line format 'f1=X.XXXX' printed to stdout."""
        monkeypatch.setenv("DATA_PATH", sample_parquet)
        monkeypatch.setenv("N_ESTIMATORS", "10")   # small for speed

        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_params"), \
             patch("mlflow.log_metrics"), \
             patch("mlflow.sklearn.log_model"):
            mock_run.return_value.__enter__ = MagicMock(return_value=None)
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            from ml.trainer import main
            main()

        captured = capsys.readouterr()
        assert captured.out.startswith("f1=")
        f1_value = float(captured.out.strip().split("=")[1])
        assert 0.0 <= f1_value <= 1.0

    def test_mlflow_logs_f1_precision_recall(self, sample_parquet, monkeypatch):
        """MLflow log_metrics called with f1, precision, recall keys."""
        monkeypatch.setenv("DATA_PATH", sample_parquet)
        monkeypatch.setenv("N_ESTIMATORS", "10")

        logged_metrics = {}

        def capture_metrics(m):
            logged_metrics.update(m)

        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_params"), \
             patch("mlflow.log_metrics", side_effect=capture_metrics), \
             patch("mlflow.sklearn.log_model"):
            mock_run.return_value.__enter__ = MagicMock(return_value=None)
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            from ml.trainer import main
            main()

        assert "f1" in logged_metrics
        assert "precision" in logged_metrics
        assert "recall" in logged_metrics

    def test_mlflow_logs_hyperparams(self, sample_parquet, monkeypatch):
        """MLflow log_params records n_estimators, contamination, max_samples."""
        monkeypatch.setenv("DATA_PATH", sample_parquet)
        monkeypatch.setenv("N_ESTIMATORS", "50")
        monkeypatch.setenv("CONTAMINATION", "0.05")
        monkeypatch.setenv("MAX_SAMPLES", "auto")

        logged_params = {}

        def capture_params(p):
            logged_params.update(p)

        with patch("mlflow.set_tracking_uri"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_params", side_effect=capture_params), \
             patch("mlflow.log_metrics"), \
             patch("mlflow.sklearn.log_model"):
            mock_run.return_value.__enter__ = MagicMock(return_value=None)
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            from ml.trainer import main
            main()

        assert logged_params["n_estimators"] == 50
        assert logged_params["contamination"] == 0.05
        assert logged_params["max_samples"] == "auto"


class TestModelFit:

    def test_isolation_forest_trains_on_feature_cols(self, sample_parquet, monkeypatch):
        """Model fit called with correct feature subset (not all columns)."""
        monkeypatch.setenv("DATA_PATH", sample_parquet)
        monkeypatch.setenv("N_ESTIMATORS", "5")

        fitted_X = {}

        from sklearn.ensemble import IsolationForest
        original_fit = IsolationForest.fit

        def capture_fit(self, X, *args, **kwargs):
            fitted_X["shape"] = X.shape
            return original_fit(self, X, *args, **kwargs)

        with patch.object(IsolationForest, "fit", capture_fit), \
             patch("mlflow.set_tracking_uri"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_params"), \
             patch("mlflow.log_metrics"), \
             patch("mlflow.sklearn.log_model"):
            mock_run.return_value.__enter__ = MagicMock(return_value=None)
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            from ml.trainer import main
            main()

        assert fitted_X["shape"][1] == 18

    def test_train_test_split_20_percent(self, sample_parquet, monkeypatch):
        """20% test split — 200 samples → 160 train, 40 val (approx)."""
        monkeypatch.setenv("DATA_PATH", sample_parquet)
        monkeypatch.setenv("N_ESTIMATORS", "5")

        fitted_X = {}

        from sklearn.ensemble import IsolationForest
        original_fit = IsolationForest.fit

        def capture_fit(self, X, *args, **kwargs):
            fitted_X["n_train"] = X.shape[0]
            return original_fit(self, X, *args, **kwargs)

        with patch.object(IsolationForest, "fit", capture_fit), \
             patch("mlflow.set_tracking_uri"), \
             patch("mlflow.start_run") as mock_run, \
             patch("mlflow.log_params"), \
             patch("mlflow.log_metrics"), \
             patch("mlflow.sklearn.log_model"):
            mock_run.return_value.__enter__ = MagicMock(return_value=None)
            mock_run.return_value.__exit__ = MagicMock(return_value=False)

            from ml.trainer import main
            main()

        # 80% of 200 = 160
        assert fitted_X["n_train"] == 160

    def test_missing_feature_col_raises(self, tmp_path, monkeypatch):
        """TS-8-EC-04: parquet missing a feature column → KeyError, not silent wrong model."""
        partial_cols = EXPECTED_FEATURE_COLS[:17]   # only 17 cols
        data = {col: [0.0] * 100 for col in partial_cols}
        data["label"] = [0] * 100
        df = pd.DataFrame(data)
        path = tmp_path / "partial.parquet"
        df.to_parquet(path)

        monkeypatch.setenv("DATA_PATH", str(path))

        with pytest.raises(KeyError):
            from ml import trainer
            import importlib
            importlib.reload(trainer)
            trainer.main()

    def test_empty_dataset_raises(self, tmp_path, monkeypatch):
        """Empty parquet raises on train_test_split."""
        data = {col: [] for col in EXPECTED_FEATURE_COLS}
        data["label"] = []
        df = pd.DataFrame(data)
        path = tmp_path / "empty.parquet"
        df.to_parquet(path)

        monkeypatch.setenv("DATA_PATH", str(path))

        with pytest.raises(Exception):
            from ml import trainer
            import importlib
            importlib.reload(trainer)
            trainer.main()
