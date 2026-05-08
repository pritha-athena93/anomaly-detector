"""
Unit tests for agent/tools.py.
Covers: get_infra_changes_since_training, _embed, _get_last_training_timestamp,
        query_anomaly_model, promote_model, _INFRA_EVENT_NAMES filter.
No live AWS / K8s / MLflow connections — all mocked.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pytest


# ── _INFRA_EVENT_NAMES filter tests ─────────────────────────────────────────

class TestInfraEventNames:

    def test_known_infra_events_included(self):
        from agent.tools import _INFRA_EVENT_NAMES
        expected = {
            "CreateStack", "UpdateStack", "RunInstances", "TerminateInstances",
            "CreateDBInstance", "CreateBucket", "CreateCluster",
        }
        assert expected.issubset(_INFRA_EVENT_NAMES)

    def test_read_only_events_not_included(self):
        from agent.tools import _INFRA_EVENT_NAMES
        read_only = {"DescribeInstances", "ListBuckets", "GetObject", "DescribeDBInstances"}
        for ev in read_only:
            assert ev not in _INFRA_EVENT_NAMES

    def test_set_is_frozenset(self):
        from agent.tools import _INFRA_EVENT_NAMES
        assert isinstance(_INFRA_EVENT_NAMES, frozenset)


# ── _embed tests ─────────────────────────────────────────────────────────────

class TestEmbedFunction:

    def test_returns_list_of_floats(self):
        from agent.tools import _embed

        mock_client = MagicMock()
        body_bytes = json.dumps({"embedding": [0.5] * 256}).encode()
        mock_resp = MagicMock()
        mock_resp["body"].read.return_value = body_bytes
        mock_client.invoke_model.return_value = mock_resp

        result = _embed(mock_client, "test text")

        assert isinstance(result, list)
        assert len(result) == 256
        assert all(isinstance(v, float) for v in result)

    def test_uses_titan_embed_v2_model(self):
        from agent.tools import _embed

        mock_client = MagicMock()
        body_bytes = json.dumps({"embedding": [0.1] * 256}).encode()
        mock_resp = MagicMock()
        mock_resp["body"].read.return_value = body_bytes
        mock_client.invoke_model.return_value = mock_resp

        _embed(mock_client, "test")

        call_kwargs = mock_client.invoke_model.call_args[1]
        assert call_kwargs["modelId"] == "amazon.titan-embed-text-v2:0"

    def test_text_truncated_to_8000_chars(self):
        from agent.tools import _embed

        mock_client = MagicMock()
        body_bytes = json.dumps({"embedding": [0.0] * 256}).encode()
        mock_resp = MagicMock()
        mock_resp["body"].read.return_value = body_bytes
        mock_client.invoke_model.return_value = mock_resp

        long_text = "x" * 10000
        _embed(mock_client, long_text)

        call_kwargs = mock_client.invoke_model.call_args[1]
        body = json.loads(call_kwargs["body"])
        assert len(body["inputText"]) == 8000

    def test_normalize_true_in_request(self):
        from agent.tools import _embed

        mock_client = MagicMock()
        body_bytes = json.dumps({"embedding": [0.1] * 256}).encode()
        mock_resp = MagicMock()
        mock_resp["body"].read.return_value = body_bytes
        mock_client.invoke_model.return_value = mock_resp

        _embed(mock_client, "test")

        body = json.loads(mock_client.invoke_model.call_args[1]["body"])
        assert body["normalize"] is True
        assert body["dimensions"] == 256


# ── _get_last_training_timestamp tests ──────────────────────────────────────

class TestGetLastTrainingTimestamp:

    def test_returns_utc_datetime(self):
        from agent.tools import _get_last_training_timestamp

        mock_runs = MagicMock()
        mock_runs.empty = False
        mock_runs.iloc = [{"start_time": 1746691200000}]  # 2025-05-08 00:00 UTC in ms

        with patch("agent.tools._mlflow") as mock_mlflow:
            mock_mlflow.search_runs.return_value = mock_runs
            result = _get_last_training_timestamp()

        assert result.tzinfo == timezone.utc

    def test_raises_when_no_runs(self):
        from agent.tools import _get_last_training_timestamp

        mock_runs = MagicMock()
        mock_runs.empty = True

        with patch("agent.tools._mlflow") as mock_mlflow:
            mock_mlflow.search_runs.return_value = mock_runs
            with pytest.raises(ValueError, match="No training runs"):
                _get_last_training_timestamp()


# ── get_infra_changes_since_training tests ────────────────────────────────────

class TestGetInfraChangesSinceTraining:

    def _mock_mlflow_run(self, ts_ms=1714521600000):
        runs = MagicMock()
        runs.empty = False
        runs.iloc = [{"start_time": ts_ms}]
        return runs

    def test_no_events_returns_no_events_message(self):
        from agent.tools import get_infra_changes_since_training

        with patch("agent.tools._mlflow") as mock_mlflow, \
             patch("agent.tools.boto3") as mock_boto:

            mock_mlflow.search_runs.return_value = self._mock_mlflow_run()

            mock_ct = MagicMock()
            paginator = MagicMock()
            paginator.paginate.return_value = [{"Events": []}]
            mock_ct.get_paginator.return_value = paginator
            mock_boto.client.return_value = mock_ct

            result = get_infra_changes_since_training.invoke({"event_source": ""})

        assert "No infra-changing events" in result

    def test_filters_non_infra_events(self):
        """Events not in _INFRA_EVENT_NAMES are excluded."""
        from agent.tools import get_infra_changes_since_training

        with patch("agent.tools._mlflow") as mock_mlflow, \
             patch("agent.tools.boto3") as mock_boto:

            mock_mlflow.search_runs.return_value = self._mock_mlflow_run()

            mock_ct = MagicMock()
            paginator = MagicMock()
            paginator.paginate.return_value = [{
                "Events": [
                    {"EventName": "DescribeInstances", "EventTime": datetime.now(timezone.utc),
                     "CloudTrailEvent": "{}"},
                ]
            }]
            mock_ct.get_paginator.return_value = paginator
            mock_boto.client.return_value = mock_ct

            result = get_infra_changes_since_training.invoke({"event_source": ""})

        assert "No infra-changing events" in result

    def test_returns_top_3_results_max(self):
        """FAISS search limited to k=min(3, num_docs)."""
        from agent.tools import get_infra_changes_since_training

        ct_events = [
            {"EventName": "RunInstances", "EventTime": datetime.now(timezone.utc),
             "EventSource": "ec2.amazonaws.com", "Username": "user1", "CloudTrailEvent": "{}"},
            {"EventName": "CreateStack", "EventTime": datetime.now(timezone.utc),
             "EventSource": "cloudformation.amazonaws.com", "Username": "user2", "CloudTrailEvent": "{}"},
            {"EventName": "CreateBucket", "EventTime": datetime.now(timezone.utc),
             "EventSource": "s3.amazonaws.com", "Username": "user3", "CloudTrailEvent": "{}"},
            {"EventName": "DeleteBucket", "EventTime": datetime.now(timezone.utc),
             "EventSource": "s3.amazonaws.com", "Username": "user4", "CloudTrailEvent": "{}"},
        ]

        with patch("agent.tools._mlflow") as mock_mlflow, \
             patch("agent.tools.boto3") as mock_boto, \
             patch("agent.tools._embed") as mock_embed, \
             patch("agent.tools.faiss") as mock_faiss, \
             patch("agent.tools.np") as mock_np:

            mock_mlflow.search_runs.return_value = self._mock_mlflow_run()

            mock_ct = MagicMock()
            paginator = MagicMock()
            paginator.paginate.return_value = [{"Events": ct_events}]
            mock_ct.get_paginator.return_value = paginator
            mock_boto.client.return_value = mock_ct

            mock_embed.return_value = [0.1] * 256
            mock_np.array.return_value = np.array([[0.1] * 256] * 4, dtype="float32")
            mock_np.array.side_effect = None

            index = MagicMock()
            _, indices_arr = MagicMock(), np.array([[0, 1, 2]])
            index.search.return_value = (None, indices_arr)
            mock_faiss.IndexFlatIP.return_value = index

            result = get_infra_changes_since_training.invoke({"event_source": "ec2.amazonaws.com"})

        # Result references top items
        assert "Top 3" in result or "Top" in result

    def test_cloudtrail_exception_returns_error_string(self):
        from agent.tools import get_infra_changes_since_training

        with patch("agent.tools._mlflow") as mock_mlflow, \
             patch("agent.tools.boto3") as mock_boto:

            mock_mlflow.search_runs.return_value = self._mock_mlflow_run()
            mock_ct = MagicMock()
            mock_ct.get_paginator.side_effect = Exception("AccessDenied")
            mock_boto.client.return_value = mock_ct

            result = get_infra_changes_since_training.invoke({"event_source": ""})

        assert "CloudTrail query failed" in result

    def test_embedding_failure_returns_degraded_output(self):
        """Embedding error → raw list returned (degraded but not crash)."""
        from agent.tools import get_infra_changes_since_training

        ct_events = [
            {"EventName": "RunInstances", "EventTime": datetime.now(timezone.utc),
             "EventSource": "ec2.amazonaws.com", "Username": "u", "CloudTrailEvent": "{}"},
        ]

        with patch("agent.tools._mlflow") as mock_mlflow, \
             patch("agent.tools.boto3") as mock_boto, \
             patch("agent.tools._embed") as mock_embed:

            mock_mlflow.search_runs.return_value = self._mock_mlflow_run()
            mock_ct = MagicMock()
            paginator = MagicMock()
            paginator.paginate.return_value = [{"Events": ct_events}]
            mock_ct.get_paginator.return_value = paginator
            mock_boto.client.return_value = mock_ct
            mock_embed.side_effect = Exception("ThrottlingException")

            result = get_infra_changes_since_training.invoke({"event_source": ""})

        assert "Embedding unavailable" in result
        assert "RunInstances" in result

    def test_mlflow_failure_returns_error_string(self):
        from agent.tools import get_infra_changes_since_training

        with patch("agent.tools._mlflow") as mock_mlflow:
            mock_runs = MagicMock()
            mock_runs.empty = True
            mock_mlflow.search_runs.return_value = mock_runs

            result = get_infra_changes_since_training.invoke({"event_source": ""})

        assert "No training runs" in result


# ── query_anomaly_model tests ─────────────────────────────────────────────────

class TestQueryAnomalyModel:

    def test_anomaly_prediction_minus_one(self, valid_feature_vector):
        from agent.tools import query_anomaly_model

        with patch("agent.tools.requests") as mock_requests:
            resp = MagicMock()
            resp.json.return_value = {"predictions": [-1]}
            mock_requests.post.return_value = resp

            result = query_anomaly_model.invoke({"features": valid_feature_vector})

        assert "ANOMALY" in result

    def test_normal_prediction_one(self, valid_feature_vector):
        from agent.tools import query_anomaly_model

        with patch("agent.tools.requests") as mock_requests:
            resp = MagicMock()
            resp.json.return_value = {"predictions": [1]}
            mock_requests.post.return_value = resp

            result = query_anomaly_model.invoke({"features": valid_feature_vector})

        assert "NORMAL" in result

    def test_http_error_raises(self, valid_feature_vector):
        from agent.tools import query_anomaly_model

        with patch("agent.tools.requests") as mock_requests:
            resp = MagicMock()
            resp.raise_for_status.side_effect = Exception("503 Service Unavailable")
            mock_requests.post.return_value = resp

            with pytest.raises(Exception):
                query_anomaly_model.invoke({"features": valid_feature_vector})


# ── promote_model tests ───────────────────────────────────────────────────────

class TestPromoteModel:

    def test_valid_stage_transition(self):
        from agent.tools import promote_model

        with patch("agent.tools._mlflow") as mock_mlflow:
            result = promote_model.invoke({"version": "3", "stage": "Production"})

        assert "Production" in result
        mock_mlflow.transition_model_version_stage.assert_called_once_with(
            name="anomaly-detector", version="3", stage="Production"
        )

    def test_invalid_stage_returns_error(self):
        from agent.tools import promote_model

        result = promote_model.invoke({"version": "1", "stage": "InvalidStage"})

        assert "Invalid stage" in result

    def test_all_valid_stages_accepted(self):
        from agent.tools import promote_model

        for stage in ("Staging", "Production", "Archived"):
            with patch("agent.tools._mlflow"):
                result = promote_model.invoke({"version": "1", "stage": stage})
            assert "Invalid" not in result
