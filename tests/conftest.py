"""
Shared pytest fixtures and configuration.

Markers:
  unit        — no external services required
  integration — requires live Kafka / Postgres / KServe / Vault in-cluster
  infra       — requires AWS credentials + kubectl access
  e2e         — full CloudTrail → Slack pipeline
  security    — secret / IAM audit (needs kubectl + AWS)

Run subsets:
  pytest -m unit
  pytest -m "integration or unit"
  pytest -m e2e
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest


# ── Environment defaults (override via env vars in CI) ──────────────────────

DB_DSN          = os.environ.get("TEST_DB_DSN",         "postgresql://anomaly_admin:test@localhost:5432/anomaly_db")
KAFKA_BROKERS   = os.environ.get("TEST_KAFKA_BROKERS",  "localhost:9092")
KSERVE_HOST     = os.environ.get("TEST_KSERVE_HOST",    "http://localhost:8080")
VAULT_ADDR      = os.environ.get("TEST_VAULT_ADDR",     "http://localhost:8200")
AWS_REGION      = os.environ.get("TEST_AWS_REGION",     "us-east-1")
SLACK_WEBHOOK   = os.environ.get("TEST_SLACK_WEBHOOK",  "https://hooks.slack.com/test")


# ── Vault secret file fixtures ───────────────────────────────────────────────

@pytest.fixture
def vault_postgres_file(tmp_path):
    """Simulate /vault/secrets/postgres written by Vault Agent Injector."""
    f = tmp_path / "postgres"
    f.write_text("DB_DSN=postgresql://anomaly_admin:secret@rds.host:5432/anomaly_db\n")
    return str(f)


@pytest.fixture
def vault_slack_file(tmp_path):
    f = tmp_path / "slack"
    f.write_text("SLACK_WEBHOOK=https://hooks.slack.com/services/AAA/BBB/CCC\n")
    return str(f)


@pytest.fixture
def vault_kafka_file(tmp_path):
    f = tmp_path / "kafka"
    f.write_text("KAFKA_BROKERS=anomaly-kafka-kafka-bootstrap.kafka.svc:9092\n")
    return str(f)


@pytest.fixture
def vault_secret_dir(tmp_path, vault_postgres_file, vault_slack_file, vault_kafka_file):
    """Directory mimicking /vault/secrets/ with all three secret files."""
    import shutil
    dest = tmp_path / "vault_secrets"
    dest.mkdir()
    for name, src in [("postgres", vault_postgres_file),
                      ("slack",    vault_slack_file),
                      ("kafka",    vault_kafka_file)]:
        shutil.copy(src, dest / name)
    return str(dest)


# ── Sample CloudTrail / Kafka event fixtures ─────────────────────────────────

@pytest.fixture
def sample_cloudtrail_event():
    """Minimal valid CloudTrail event as consumed from Kafka."""
    return {
        "eventID":     "aaa-111-bbb",
        "eventName":   "RunInstances",
        "eventSource": "ec2.amazonaws.com",
        "eventTime":   "2026-05-08T10:00:00Z",
        "awsRegion":   "us-east-1",
        "errorCode":   None,
        "userIdentity": {"arn": "arn:aws:iam::123456789012:user/devops"},
        "score":       -0.25,
    }


@pytest.fixture
def sample_anomaly_state(sample_cloudtrail_event):
    """Minimal AnomalyState for graph node tests."""
    return {
        "kafka_event":   sample_cloudtrail_event,
        "kafka_offset":  42,
        "last_train_ts": "2026-04-01T00:00:00Z",
        "rag_chunks":    [],
        "prompt":        "",
        "llm_response":  "",
        "decision":      "",
        "confidence":    0.0,
        "reasoning":     "",
        "slack_ts":      None,
    }


@pytest.fixture
def sample_rag_chunks():
    return [
        {
            "raw_text":   "RunInstances called during AMI rotation",
            "event_name": "RunInstances",
            "event_time": "2026-04-15T08:00:00Z",
            "resource":   "i-0abc123",
            "principal":  "arn:aws:iam::123456789012:role/deployer",
        }
    ]


# ── Bedrock mock helpers ─────────────────────────────────────────────────────

def make_bedrock_response(decision="genuine_bug", confidence=0.9, reasoning="Test reason"):
    """Build a mock boto3 bedrock invoke_model response."""
    content = json.dumps({
        "decision":   decision,
        "confidence": confidence,
        "reasoning":  reasoning,
    })
    body_bytes = json.dumps({
        "content": [{"text": content}]
    }).encode()
    mock_resp = MagicMock()
    mock_resp["body"].read.return_value = body_bytes
    return mock_resp


@pytest.fixture
def mock_bedrock_genuine():
    """Bedrock client that returns genuine_bug classification."""
    with patch("boto3.client") as mock_boto:
        client = MagicMock()
        client.invoke_model.return_value = make_bedrock_response("genuine_bug", 0.92)
        mock_boto.return_value = client
        yield client


@pytest.fixture
def mock_bedrock_known_change():
    with patch("boto3.client") as mock_boto:
        client = MagicMock()
        client.invoke_model.return_value = make_bedrock_response("known_change", 0.85)
        mock_boto.return_value = client
        yield client


@pytest.fixture
def mock_bedrock_embed():
    """Bedrock client that returns a 1536-dim embedding."""
    with patch("boto3.client") as mock_boto:
        client = MagicMock()
        body_bytes = json.dumps({"embedding": [0.1] * 1536}).encode()
        mock_resp = MagicMock()
        mock_resp["body"].read.return_value = body_bytes
        client.invoke_model.return_value = mock_resp
        mock_boto.return_value = client
        yield client


# ── Postgres / psycopg2 mock ─────────────────────────────────────────────────

@pytest.fixture
def mock_psycopg2_conn():
    """Mock psycopg2 connection + cursor."""
    with patch("psycopg2.connect") as mock_connect:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.description = [
            ("raw_text",), ("event_name",), ("event_time",), ("resource",), ("principal",)
        ]
        cursor.fetchall.return_value = [
            ("RunInstances during rotation", "RunInstances", "2026-04-15T08:00:00+00:00", "i-0abc", "arn:aws:iam::123:role/dep")
        ]
        conn.cursor.return_value = cursor
        mock_connect.return_value = conn
        yield conn, cursor


# ── Slack mock ───────────────────────────────────────────────────────────────

@pytest.fixture
def mock_slack_post():
    with patch("requests.post") as mock_post:
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"x-slack-message-ts": "1234567890.123456"}
        mock_post.return_value = resp
        yield mock_post


# ── 18-feature vector ────────────────────────────────────────────────────────

@pytest.fixture
def valid_feature_vector():
    """18-element float list matching IsolationForest feature schema."""
    return [
        1.0,   # is_error
        0.0,   # is_root
        10.0,  # hour
        2.0,   # day_of_week
        1.0,   # is_mgmt_event
        0.0,   # is_read_only
        20.0,  # user_agent_len
        150.0, # req_params_len
        80.0,  # resp_elements_len
        3.0,   # event_source_enc
        42.0,  # event_name_enc
        1.0,   # region_enc
        2.0,   # identity_type_enc
        0.0,   # mfa_auth
        0.0,   # is_console
        16.0,  # access_key_len
        0.0,   # src_ip_private
        1.0,   # event_version_major
    ]
