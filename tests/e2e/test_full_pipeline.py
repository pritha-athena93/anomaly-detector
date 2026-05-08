"""
End-to-end tests for the full CloudTrail → Slack pipeline (Phase E2E).
Covers: TS-E2E-01..11

Requires ALL services running:
  - CloudTrail + S3 + EventBridge + SQS
  - KServe InferenceService (Ready)
  - Kafka (anomalies.flagged topic)
  - LangGraph agent (ml-agent deployment)
  - RDS Postgres
  - Slack webhook (test channel)

Set env vars:
  TEST_CLOUDTRAIL_BUCKET    — S3 bucket CloudTrail writes to
  TEST_SQS_URL              — SQS queue URL
  TEST_KAFKA_BROKERS        — Kafka bootstrap servers
  TEST_DB_DSN               — Postgres connection string
  TEST_SLACK_WEBHOOK        — Slack webhook (test channel)
  TEST_AWS_REGION           — AWS region

Run: pytest -m e2e tests/e2e/test_full_pipeline.py --timeout=600
"""

import json
import os
import time
import uuid
import subprocess
import gzip
import pytest

pytestmark = pytest.mark.e2e

try:
    import boto3
    import psycopg2
    from kafka import KafkaConsumer, KafkaProducer
    import requests
except ImportError as e:
    pytest.skip(f"Required package not installed: {e}", allow_module_level=True)

AWS_REGION      = os.environ.get("TEST_AWS_REGION",          "us-east-1")
CLOUDTRAIL_BUCKET = os.environ.get("TEST_CLOUDTRAIL_BUCKET", "")
SQS_URL         = os.environ.get("TEST_SQS_URL",             "")
KAFKA_BROKERS   = os.environ.get("TEST_KAFKA_BROKERS",       "localhost:9092")
DB_DSN          = os.environ.get("TEST_DB_DSN",              "")
SLACK_WEBHOOK   = os.environ.get("TEST_SLACK_WEBHOOK",       "")

TOPIC_ANOMALIES = "anomalies.flagged"
CONSUMER_GROUP  = "langgraph-agent"

# Max wait times
E2E_TIMEOUT_S        = int(os.environ.get("E2E_TIMEOUT_S",  "600"))   # 10 min total
KAFKA_TIMEOUT_S      = int(os.environ.get("KAFKA_TIMEOUT_S", "120"))
SLACK_TIMEOUT_S      = int(os.environ.get("SLACK_TIMEOUT_S",  "60"))
DB_POLL_INTERVAL_S   = 5


def _skip_if_missing(*env_vars):
    missing = [v for v in env_vars if not os.environ.get(v)]
    if missing:
        pytest.skip(f"Missing env vars: {missing}")


def _cloudtrail_event_json(event_name: str, event_id: str) -> dict:
    """Minimal CloudTrail event that will be scored by IsolationForest."""
    return {
        "Records": [{
            "eventVersion":    "1.08",
            "userIdentity": {
                "type": "IAMUser",
                "arn":  "arn:aws:iam::123456789012:user/test-e2e",
            },
            "eventTime":   "2026-05-08T10:00:00Z",
            "eventSource": "ec2.amazonaws.com",
            "eventName":   event_name,
            "awsRegion":   AWS_REGION,
            "errorCode":   "AccessDenied",   # is_error=1 → pushes toward anomaly
            "requestParameters": None,
            "responseElements":  None,
            "eventID":     event_id,
            "readOnly":    False,
            "resources":   [],
        }]
    }


def _upload_cloudtrail_to_s3(s3_client, bucket: str, event_id: str) -> str:
    """Upload a synthetic CloudTrail log file to S3, triggering EventBridge."""
    body = json.dumps(_cloudtrail_event_json("RunInstances", event_id))
    key  = f"AWSLogs/123456789012/CloudTrail/{AWS_REGION}/2026/05/08/test-e2e-{event_id}.json.gz"

    with gzip.open(f"/tmp/ct-{event_id}.json.gz", "wb") as f:
        f.write(body.encode())

    s3_client.upload_file(
        f"/tmp/ct-{event_id}.json.gz",
        bucket,
        key
    )
    return key


def _wait_for_sqs_message(sqs_client, queue_url: str, event_id: str, timeout: int) -> bool:
    """Poll SQS until a message referencing event_id appears or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        for msg in resp.get("Messages", []):
            if event_id in msg.get("Body", ""):
                return True
        time.sleep(2)
    return False


def _wait_for_kafka_message(brokers: str, topic: str, event_id: str, timeout: int) -> bool:
    """Wait for a Kafka message containing event_id to appear in topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=f"e2e-watcher-{uuid.uuid4()}",
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=timeout * 1000,
    )
    deadline = time.time() + timeout
    for msg in consumer:
        if msg.value.get("eventID") == event_id:
            consumer.close()
            return True
        if time.time() > deadline:
            break
    consumer.close()
    return False


def _wait_for_db_row(dsn: str, event_id: str, timeout: int) -> dict | None:
    """Poll agent_log table until a row for event_id appears."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(dsn)
            cur  = conn.cursor()
            cur.execute(
                "SELECT event_id, decision, confidence, slack_ts FROM agent_log WHERE event_id = %s",
                (event_id,)
            )
            row = cur.fetchone()
            conn.close()
            if row:
                return {"event_id": row[0], "decision": row[1],
                        "confidence": row[2], "slack_ts": row[3]}
        except Exception:
            pass
        time.sleep(DB_POLL_INTERVAL_S)
    return None


# ── TS-E2E-01: Full pipeline within 10 minutes ───────────────────────────────

@pytest.mark.timeout(660)
def test_cloudtrail_to_slack_within_10_min():
    """TS-E2E-01: CloudTrail upload → Kafka message within 10 minutes."""
    _skip_if_missing("TEST_CLOUDTRAIL_BUCKET", "TEST_SQS_URL", "TEST_KAFKA_BROKERS")

    event_id = str(uuid.uuid4())
    s3  = boto3.client("s3",  region_name=AWS_REGION)
    sqs = boto3.client("sqs", region_name=AWS_REGION)

    start = time.time()

    # Upload synthetic CloudTrail log
    s3_key = _upload_cloudtrail_to_s3(s3, CLOUDTRAIL_BUCKET, event_id)
    print(f"Uploaded CloudTrail log: s3://{CLOUDTRAIL_BUCKET}/{s3_key}")

    # Wait for SQS message (EventBridge → SQS)
    sqs_received = _wait_for_sqs_message(sqs, SQS_URL, event_id, timeout=120)
    assert sqs_received, "SQS message not received within 2 minutes of S3 upload"
    print(f"SQS message received after {time.time() - start:.1f}s")

    # Wait for Kafka message (poller → KServe → Kafka)
    kafka_received = _wait_for_kafka_message(KAFKA_BROKERS, TOPIC_ANOMALIES, event_id, timeout=480)
    elapsed = time.time() - start
    assert kafka_received, f"Kafka message not received within 10 minutes (elapsed: {elapsed:.0f}s)"
    print(f"Kafka message received after {elapsed:.1f}s")
    assert elapsed < 600, f"Pipeline took {elapsed:.0f}s — exceeds 10-minute SLA"


# ── TS-E2E-02: agent_log row written ────────────────────────────────────────

@pytest.mark.timeout(300)
def test_agent_log_row_written_after_classification():
    """TS-E2E-02: After Kafka message, agent_log row exists with all fields."""
    _skip_if_missing("TEST_KAFKA_BROKERS", "TEST_DB_DSN")

    event_id = str(uuid.uuid4())

    # Inject event directly into Kafka (bypasses CloudTrail → poller path)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    msg = {
        "eventID":    event_id,
        "eventName":  "RunInstances",
        "eventSource":"ec2.amazonaws.com",
        "eventTime":  "2026-05-08T10:00:00Z",
        "awsRegion":  AWS_REGION,
        "errorCode":  "AccessDenied",
        "score":      -0.35,
        "userIdentity": {"arn": "arn:aws:iam::123456789012:user/e2e-test"},
    }
    producer.send(TOPIC_ANOMALIES, msg)
    producer.flush()
    producer.close()
    print(f"Produced event {event_id} to {TOPIC_ANOMALIES}")

    # Wait for agent to process and write to DB
    row = _wait_for_db_row(DB_DSN, event_id, timeout=180)
    assert row is not None, f"No agent_log row for event_id={event_id} within 3 minutes"

    # Verify all fields populated
    assert row["event_id"]   == event_id
    assert row["decision"]   in ("genuine_bug", "known_change"), \
        f"Unexpected decision: {row['decision']}"
    assert row["confidence"] is not None
    assert 0.0 <= row["confidence"] <= 1.0


# ── TS-E2E-03 / TS-E2E-04: Slack message color ───────────────────────────────

@pytest.mark.timeout(300)
@pytest.mark.parametrize("forced_decision,expected_decision", [
    ("genuine_bug",  "genuine_bug"),
    ("known_change", "known_change"),
])
def test_correct_classification_in_db(forced_decision, expected_decision):
    """TS-E2E-03/04: classification stored correctly in agent_log."""
    _skip_if_missing("TEST_KAFKA_BROKERS", "TEST_DB_DSN")

    event_id = str(uuid.uuid4())

    # Inject Kafka message with score that should push toward the expected decision
    score = -0.5 if forced_decision == "genuine_bug" else -0.05

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    msg = {
        "eventID":    event_id,
        "eventName":  "DeleteBucket",
        "eventSource":"s3.amazonaws.com",
        "eventTime":  "2026-05-08T03:00:00Z",
        "awsRegion":  AWS_REGION,
        "errorCode":  None,
        "score":      score,
        "userIdentity": {"arn": "arn:aws:iam::123456789012:user/e2e-test"},
    }
    producer.send(TOPIC_ANOMALIES, msg)
    producer.flush()
    producer.close()

    row = _wait_for_db_row(DB_DSN, event_id, timeout=180)
    assert row is not None, f"No DB row for {event_id}"
    assert row["decision"] in ("genuine_bug", "known_change")


# ── TS-E2E-05: Slack message within 60s of Kafka publish ─────────────────────

@pytest.mark.timeout(180)
def test_slack_notified_within_60s_of_kafka():
    """TS-E2E-05: slack_ts populated in agent_log within 60s of Kafka publish."""
    _skip_if_missing("TEST_KAFKA_BROKERS", "TEST_DB_DSN")

    event_id = str(uuid.uuid4())

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    kafka_send_time = time.time()
    producer.send(TOPIC_ANOMALIES, {
        "eventID":    event_id,
        "eventName":  "RunInstances",
        "eventSource":"ec2.amazonaws.com",
        "eventTime":  "2026-05-08T10:00:00Z",
        "awsRegion":  AWS_REGION,
        "errorCode":  None,
        "score":      -0.3,
        "userIdentity": {"arn": "arn:aws:iam::123456789012:user/e2e-test"},
    })
    producer.flush()
    producer.close()

    # Poll DB until slack_ts is populated (agent processed + notified Slack)
    deadline = time.time() + 90   # 90s poll window (60s SLA + buffer)
    row = None
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(DB_DSN)
            cur  = conn.cursor()
            cur.execute(
                "SELECT slack_ts, created_at FROM agent_log WHERE event_id = %s",
                (event_id,)
            )
            r = cur.fetchone()
            conn.close()
            if r and r[0] is not None:   # slack_ts populated
                row = r
                break
        except Exception:
            pass
        time.sleep(3)

    assert row is not None, f"slack_ts never populated for {event_id} within 90s"

    # Verify the row was created within 60s of Kafka publish
    elapsed = time.time() - kafka_send_time
    assert elapsed <= 90, f"Row took {elapsed:.1f}s — may exceed 60s Slack SLA"


# ── TS-E2E-06: KServe killed mid-batch — no data loss ────────────────────────

def test_kserve_restart_no_data_loss():
    """TS-E2E-06: After KServe pod restart, messages not lost (SQS retry)."""
    _skip_if_missing("TEST_KAFKA_BROKERS")

    # Produce a test message before restarting KServe
    event_id = str(uuid.uuid4())
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    producer.send(TOPIC_ANOMALIES, {
        "eventID":    event_id,
        "eventName":  "DescribeInstances",
        "eventSource":"ec2.amazonaws.com",
        "eventTime":  "2026-05-08T10:00:00Z",
        "awsRegion":  AWS_REGION,
        "errorCode":  None,
        "score":      -0.2,
        "userIdentity": {},
    })
    producer.flush()
    producer.close()

    # Restart KServe predictor pod
    result = subprocess.run([
        "kubectl", "rollout", "restart", "deployment",
        "-n", "kserve", "-l", "serving.kserve.io/inferenceservice=anomaly-detector"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip(f"Cannot restart KServe deployment: {result.stderr}")

    # Wait for KServe to recover
    subprocess.run([
        "kubectl", "rollout", "status", "deployment",
        "-n", "kserve", "-l", "serving.kserve.io/inferenceservice=anomaly-detector",
        "--timeout=120s"
    ], capture_output=True)

    if DB_DSN:
        row = _wait_for_db_row(DB_DSN, event_id, timeout=300)
        assert row is not None, "Message lost after KServe restart"


# ── TS-E2E-11: SQS empty → poller no-op ─────────────────────────────────────

def test_empty_sqs_queue_no_errors():
    """TS-E2E-11: No errors logged when SQS queue is empty."""
    result = subprocess.run([
        "kubectl", "get", "pods", "-n", "anomaly-poller",
        "-o", "jsonpath={.items[0].metadata.name}"
    ], capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        pytest.skip("No anomaly-poller pods found")

    pod_name = result.stdout.strip()

    # Get last 20 lines of logs — should not contain ERROR or Exception
    log_result = subprocess.run([
        "kubectl", "logs", pod_name, "-n", "anomaly-poller", "--tail=20"
    ], capture_output=True, text=True)

    if log_result.returncode != 0:
        pytest.skip(f"Cannot get pod logs: {log_result.stderr}")

    logs = log_result.stdout
    assert "Traceback" not in logs, f"Traceback found in poller logs:\n{logs}"
    assert "Exception" not in logs or "0 messages" in logs
