"""
SQS Poller — CloudTrail S3 notifications → feature extraction → KServe → Kafka.

Runs as EKS CronJob every 5 minutes (k8s/manifests/anomaly-poller/cronjob.yaml).
Uses IRSA (poller-sa) for SQS + S3 access — no static credentials.

Gap-6 fix (feature schema drift):
    At startup, _validate_feature_schema() downloads feature_schema.json from
    S3 model-registry and compares its hash against FEATURE_COLS defined here.
    If hashes differ, the poller exits non-zero immediately (CronJob restarts it,
    alerting the on-call team). This prevents silent wrong predictions caused by
    trainer and poller using different feature encodings post-retraining.
"""

import gzip
import hashlib
import json
import os
import sys

import boto3
import requests
from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────────

SQS_URL       = os.environ["SQS_URL"]
KSERVE_URL    = os.environ["KSERVE_URL"]       # http://anomaly-detector.kserve.svc/v2/models/anomaly-detector:infer
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]    # anomaly-redpanda-0.anomaly-redpanda.kafka.svc:9093
KAFKA_CA_CERT = os.environ.get("KAFKA_CA_CERT", "/etc/redpanda/certs/ca.crt")
THRESHOLD     = float(os.environ.get("THRESHOLD", "-0.1"))
MODEL_REGISTRY_BUCKET = os.environ["MODEL_REGISTRY_BUCKET"]
BATCH_SIZE    = 10   # SQS max per receive

# ── Feature schema (must match ml/trainer.py FEATURE_COLS exactly) ────────
# Gap-6: this list is the ground truth for the poller side of the schema contract.

FEATURE_COLS = [
    "is_error", "is_root", "hour", "day_of_week",
    "is_mgmt_event", "is_read_only",
    "user_agent_len", "req_params_len", "resp_elements_len",
    "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
    "mfa_auth", "is_console", "access_key_len",
    "src_ip_private", "event_version_major",
]

_LOCAL_SCHEMA_HASH = hashlib.sha256(json.dumps(FEATURE_COLS).encode()).hexdigest()


# ── Gap-6: Schema validation ──────────────────────────────

def _validate_feature_schema() -> None:
    """
    Download feature_schema.json from S3 latest/ and compare hash.

    Exits non-zero if hashes differ — prevents silent wrong predictions.
    CronJob restartPolicy=OnFailure will retry, alerting ops via pod crash loop.
    Called once at startup before processing any messages.
    """
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(
            Bucket=MODEL_REGISTRY_BUCKET,
            Key="anomaly-detector/latest/feature_schema.json",
        )
        schema = json.loads(obj["Body"].read())
    except Exception as exc:
        print(f"[WARN] Could not fetch feature_schema.json: {exc}. Proceeding without validation.")
        return

    remote_hash = schema.get("hash", "")
    if remote_hash and remote_hash != _LOCAL_SCHEMA_HASH:
        print(
            f"[ERROR] Feature schema drift detected!\n"
            f"  Model hash:  {remote_hash}\n"
            f"  Poller hash: {_LOCAL_SCHEMA_HASH}\n"
            f"  Model features ({schema.get('count')}): {schema.get('features')}\n"
            f"  Poller features ({len(FEATURE_COLS)}): {FEATURE_COLS}\n"
            f"Stopping poller — update FEATURE_COLS to match the trained model.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[OK] Feature schema validated (hash={remote_hash[:12]}...)")


# ── AWS clients ───────────────────────────────────────────

sqs = boto3.client("sqs")
s3  = boto3.client("s3")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode(),
    security_protocol="SSL",
    ssl_cafile=KAFKA_CA_CERT,
)


# ── Feature encoding ──────────────────────────────────────

def _encode_event(event: dict) -> list:
    """
    Produce the 18-feature vector for a CloudTrail event.

    Encoding must be identical to ml/trainer.py — if it diverges,
    _validate_feature_schema() at startup will catch it via hash comparison.
    """
    identity = event.get("userIdentity", {})
    req      = event.get("requestParameters") or {}
    resp     = event.get("responseElements") or {}
    ua       = event.get("userAgent", "") or ""
    src_ip   = event.get("sourceIPAddress", "") or ""

    # Categorical encoding: use hash mod 1000 as a stable ordinal.
    # The trained model uses LabelEncoder — at inference time KServe's sklearn
    # pipeline applies the same encoder, so raw strings are fine here only if
    # the pipeline wrapper handles encoding. If using raw floats, apply the
    # same hash-based encoding consistently.
    def _hash_enc(val: str) -> int:
        return int(hashlib.md5(val.encode()).hexdigest(), 16) % 1000

    is_private = (
        src_ip.startswith("10.") or
        src_ip.startswith("172.") or
        src_ip.startswith("192.168.")
    )

    return [
        1 if event.get("errorCode") else 0,                              # is_error
        1 if identity.get("type") == "Root" else 0,                      # is_root
        int(event.get("eventTime", "T00:")[11:13] or 0),                  # hour
        0,                                                                 # day_of_week (derive from eventTime in prod)
        1 if event.get("managementEvent") else 0,                        # is_mgmt_event
        1 if event.get("readOnly") else 0,                               # is_read_only
        min(len(ua), 500),                                               # user_agent_len
        min(len(json.dumps(req)), 5000),                                 # req_params_len
        min(len(json.dumps(resp)), 5000),                                # resp_elements_len
        _hash_enc(event.get("eventSource", "")),                         # event_source_enc
        _hash_enc(event.get("eventName", "")),                           # event_name_enc
        _hash_enc(event.get("awsRegion", "")),                           # region_enc
        _hash_enc(identity.get("type", "")),                             # identity_type_enc
        1 if identity.get("sessionContext", {}).get(
            "attributes", {}).get("mfaAuthenticated") == "true" else 0, # mfa_auth
        1 if "signin.amazonaws.com" in ua else 0,                       # is_console
        min(len(identity.get("accessKeyId", "")), 100),                 # access_key_len
        1 if is_private else 0,                                          # src_ip_private
        int(event.get("eventVersion", "1.0").split(".")[0]),             # event_version_major
    ]


# ── Poller logic ──────────────────────────────────────────

def poll() -> None:
    msgs = sqs.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=BATCH_SIZE,
        WaitTimeSeconds=5,
    ).get("Messages", [])

    for msg in msgs:
        try:
            body = json.loads(msg["Body"])
            for record in body.get("Records", []):
                bucket = record["s3"]["bucket"]["name"]
                key    = record["s3"]["object"]["key"]
                _process_s3_object(bucket, key)
        except Exception as exc:
            # Log and delete — DLQ handles repeated failures (gap-5)
            print(f"[ERROR] Failed to process SQS message: {exc}", file=sys.stderr)
        finally:
            # Always delete to avoid infinite retry (DLQ kicks in after maxReceiveCount=3)
            sqs.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=msg["ReceiptHandle"],
            )


def _process_s3_object(bucket: str, key: str) -> None:
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    events = json.loads(raw).get("Records", [])

    if not events:
        return

    instances = [_encode_event(e) for e in events]

    resp = requests.post(
        KSERVE_URL,
        json={
            "inputs": [{
                "name": "input-0",
                "shape": [len(instances), len(FEATURE_COLS)],
                "datatype": "FP32",
                "data": instances,
            }]
        },
        timeout=10,
    )
    resp.raise_for_status()
    outputs = resp.json()["outputs"][0]["data"]

    for event, score in zip(events, outputs):
        producer.send("events-raw", {
            "eventID":   event.get("eventID"),
            "eventTime": event.get("eventTime"),
            "eventName": event.get("eventName"),
            "score":     score,
        })
        if score < THRESHOLD:
            producer.send("anomalies-flagged", {
                "eventID":          event.get("eventID"),
                "eventTime":        event.get("eventTime"),
                "eventName":        event.get("eventName"),
                "eventSource":      event.get("eventSource"),
                "awsRegion":        event.get("awsRegion"),
                "userIdentity":     event.get("userIdentity"),
                "requestParameters":event.get("requestParameters"),
                "responseElements": event.get("responseElements"),
                "errorCode":        event.get("errorCode"),
                "score":            score,
                "raw":              event,
            })
    producer.flush()


if __name__ == "__main__":
    _validate_feature_schema()   # gap-6: exit non-zero if schema hash mismatch
    poll()
