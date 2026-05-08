"""
RAG Indexer — CloudTrail infra-change events → Bedrock Titan embeddings → pgvector.

Called as step 7 of the KFP training pipeline (ml/training_pipeline.py):
    rag = index_rag_chunks(...)
    rag.after(kserve)

What it does:
    1. Fetches CloudTrail log files from S3 written AFTER last_train_ts
    2. Filters to infrastructure-change events (non-read-only, management events)
    3. Builds a human-readable summary per event (raw_text)
    4. Embeds each summary with Bedrock Titan Text Embeddings v2 (1536-dim)
    5. Upserts to rag_chunks table (ON CONFLICT event_id DO UPDATE)

Run:
    python ml/rag_indexer.py \
        --bucket cloudtrail-logs-bucket \
        --prefix AWSLogs/ACCOUNT_ID/CloudTrail/ \
        --since 2026-01-01T00:00:00Z \
        --dsn postgresql://...
"""

import argparse
import gzip
import json
import os
import re
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2.extras import execute_values

# ── AWS clients ───────────────────────────────────────────

s3      = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name=os.environ.get("AWS_REGION", "us-east-1"))

# ── Constants ─────────────────────────────────────────────

EMBED_MODEL_ID = "amazon.titan-embed-text-v2:0"
EMBED_DIM      = 1536
BATCH_SIZE     = 25   # Bedrock rate limit: ~25 embed calls per second safely

# Infrastructure-change event filter:
# Keep management events that are NOT read-only.
# This captures: CreateStack, PutRule, UpdateFunctionConfiguration, etc.
# Excludes: Describe*, List*, Get* which are noise for RAG retrieval.
INFRA_CHANGE_WRITE_VERBS = re.compile(
    r"^(Create|Update|Delete|Put|Attach|Detach|Modify|Run|Start|Stop|"
    r"Terminate|Apply|Enable|Disable|Associate|Disassociate|Register|"
    r"Deregister|Import|Export|Authorize|Revoke|Tag|Untag|"
    r"Set|Reset|Restore|Reboot|Replace|Copy|Move|Rotate|Provision)",
    re.IGNORECASE,
)


def _is_infra_change(event: dict) -> bool:
    """Keep write-type management events."""
    if not event.get("managementEvent", False):
        return False
    if event.get("readOnly", True):
        return False
    event_name = event.get("eventName", "")
    return bool(INFRA_CHANGE_WRITE_VERBS.match(event_name))


def _make_raw_text(event: dict) -> str:
    """Human-readable summary — this is what gets embedded and retrieved by the agent."""
    identity = event.get("userIdentity", {})
    principal = (
        identity.get("arn")
        or identity.get("userName")
        or identity.get("type", "Unknown")
    )
    req = json.dumps(event.get("requestParameters") or {}, default=str)[:500]
    return (
        f"[{event.get('eventTime')}] "
        f"{event.get('eventName')} on {event.get('eventSource', '?')} "
        f"in {event.get('awsRegion', '?')} "
        f"by {principal}. "
        f"Params: {req}"
    )


def embed(text: str) -> list[float]:
    """Bedrock Titan Text Embeddings v2 — 1536-dim normalized vector."""
    resp = bedrock.invoke_model(
        modelId=EMBED_MODEL_ID,
        body=json.dumps({"inputText": text[:8000]}),  # Titan v2 max input
        contentType="application/json",
        accept="application/json",
    )
    return json.loads(resp["body"].read())["embedding"]


def fetch_logs_since(bucket: str, prefix: str, since: str) -> list[dict]:
    """
    List S3 objects under prefix written after `since` timestamp.
    Downloads and decompresses each .json.gz, returns flat list of CloudTrail records.
    """
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    events: list[dict] = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # S3 object LastModified is timezone-aware
            if obj["LastModified"].replace(tzinfo=timezone.utc) <= since_dt:
                continue
            key = obj["Key"]
            if not key.endswith(".json.gz") and not key.endswith(".json"):
                continue

            raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            if key.endswith(".gz"):
                raw = gzip.decompress(raw)
            records = json.loads(raw).get("Records", [])
            events.extend(records)

    return events


def upsert_chunks(conn, rows: list[tuple]) -> int:
    """
    Bulk upsert into rag_chunks. ON CONFLICT event_id DO UPDATE so re-indexing is safe.
    rows: list of (event_id, event_time, event_name, resource, principal, raw_text, embedding_list)
    Returns number of rows inserted/updated.
    """
    if not rows:
        return 0

    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO rag_chunks
            (event_id, event_time, event_name, resource, principal, raw_text, embedding)
        VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            raw_text   = EXCLUDED.raw_text,
            embedding  = EXCLUDED.embedding,
            indexed_at = NOW()
        """,
        rows,
        template=(
            "(%s, %s, %s, %s, %s, %s, %s::vector)"
        ),
    )
    count = cur.rowcount
    conn.commit()
    cur.close()
    return count


def run(bucket: str, prefix: str, since: str, dsn: str) -> None:
    print(f"Fetching CloudTrail logs from s3://{bucket}/{prefix} since {since}")
    all_events = fetch_logs_since(bucket, prefix, since)
    print(f"Total events fetched: {len(all_events)}")

    infra_events = [e for e in all_events if _is_infra_change(e)]
    print(f"Infrastructure-change events to index: {len(infra_events)}")

    if not infra_events:
        print("Nothing to index.")
        return

    conn = psycopg2.connect(dsn)
    total_upserted = 0

    # Process in batches to respect Bedrock rate limits
    for i in range(0, len(infra_events), BATCH_SIZE):
        batch = infra_events[i : i + BATCH_SIZE]
        rows = []
        for event in batch:
            raw_text = _make_raw_text(event)
            try:
                embedding = embed(raw_text)
            except Exception as exc:
                print(f"[WARN] embed failed for {event.get('eventID')}: {exc}")
                continue

            identity = event.get("userIdentity", {})
            resource = (
                event.get("requestParameters", {}) or {}
            )
            # Best-effort resource ARN extraction
            resource_arn = (
                resource.get("resourceArn")
                or resource.get("stackName")
                or resource.get("functionName")
                or resource.get("bucketName")
                or None
            )

            rows.append((
                event.get("eventID"),
                event.get("eventTime"),
                event.get("eventName"),
                resource_arn,
                identity.get("arn") or identity.get("userName"),
                raw_text,
                json.dumps(embedding),  # pgvector accepts JSON array
            ))

        n = upsert_chunks(conn, rows)
        total_upserted += n
        print(f"  Batch {i // BATCH_SIZE + 1}: upserted {n} chunks")

    conn.close()
    print(f"Done. Total upserted: {total_upserted}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket",  required=True, help="CloudTrail S3 bucket name")
    parser.add_argument("--prefix",  default="AWSLogs/", help="S3 key prefix")
    parser.add_argument("--since",   required=True, help="ISO8601 timestamp (last_train_ts)")
    parser.add_argument("--dsn",     default=os.environ.get("DB_DSN"), help="Postgres DSN")
    args = parser.parse_args()

    if not args.dsn:
        raise SystemExit("--dsn or DB_DSN env var required")

    run(args.bucket, args.prefix, args.since, args.dsn)
