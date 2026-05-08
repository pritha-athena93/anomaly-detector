"""
Database helpers — RDS Postgres connection + agent_log operations.

DSN fetched from Secrets Manager once per process (cached in module-level var).
Each call to get_db() opens a fresh connection; callers must close/use as context manager.

Gap-1 fix (duplicate Slack): is_already_processed() lets the graph's idempotency
node short-circuit before any side effects on replayed Kafka messages.
"""

import json
import os
from typing import Optional

import boto3
import psycopg2

_DSN: Optional[str] = None


def _resolve_dsn() -> str:
    global _DSN
    if _DSN is not None:
        return _DSN
    # Vault-injected file takes priority; fall back to Secrets Manager.
    vault_file = "/vault/secrets/postgres"
    if os.path.exists(vault_file):
        for line in open(vault_file):
            k, _, v = line.strip().partition("=")
            if k == "DB_DSN":
                _DSN = v
                return _DSN
    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId="anomaly/rds-password"
    )["SecretString"]
    _DSN = json.loads(secret)["dsn"]
    return _DSN


def get_db() -> psycopg2.extensions.connection:
    """Open and return a new psycopg2 connection. Caller owns lifecycle."""
    return psycopg2.connect(_resolve_dsn())


def is_already_processed(event_id: str) -> bool:
    """
    Gap-1 fix: idempotency check before any side effects.

    Returns True if this event_id already has a row in agent_log,
    meaning a previous graph run completed (Kafka offset not yet committed,
    but DB write succeeded). Graph routes to END immediately on True.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM agent_log WHERE event_id = %s LIMIT 1",
                (event_id,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def checkpoint(state: dict) -> None:
    """
    Write final decision to agent_log.

    ON CONFLICT DO NOTHING ensures re-runs on the same event_id are safe
    (e.g., Kafka replay after pod restart between route and checkpoint).
    """
    event = state["kafka_event"]
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_log
                  (kafka_offset, event_id, event_time, anomaly_score,
                   retrieved_ctx, bedrock_prompt, bedrock_response,
                   decision, confidence, pagerduty_id, slack_ts)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    state.get("kafka_offset"),
                    event["eventID"],
                    event["eventTime"],
                    event["score"],
                    json.dumps(state.get("rag_chunks", [])),
                    state.get("prompt", ""),
                    state.get("llm_response", ""),
                    state["decision"],
                    state["confidence"],
                    state.get("pagerduty_id"),
                    state.get("slack_ts"),
                ),
            )
            conn.commit()
    finally:
        conn.close()
