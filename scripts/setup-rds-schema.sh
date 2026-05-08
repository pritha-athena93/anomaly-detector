#!/usr/bin/env bash
# setup-rds-schema.sh — run once after RDS is provisioned (terraform apply).
#
# Creates:
#   - pgvector extension
#   - rag_chunks table  (infra change embeddings for RAG retrieval)
#   - agent_log table   (audit trail of every agent decision)
#
# Requires:
#   psql on PATH
#   DSN from Secrets Manager (or set RDS_DSN manually)
#
# Run from bastion (inside VPC — RDS is not publicly accessible):
#   export RDS_DSN=$(aws secretsmanager get-secret-value \
#     --secret-id anomaly/rds-password \
#     --query 'SecretString' --output text | jq -r .dsn)
#   bash scripts/setup-rds-schema.sh

set -euo pipefail

if [[ -z "${RDS_DSN:-}" ]]; then
  echo "Fetching DSN from Secrets Manager..."
  RDS_DSN=$(aws secretsmanager get-secret-value \
    --secret-id anomaly/rds-password \
    --query 'SecretString' --output text | jq -r .dsn)
fi

echo "==> Connecting to RDS..."
psql "$RDS_DSN" <<'SQL'

-- pgvector extension (requires shared_preload_libraries = pg_vector in parameter group)
CREATE EXTENSION IF NOT EXISTS vector;

-- ── rag_chunks ────────────────────────────────────────────
-- Infra-change CloudTrail events embedded with Bedrock Titan Text v2 (1536-dim).
-- Agent queries this table at runtime to find changes closest to a flagged event.
-- RAG indexer (ml/rag_indexer.py) populates this table after each training run.

CREATE TABLE IF NOT EXISTS rag_chunks (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT UNIQUE NOT NULL,          -- CloudTrail eventID (dedup key)
    event_time  TIMESTAMPTZ NOT NULL,
    event_name  TEXT NOT NULL,                 -- e.g. UpdateStack, PutRule
    resource    TEXT,                          -- ARN or resource name
    principal   TEXT,                          -- IAM principal ARN
    raw_text    TEXT NOT NULL,                 -- human-readable summary sent to embedding model
    embedding   VECTOR(1536) NOT NULL,         -- Bedrock Titan Text Embeddings v2
    indexed_at  TIMESTAMPTZ DEFAULT NOW()
);

-- IVFFlat index: approximate k-NN with cosine similarity.
-- lists=100 is a good starting point for up to ~1M rows.
-- Rebuild (REINDEX) after bulk inserts to improve query speed.
CREATE INDEX IF NOT EXISTS rag_chunks_embedding_idx
    ON rag_chunks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS rag_chunks_event_time_idx
    ON rag_chunks (event_time);

-- ── agent_log ─────────────────────────────────────────────
-- Every decision the agent makes is checkpointed here.
-- Used for: audit, false-positive rate calculation, Grafana dashboard queries.

-- Gap-1 fix: event_id UNIQUE enables ON CONFLICT DO NOTHING in checkpoint(),
-- preventing duplicate agent_log rows on Kafka replay after pod restart.
CREATE TABLE IF NOT EXISTS agent_log (
    id               BIGSERIAL PRIMARY KEY,
    kafka_offset     BIGINT,
    event_id         TEXT NOT NULL UNIQUE,   -- dedup key for idempotency
    event_time       TIMESTAMPTZ NOT NULL,
    anomaly_score    FLOAT NOT NULL,
    retrieved_ctx    JSONB,                    -- top-k RAG chunks used for this decision
    bedrock_prompt   TEXT,
    bedrock_response TEXT,
    decision         TEXT CHECK (decision IN ('genuine_bug', 'known_change')),
    confidence       FLOAT,
    pagerduty_id     TEXT,                     -- PD incident dedup_key if created
    slack_ts         TEXT,                     -- Slack message timestamp
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS agent_log_event_time_idx ON agent_log (event_time);
CREATE INDEX IF NOT EXISTS agent_log_decision_idx   ON agent_log (decision);
CREATE INDEX IF NOT EXISTS agent_log_kafka_offset_idx ON agent_log (kafka_offset);

\echo 'Schema applied successfully.'
\echo 'Tables: rag_chunks, agent_log'
\echo 'Indexes: ivfflat on embedding, btree on event_time and decision'

SQL

echo "==> Done."
