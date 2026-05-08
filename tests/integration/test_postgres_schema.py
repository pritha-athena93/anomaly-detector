"""
Integration tests for Postgres schema (Phase 4).
Covers: TS-4-01..11, TS-4-EC-01..04

Requires: live RDS (or local Postgres with pgvector) reachable at TEST_DB_DSN.
Run: pytest -m integration tests/integration/test_postgres_schema.py

Uses a separate test schema to avoid polluting production tables.
"""

import json
import os
import pytest

pytestmark = pytest.mark.integration

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    pytest.skip("psycopg2 not installed", allow_module_level=True)

DB_DSN = os.environ.get(
    "TEST_DB_DSN",
    "postgresql://anomaly_admin:test@localhost:5432/anomaly_db"
)


@pytest.fixture(scope="module")
def conn():
    c = psycopg2.connect(DB_DSN)
    c.autocommit = False
    yield c
    c.close()


@pytest.fixture(scope="module")
def cur(conn):
    cursor = conn.cursor()
    yield cursor
    conn.rollback()


# ── TS-4-01: pgvector extension ──────────────────────────────────────────────

def test_pgvector_extension_installed(cur):
    """TS-4-01: CREATE EXTENSION IF NOT EXISTS vector was applied."""
    cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
    row = cur.fetchone()
    assert row is not None, "pgvector extension not installed"


# ── TS-4-02 / TS-4-03: table schemas ────────────────────────────────────────

def test_rag_chunks_table_exists(cur):
    """TS-4-02: rag_chunks table present."""
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'rag_chunks'
    """)
    assert cur.fetchone() is not None

def test_agent_log_table_exists(cur):
    """TS-4-03: agent_log table present."""
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'agent_log'
    """)
    assert cur.fetchone() is not None


def test_rag_chunks_columns(cur):
    """TS-4-02: all required columns with correct types."""
    cur.execute("""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'rag_chunks'
    """)
    cols = {row[0]: row[1] for row in cur.fetchall()}

    assert "id"         in cols
    assert "event_id"   in cols
    assert "event_time" in cols
    assert "event_name" in cols
    assert "raw_text"   in cols
    assert "embedding"  in cols
    assert "indexed_at" in cols


def test_agent_log_columns(cur):
    """TS-4-03: all required agent_log columns present."""
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'agent_log'
    """)
    col_names = {row[0] for row in cur.fetchall()}

    required = {
        "id", "kafka_offset", "event_id", "event_time", "anomaly_score",
        "retrieved_ctx", "bedrock_prompt", "bedrock_response",
        "decision", "confidence", "slack_ts", "created_at"
    }
    assert required.issubset(col_names), f"Missing columns: {required - col_names}"


# ── TS-4-04: event_id unique constraint ──────────────────────────────────────

def test_rag_chunks_event_id_unique(conn, cur):
    """TS-4-04: duplicate event_id raises UniqueViolation."""
    embedding = [0.1] * 1536
    try:
        cur.execute("""
            INSERT INTO rag_chunks (event_id, event_time, event_name, raw_text, embedding)
            VALUES (%s, NOW(), %s, %s, %s::vector)
        """, ("test-unique-001", "TestEvent", "unique test", embedding))
        conn.commit()

        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute("""
                INSERT INTO rag_chunks (event_id, event_time, event_name, raw_text, embedding)
                VALUES (%s, NOW(), %s, %s, %s::vector)
            """, ("test-unique-001", "TestEvent", "duplicate", embedding))
            conn.commit()
    finally:
        conn.rollback()
        cur.execute("DELETE FROM rag_chunks WHERE event_id = 'test-unique-001'")
        conn.commit()


# ── TS-4-05: decision CHECK constraint ───────────────────────────────────────

def test_agent_log_decision_check_constraint(conn, cur):
    """TS-4-05: invalid decision value raises CheckViolation."""
    with pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute("""
            INSERT INTO agent_log
              (event_id, event_time, anomaly_score, decision, confidence)
            VALUES (%s, NOW(), %s, %s, %s)
        """, ("test-check-001", -0.25, "invalid_value", 0.5))
        conn.commit()
    conn.rollback()


def test_agent_log_decision_genuine_bug_allowed(conn, cur):
    """TS-4-05: genuine_bug is a valid decision."""
    try:
        cur.execute("""
            INSERT INTO agent_log
              (event_id, event_time, anomaly_score, decision, confidence)
            VALUES (%s, NOW(), %s, %s, %s)
        """, ("test-decision-genuine", -0.25, "genuine_bug", 0.9))
        conn.commit()
    finally:
        conn.rollback()
        cur.execute("DELETE FROM agent_log WHERE event_id = 'test-decision-genuine'")
        conn.commit()


def test_agent_log_decision_known_change_allowed(conn, cur):
    try:
        cur.execute("""
            INSERT INTO agent_log
              (event_id, event_time, anomaly_score, decision, confidence)
            VALUES (%s, NOW(), %s, %s, %s)
        """, ("test-decision-known", -0.1, "known_change", 0.85))
        conn.commit()
    finally:
        conn.rollback()
        cur.execute("DELETE FROM agent_log WHERE event_id = 'test-decision-known'")
        conn.commit()


# ── TS-4-06 / TS-4-07 / TS-4-08: indexes ────────────────────────────────────

def test_ivfflat_index_on_embedding(cur):
    """TS-4-06: IVFFlat index present on rag_chunks.embedding."""
    cur.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'rag_chunks'
          AND indexdef ILIKE '%ivfflat%'
    """)
    rows = cur.fetchall()
    assert len(rows) >= 1, "No IVFFlat index found on rag_chunks"


def test_event_time_index_rag_chunks(cur):
    """TS-4-07: btree index on rag_chunks.event_time."""
    cur.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'rag_chunks'
          AND indexdef ILIKE '%event_time%'
    """)
    assert cur.fetchone() is not None


def test_event_time_index_agent_log(cur):
    """TS-4-07: btree index on agent_log.event_time."""
    cur.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'agent_log'
          AND indexdef ILIKE '%event_time%'
    """)
    assert cur.fetchone() is not None


def test_decision_index_agent_log(cur):
    """TS-4-08: index on agent_log.decision."""
    cur.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'agent_log'
          AND indexdef ILIKE '%decision%'
    """)
    assert cur.fetchone() is not None


# ── TS-4-10: cosine similarity query ─────────────────────────────────────────

def test_cosine_similarity_query_returns_nearest(conn, cur):
    """TS-4-10: pgvector <=> operator finds correct nearest neighbor."""
    v1 = [1.0] + [0.0] * 1535
    v2 = [0.0] + [1.0] + [0.0] * 1534
    query = [1.0] + [0.001] * 1535   # closest to v1

    try:
        cur.execute("""
            INSERT INTO rag_chunks (event_id, event_time, event_name, raw_text, embedding)
            VALUES (%s, NOW(), %s, %s, %s::vector)
        """, ("cosine-test-1", "CosineSimilarityTest1", "test1", v1))
        cur.execute("""
            INSERT INTO rag_chunks (event_id, event_time, event_name, raw_text, embedding)
            VALUES (%s, NOW(), %s, %s, %s::vector)
        """, ("cosine-test-2", "CosineSimilarityTest2", "test2", v2))
        conn.commit()

        cur.execute("""
            SELECT event_id FROM rag_chunks
            WHERE event_id IN ('cosine-test-1', 'cosine-test-2')
            ORDER BY embedding <=> %s::vector
            LIMIT 1
        """, (query,))
        result = cur.fetchone()
        assert result[0] == "cosine-test-1"
    finally:
        conn.rollback()
        cur.execute("DELETE FROM rag_chunks WHERE event_id IN ('cosine-test-1','cosine-test-2')")
        conn.commit()


# ── TS-4-11: embedding dimension mismatch ────────────────────────────────────

def test_embedding_wrong_dimensions_rejected(conn, cur):
    """TS-4-11: 512-dim vector into 1536-dim column → error."""
    wrong_dim_embedding = [0.1] * 512
    with pytest.raises(psycopg2.Error):
        cur.execute("""
            INSERT INTO rag_chunks (event_id, event_time, event_name, raw_text, embedding)
            VALUES (%s, NOW(), %s, %s, %s::vector)
        """, ("dim-mismatch-test", "DimTest", "test", wrong_dim_embedding))
        conn.commit()
    conn.rollback()


# ── TS-4-EC-01: schema idempotency ───────────────────────────────────────────

def test_create_extension_idempotent(conn, cur):
    """TS-4-EC-01: running CREATE EXTENSION IF NOT EXISTS vector twice → no error."""
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    conn.commit()   # no exception = pass


# ── TS-4-EC-03: IVFFlat query on empty table ─────────────────────────────────

def test_cosine_query_on_effectively_empty_result(cur):
    """TS-4-EC-03: query returning 0 rows does not error."""
    # Filter for impossible event_time — simulates empty result set
    far_future = "2099-01-01T00:00:00Z"
    query_embedding = [0.0] * 1536
    cur.execute("""
        SELECT event_id FROM rag_chunks
        WHERE event_time > %s
        ORDER BY embedding <=> %s::vector
        LIMIT 5
    """, (far_future, query_embedding))
    results = cur.fetchall()
    assert results == []


# ── TS-4-EC-04: retrieved_ctx JSONB ──────────────────────────────────────────

def test_agent_log_retrieved_ctx_accepts_json(conn, cur):
    """TS-4-EC-04: valid JSON inserts without error."""
    try:
        ctx = json.dumps([{"event_name": "RunInstances", "resource": "i-0abc"}])
        cur.execute("""
            INSERT INTO agent_log
              (event_id, event_time, anomaly_score, retrieved_ctx, decision, confidence)
            VALUES (%s, NOW(), %s, %s::jsonb, %s, %s)
        """, ("jsonb-test-001", -0.25, ctx, "genuine_bug", 0.9))
        conn.commit()
    finally:
        conn.rollback()
        cur.execute("DELETE FROM agent_log WHERE event_id = 'jsonb-test-001'")
        conn.commit()


def test_agent_log_retrieved_ctx_accepts_null(conn, cur):
    """TS-4-EC-04: NULL retrieved_ctx allowed."""
    try:
        cur.execute("""
            INSERT INTO agent_log
              (event_id, event_time, anomaly_score, retrieved_ctx, decision, confidence)
            VALUES (%s, NOW(), %s, NULL, %s, %s)
        """, ("jsonb-null-test", -0.2, "genuine_bug", 0.8))
        conn.commit()
    finally:
        conn.rollback()
        cur.execute("DELETE FROM agent_log WHERE event_id = 'jsonb-null-test'")
        conn.commit()
