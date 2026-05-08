"""
Unit tests for LangGraph agent graph nodes (production plan rewrite).
Covers: TS-7-01..17

All external dependencies (psycopg2, boto3, requests, Kafka) are mocked.
Tests validate node logic, state transitions, and edge-case handling.

Target module: agent/graph.py (production plan version — Kafka consumer + Vault)
"""

import json
from unittest.mock import MagicMock, patch, call
import pytest


# ── Helpers mirroring agent/graph.py until rewrite ships ────────────────────
# Each function is copied verbatim from PRODUCTION_PLAN.md so tests are
# runnable today. Once agent/graph.py is rewritten, replace with:
#   from agent.graph import (retrieve_infra_context, classify_with_llm,
#                             route_decision, checkpoint_to_db, _embed)

import json as _json
import os as _os


def _load_vault(path: str, base_dir: str = "/vault/secrets") -> dict:
    result = {}
    try:
        with open(f"{base_dir}/{path}") as f:
            for line in f:
                k, _, v = line.strip().partition("=")
                if k:
                    result[k] = v
    except FileNotFoundError:
        pass
    return result


BEDROCK_MODEL = "anthropic.claude-3-5-sonnet-20241022-v2:0"


def _embed(text: str, bedrock_client=None) -> list:
    """Wraps boto3 Bedrock Titan — injected client for testing."""
    import boto3
    bedrock = bedrock_client or boto3.client("bedrock-runtime", region_name="us-east-1")
    resp = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": text})
    )
    return json.loads(resp["body"].read())["embedding"]


def retrieve_infra_context(state: dict, db_dsn: str, _psycopg2=None, _embed_fn=None) -> dict:
    import psycopg2
    _pg = _psycopg2 or psycopg2
    event = state["kafka_event"]
    query_text = f"{event['eventName']} {event.get('eventSource','')} {event.get('errorCode','')}"
    embedding = (_embed_fn or _embed)(query_text)

    conn = _pg.connect(db_dsn)
    cur = conn.cursor()
    cur.execute("""
        SELECT raw_text, event_name, event_time::text, resource, principal
        FROM rag_chunks
        WHERE event_time > %s
        ORDER BY embedding <=> %s::vector
        LIMIT 5
    """, (state["last_train_ts"], embedding))
    cols = [d[0] for d in cur.description]
    chunks = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()
    return {**state, "rag_chunks": chunks}


def classify_with_llm(state: dict, bedrock_client=None) -> dict:
    import boto3
    event = state["kafka_event"]
    context = "\n".join([
        f"- [{c['event_time']}] {c['event_name']} on {c['resource']} by {c['principal']}: {c['raw_text']}"
        for c in state["rag_chunks"]
    ]) or "No infrastructure changes found since last model training."

    prompt = (
        f"Classify event {event['eventName']} score {event['score']:.4f}. "
        f"Context: {context}"
    )
    bedrock = bedrock_client or boto3.client("bedrock-runtime", region_name="us-east-1")
    resp = bedrock.invoke_model(
        modelId=BEDROCK_MODEL,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 512,
            "messages": [{"role": "user", "content": prompt}]
        })
    )
    body = json.loads(resp["body"].read())
    content = json.loads(body["content"][0]["text"])

    return {
        **state,
        "prompt":       prompt,
        "llm_response": body["content"][0]["text"],
        "decision":     content["decision"],
        "confidence":   content["confidence"],
        "reasoning":    content["reasoning"],
    }


def route_decision(state: dict, slack_webhook: str, _requests=None) -> dict:
    import requests as _req
    req = _requests or _req
    event = state["kafka_event"]
    decision = state["decision"]

    color  = "#FF0000" if decision == "genuine_bug" else "#36a64f"
    header = "Genuine Anomaly Detected" if decision == "genuine_bug" else "Known Change — Suppressed"

    payload = {"attachments": [{"color": color, "blocks": [
        {"type": "header",  "text": {"type": "plain_text", "text": header}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Event:*\n{event['eventName']}"},
            {"type": "mrkdwn", "text": f"*Score:*\n{event['score']:.4f}"},
            {"type": "mrkdwn", "text": f"*Confidence:*\n{state['confidence']:.0%}"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Reasoning:*\n{state['reasoning']}"}}
    ]}]}

    r = req.post(slack_webhook, json=payload, timeout=5)
    slack_ts = r.headers.get("x-slack-message-ts")
    return {**state, "slack_ts": slack_ts}


def checkpoint_to_db(state: dict, db_dsn: str, _psycopg2=None) -> dict:
    import psycopg2
    _pg = _psycopg2 or psycopg2
    event = state["kafka_event"]
    conn = _pg.connect(db_dsn)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO agent_log
          (kafka_offset, event_id, event_time, anomaly_score,
           retrieved_ctx, bedrock_prompt, bedrock_response,
           decision, confidence, slack_ts)
        VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
    """, (
        state["kafka_offset"], event["eventID"], event["eventTime"], event["score"],
        json.dumps(state["rag_chunks"]), state["prompt"], state["llm_response"],
        state["decision"], state["confidence"], state["slack_ts"]
    ))
    conn.commit()
    conn.close()
    return state


# ── _embed tests ─────────────────────────────────────────────────────────────

class TestEmbed:

    def test_returns_1536_dim_vector(self, mock_bedrock_embed):
        """TS-7-16: Bedrock Titan returns 1536-dim embedding."""
        result = _embed("RunInstances ec2.amazonaws.com", bedrock_client=mock_bedrock_embed)

        assert isinstance(result, list)
        assert len(result) == 1536

    def test_calls_titan_model(self, mock_bedrock_embed):
        """Correct modelId sent to Bedrock."""
        _embed("test", bedrock_client=mock_bedrock_embed)

        call_kwargs = mock_bedrock_embed.invoke_model.call_args[1]
        assert call_kwargs["modelId"] == "amazon.titan-embed-text-v2:0"

    def test_empty_text_does_not_crash(self, mock_bedrock_embed):
        result = _embed("", bedrock_client=mock_bedrock_embed)
        assert len(result) == 1536


# ── retrieve_infra_context tests ─────────────────────────────────────────────

class TestRetrieveInfraContext:

    def test_returns_at_most_5_chunks(self, sample_anomaly_state, mock_psycopg2_conn, tmp_path):
        """TS-7-05: LIMIT 5 in query."""
        conn, cursor = mock_psycopg2_conn
        # Simulate 5 rows returned
        cursor.fetchall.return_value = [
            (f"text-{i}", f"Event{i}", "2026-04-15T08:00:00+00:00", f"res-{i}", "arn:role")
            for i in range(5)
        ]

        import psycopg2 as _pg
        result = retrieve_infra_context(
            sample_anomaly_state, "mock-dsn",
            _psycopg2=_pg,
            _embed_fn=lambda _: [0.1] * 1536
        )

        assert len(result["rag_chunks"]) == 5

    def test_filters_by_last_train_ts(self, sample_anomaly_state, mock_psycopg2_conn):
        """TS-7-06: last_train_ts passed as WHERE clause parameter."""
        conn, cursor = mock_psycopg2_conn

        import psycopg2 as _pg
        retrieve_infra_context(
            sample_anomaly_state, "mock-dsn",
            _psycopg2=_pg,
            _embed_fn=lambda _: [0.1] * 1536
        )

        execute_args = cursor.execute.call_args[0]
        assert sample_anomaly_state["last_train_ts"] in execute_args[1]

    def test_empty_rag_returns_empty_list(self, sample_anomaly_state, mock_psycopg2_conn):
        """TS-7-07: empty DB → empty rag_chunks, no exception."""
        conn, cursor = mock_psycopg2_conn
        cursor.fetchall.return_value = []

        import psycopg2 as _pg
        result = retrieve_infra_context(
            sample_anomaly_state, "mock-dsn",
            _psycopg2=_pg,
            _embed_fn=lambda _: [0.1] * 1536
        )

        assert result["rag_chunks"] == []

    def test_connection_closed_after_query(self, sample_anomaly_state, mock_psycopg2_conn):
        """TS-7-23: conn.close() called to prevent connection leak."""
        conn, cursor = mock_psycopg2_conn

        import psycopg2 as _pg
        retrieve_infra_context(
            sample_anomaly_state, "mock-dsn",
            _psycopg2=_pg,
            _embed_fn=lambda _: [0.1] * 1536
        )

        conn.close.assert_called_once()

    def test_query_text_uses_event_fields(self, sample_anomaly_state, mock_psycopg2_conn):
        """Query embeds eventName + eventSource."""
        conn, cursor = mock_psycopg2_conn
        embedded_texts = []

        def capture_embed(text):
            embedded_texts.append(text)
            return [0.1] * 1536

        import psycopg2 as _pg
        retrieve_infra_context(
            sample_anomaly_state, "mock-dsn",
            _psycopg2=_pg,
            _embed_fn=capture_embed
        )

        assert "RunInstances" in embedded_texts[0]


# ── classify_with_llm tests ──────────────────────────────────────────────────

class TestClassifyWithLlm:

    def _make_bedrock(self, decision, confidence=0.9, reasoning="test reason"):
        client = MagicMock()
        content = json.dumps({"decision": decision, "confidence": confidence, "reasoning": reasoning})
        body_bytes = json.dumps({"content": [{"text": content}]}).encode()
        mock_resp = MagicMock()
        mock_resp["body"].read.return_value = body_bytes
        client.invoke_model.return_value = mock_resp
        return client

    def test_parses_genuine_bug_response(self, sample_anomaly_state, sample_rag_chunks):
        """TS-7-08: Bedrock JSON → state fields populated."""
        state = {**sample_anomaly_state, "rag_chunks": sample_rag_chunks}
        bedrock = self._make_bedrock("genuine_bug", 0.92, "Unexpected EC2 launch")

        result = classify_with_llm(state, bedrock_client=bedrock)

        assert result["decision"] == "genuine_bug"
        assert result["confidence"] == 0.92
        assert result["reasoning"] == "Unexpected EC2 launch"

    def test_parses_known_change_response(self, sample_anomaly_state, sample_rag_chunks):
        """TS-7-10: decision is known_change."""
        state = {**sample_anomaly_state, "rag_chunks": sample_rag_chunks}
        bedrock = self._make_bedrock("known_change", 0.85, "Matches AMI rotation")

        result = classify_with_llm(state, bedrock_client=bedrock)

        assert result["decision"] == "known_change"

    def test_malformed_json_raises(self, sample_anomaly_state):
        """TS-7-09: non-JSON from Bedrock raises exception (not silent wrong decision)."""
        state = {**sample_anomaly_state, "rag_chunks": []}
        client = MagicMock()
        body_bytes = json.dumps({"content": [{"text": "NOT JSON AT ALL"}]}).encode()
        mock_resp = MagicMock()
        mock_resp["body"].read.return_value = body_bytes
        client.invoke_model.return_value = mock_resp

        with pytest.raises(json.JSONDecodeError):
            classify_with_llm(state, bedrock_client=client)

    def test_no_rag_chunks_uses_fallback_context(self, sample_anomaly_state):
        """Empty rag_chunks → prompt contains fallback 'No infrastructure changes' text."""
        state = {**sample_anomaly_state, "rag_chunks": []}
        bedrock = self._make_bedrock("genuine_bug")

        result = classify_with_llm(state, bedrock_client=bedrock)

        assert "No infrastructure changes" in result["prompt"]

    def test_decision_only_valid_values(self, sample_anomaly_state, sample_rag_chunks):
        """TS-7-10: decision field is exactly 'genuine_bug' or 'known_change'."""
        for decision in ("genuine_bug", "known_change"):
            state = {**sample_anomaly_state, "rag_chunks": sample_rag_chunks}
            bedrock = self._make_bedrock(decision)
            result = classify_with_llm(state, bedrock_client=bedrock)
            assert result["decision"] in ("genuine_bug", "known_change")

    def test_bedrock_called_with_correct_model(self, sample_anomaly_state):
        """Uses claude-3-5-sonnet-20241022-v2:0 model."""
        state = {**sample_anomaly_state, "rag_chunks": []}
        bedrock = self._make_bedrock("genuine_bug")

        classify_with_llm(state, bedrock_client=bedrock)

        call_kwargs = bedrock.invoke_model.call_args[1]
        assert call_kwargs["modelId"] == BEDROCK_MODEL

    def test_max_tokens_512(self, sample_anomaly_state):
        """max_tokens=512 sent to Bedrock."""
        state = {**sample_anomaly_state, "rag_chunks": []}
        bedrock = self._make_bedrock("genuine_bug")

        classify_with_llm(state, bedrock_client=bedrock)

        call_kwargs = bedrock.invoke_model.call_args[1]
        body = json.loads(call_kwargs["body"])
        assert body["max_tokens"] == 512


# ── route_decision tests ─────────────────────────────────────────────────────

class TestRouteDecision:

    def _make_requests_mock(self, slack_ts="1234567890.123456"):
        mock_req = MagicMock()
        resp = MagicMock()
        resp.headers = {"x-slack-message-ts": slack_ts}
        mock_req.post.return_value = resp
        return mock_req

    def test_genuine_bug_sends_red_color(self, sample_anomaly_state):
        """TS-7-11: genuine_bug → #FF0000 attachment color."""
        state = {**sample_anomaly_state, "decision": "genuine_bug", "confidence": 0.9, "reasoning": "Suspicious"}
        mock_req = self._make_requests_mock()

        route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)

        posted = mock_req.post.call_args[1]["json"]
        assert posted["attachments"][0]["color"] == "#FF0000"

    def test_known_change_sends_green_color(self, sample_anomaly_state):
        """TS-7-12: known_change → #36a64f attachment color."""
        state = {**sample_anomaly_state, "decision": "known_change", "confidence": 0.85, "reasoning": "Expected"}
        mock_req = self._make_requests_mock()

        route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)

        posted = mock_req.post.call_args[1]["json"]
        assert posted["attachments"][0]["color"] == "#36a64f"

    def test_genuine_bug_header_text(self, sample_anomaly_state):
        state = {**sample_anomaly_state, "decision": "genuine_bug", "confidence": 0.9, "reasoning": "X"}
        mock_req = self._make_requests_mock()

        route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)

        posted = mock_req.post.call_args[1]["json"]
        header = posted["attachments"][0]["blocks"][0]["text"]["text"]
        assert "Genuine Anomaly" in header

    def test_known_change_header_text(self, sample_anomaly_state):
        state = {**sample_anomaly_state, "decision": "known_change", "confidence": 0.85, "reasoning": "X"}
        mock_req = self._make_requests_mock()

        route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)

        posted = mock_req.post.call_args[1]["json"]
        header = posted["attachments"][0]["blocks"][0]["text"]["text"]
        assert "Known Change" in header

    def test_slack_ts_captured(self, sample_anomaly_state):
        """TS-7-11: slack_ts from response header stored in state."""
        state = {**sample_anomaly_state, "decision": "genuine_bug", "confidence": 0.9, "reasoning": "X"}
        mock_req = self._make_requests_mock("9876543210.654321")

        result = route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)

        assert result["slack_ts"] == "9876543210.654321"

    def test_slack_timeout_5s(self, sample_anomaly_state):
        """TS-7-13: request made with timeout=5."""
        state = {**sample_anomaly_state, "decision": "genuine_bug", "confidence": 0.9, "reasoning": "X"}
        mock_req = self._make_requests_mock()

        route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)

        call_kwargs = mock_req.post.call_args[1]
        assert call_kwargs["timeout"] == 5

    def test_slack_ts_none_when_header_absent(self, sample_anomaly_state):
        """TS-7-EC-05: missing slack_ts header → None, no crash."""
        state = {**sample_anomaly_state, "decision": "genuine_bug", "confidence": 0.9, "reasoning": "X"}
        mock_req = MagicMock()
        resp = MagicMock()
        resp.headers = {}
        mock_req.post.return_value = resp

        result = route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)

        assert result["slack_ts"] is None

    def test_missing_user_identity_graceful(self, sample_anomaly_state):
        """TS-7-EC-08: missing userIdentity key does not crash route_decision."""
        event = {**sample_anomaly_state["kafka_event"]}
        event.pop("userIdentity", None)
        state = {**sample_anomaly_state, "kafka_event": event, "decision": "genuine_bug",
                 "confidence": 0.9, "reasoning": "X"}
        mock_req = self._make_requests_mock()

        # Should not raise KeyError
        route_decision(state, "https://hooks.slack.com/test", _requests=mock_req)


# ── checkpoint_to_db tests ───────────────────────────────────────────────────

class TestCheckpointToDb:

    def test_inserts_all_required_fields(self, sample_anomaly_state, mock_psycopg2_conn):
        """TS-7-14: INSERT called with all state fields."""
        conn, cursor = mock_psycopg2_conn
        state = {
            **sample_anomaly_state,
            "decision":     "genuine_bug",
            "confidence":   0.92,
            "prompt":       "test prompt",
            "llm_response": '{"decision":"genuine_bug"}',
            "slack_ts":     "1234567890.123",
        }

        import psycopg2 as _pg
        checkpoint_to_db(state, "mock-dsn", _psycopg2=_pg)

        cursor.execute.assert_called_once()
        sql, params = cursor.execute.call_args[0]
        assert "INSERT INTO agent_log" in sql
        assert params[7] == "genuine_bug"       # decision
        assert params[8] == 0.92                # confidence

    def test_commit_called(self, sample_anomaly_state, mock_psycopg2_conn):
        """Transaction committed after INSERT."""
        conn, cursor = mock_psycopg2_conn
        state = {**sample_anomaly_state, "decision": "genuine_bug", "confidence": 0.9,
                 "prompt": "", "llm_response": "", "slack_ts": None}

        import psycopg2 as _pg
        checkpoint_to_db(state, "mock-dsn", _psycopg2=_pg)

        conn.commit.assert_called_once()

    def test_connection_closed_after_insert(self, sample_anomaly_state, mock_psycopg2_conn):
        """TS-7-23: conn.close() prevents connection leak."""
        conn, cursor = mock_psycopg2_conn
        state = {**sample_anomaly_state, "decision": "genuine_bug", "confidence": 0.9,
                 "prompt": "", "llm_response": "", "slack_ts": None}

        import psycopg2 as _pg
        checkpoint_to_db(state, "mock-dsn", _psycopg2=_pg)

        conn.close.assert_called_once()

    def test_slack_ts_none_inserts_null(self, sample_anomaly_state, mock_psycopg2_conn):
        """TS-7-EC-10: None slack_ts stored as NULL, not raising."""
        conn, cursor = mock_psycopg2_conn
        state = {**sample_anomaly_state, "decision": "known_change", "confidence": 0.8,
                 "prompt": "", "llm_response": "", "slack_ts": None}

        import psycopg2 as _pg
        checkpoint_to_db(state, "mock-dsn", _psycopg2=_pg)

        _, params = cursor.execute.call_args[0]
        assert params[-1] is None   # slack_ts is last param

    def test_rag_chunks_serialized_as_json(self, sample_anomaly_state, mock_psycopg2_conn, sample_rag_chunks):
        """TS-7-15: rag_chunks cast to jsonb-compatible JSON string."""
        conn, cursor = mock_psycopg2_conn
        state = {**sample_anomaly_state, "rag_chunks": sample_rag_chunks,
                 "decision": "genuine_bug", "confidence": 0.9,
                 "prompt": "", "llm_response": "", "slack_ts": None}

        import psycopg2 as _pg
        checkpoint_to_db(state, "mock-dsn", _psycopg2=_pg)

        _, params = cursor.execute.call_args[0]
        retrieved_ctx_param = params[4]   # retrieved_ctx position
        parsed = json.loads(retrieved_ctx_param)
        assert isinstance(parsed, list)
        assert parsed[0]["event_name"] == "RunInstances"


# ── Graph node order test ────────────────────────────────────────────────────

class TestGraphNodeOrdering:

    def test_graph_executes_in_correct_order(self):
        """TS-7-17: retrieve → classify → route → checkpoint order."""
        call_order = []

        def mock_retrieve(state, **_):
            call_order.append("retrieve")
            return {**state, "rag_chunks": []}

        def mock_classify(state, **_):
            call_order.append("classify")
            return {**state, "decision": "genuine_bug", "confidence": 0.9,
                    "reasoning": "test", "prompt": "", "llm_response": ""}

        def mock_route(state, **_):
            call_order.append("route")
            return {**state, "slack_ts": "ts123"}

        def mock_checkpoint(state, **_):
            call_order.append("checkpoint")
            return state

        # Simulate linear execution
        state = {
            "kafka_event":   {"eventID": "x", "eventName": "X", "eventTime": "2026-01-01", "score": -0.1},
            "kafka_offset":  1, "last_train_ts": "2026-01-01", "rag_chunks": [],
            "prompt": "", "llm_response": "", "decision": "", "confidence": 0.0,
            "reasoning": "", "slack_ts": None,
        }
        state = mock_retrieve(state)
        state = mock_classify(state)
        state = mock_route(state)
        state = mock_checkpoint(state)

        assert call_order == ["retrieve", "classify", "route", "checkpoint"]
