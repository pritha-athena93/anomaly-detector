"""
Unit tests for the existing triage graph (agent/graph.py — TriageState pipeline).
Covers: _llm_verdict_node, _fetch_infra_changes_node, and triage_app compilation.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


class TestLlmVerdictNode:

    def _make_triage_state(self, **overrides):
        base = {
            "event": {
                "eventName": "RunInstances",
                "eventSource": "ec2.amazonaws.com",
                "eventTime": "2026-05-08T10:00:00Z",
                "awsRegion": "us-east-1",
                "errorCode": None,
            },
            "features": [0.0] * 18,
            "event_source": "ec2.amazonaws.com",
            "infra_changes": "RunInstances during AMI rotation at 2026-04-15",
            "verdict": "",
            "confidence": 0.0,
            "reason": "",
        }
        return {**base, **overrides}

    def _mock_llm_response(self, content_str: str):
        mock_llm = MagicMock()
        resp = MagicMock()
        resp.content = content_str
        mock_llm.invoke.return_value = resp
        return mock_llm

    def test_genuine_bug_verdict_parsed(self):
        """TS-7-08: Valid JSON → verdict/confidence/reason extracted."""
        from agent.graph import _llm_verdict_node

        state = self._make_triage_state()
        payload = json.dumps({
            "verdict":    "genuine_bug",
            "confidence": 0.91,
            "reason":     "No matching infra change."
        })

        with patch("agent.graph._triage_llm") as mock_llm:
            resp = MagicMock()
            resp.content = payload
            mock_llm.invoke.return_value = resp
            result = _llm_verdict_node(state)

        assert result["verdict"] == "genuine_bug"
        assert result["confidence"] == 0.91
        assert result["reason"] == "No matching infra change."

    def test_known_change_verdict_parsed(self):
        from agent.graph import _llm_verdict_node

        state = self._make_triage_state()
        payload = json.dumps({
            "verdict":    "known_change",
            "confidence": 0.87,
            "reason":     "AMI rotation explains RunInstances."
        })

        with patch("agent.graph._triage_llm") as mock_llm:
            resp = MagicMock()
            resp.content = payload
            mock_llm.invoke.return_value = resp
            result = _llm_verdict_node(state)

        assert result["verdict"] == "known_change"

    def test_malformed_json_falls_back_to_genuine_bug(self):
        """TS-7-09 / existing graph: bad JSON → genuine_bug safe default, no crash."""
        from agent.graph import _llm_verdict_node

        state = self._make_triage_state()
        with patch("agent.graph._triage_llm") as mock_llm:
            resp = MagicMock()
            resp.content = "This is not JSON at all: { broken"
            mock_llm.invoke.return_value = resp
            result = _llm_verdict_node(state)

        assert result["verdict"] == "genuine_bug"
        assert result["confidence"] == 0.5
        # reason contains raw LLM output (truncated)
        assert isinstance(result["reason"], str)
        assert len(result["reason"]) <= 500

    def test_confidence_cast_to_float(self):
        """Confidence returned as float, not string."""
        from agent.graph import _llm_verdict_node

        state = self._make_triage_state()
        payload = json.dumps({"verdict": "genuine_bug", "confidence": "0.9", "reason": "test"})

        with patch("agent.graph._triage_llm") as mock_llm:
            resp = MagicMock()
            resp.content = payload
            mock_llm.invoke.return_value = resp
            result = _llm_verdict_node(state)

        assert isinstance(result["confidence"], float)

    def test_missing_confidence_defaults_to_0_5(self):
        """Missing confidence key → default 0.5."""
        from agent.graph import _llm_verdict_node

        state = self._make_triage_state()
        payload = json.dumps({"verdict": "genuine_bug", "reason": "x"})

        with patch("agent.graph._triage_llm") as mock_llm:
            resp = MagicMock()
            resp.content = payload
            mock_llm.invoke.return_value = resp
            result = _llm_verdict_node(state)

        assert result["confidence"] == 0.5

    def test_verdict_prompt_contains_event_json(self):
        """Event dict serialized into LLM prompt."""
        from agent.graph import _llm_verdict_node

        state = self._make_triage_state()
        payload = json.dumps({"verdict": "genuine_bug", "confidence": 0.9, "reason": "x"})

        with patch("agent.graph._triage_llm") as mock_llm:
            resp = MagicMock()
            resp.content = payload
            mock_llm.invoke.return_value = resp
            _llm_verdict_node(state)

        prompt_content = mock_llm.invoke.call_args[0][0][0].content
        assert "RunInstances" in prompt_content

    def test_verdict_prompt_contains_infra_changes(self):
        from agent.graph import _llm_verdict_node

        state = self._make_triage_state(infra_changes="AMI rotation on 2026-04-15")
        payload = json.dumps({"verdict": "known_change", "confidence": 0.85, "reason": "x"})

        with patch("agent.graph._triage_llm") as mock_llm:
            resp = MagicMock()
            resp.content = payload
            mock_llm.invoke.return_value = resp
            _llm_verdict_node(state)

        prompt = mock_llm.invoke.call_args[0][0][0].content
        assert "AMI rotation" in prompt


class TestFetchInfraChangesNode:

    def test_calls_get_infra_changes_tool(self):
        """_fetch_infra_changes_node invokes the RAG tool."""
        from agent.graph import _fetch_infra_changes_node

        state = {
            "event": {}, "features": [], "event_source": "ec2.amazonaws.com",
            "infra_changes": "", "verdict": "", "confidence": 0.0, "reason": ""
        }
        with patch("agent.graph.get_infra_changes_since_training") as mock_tool:
            mock_tool.invoke.return_value = "Top 3 changes: ..."
            result = _fetch_infra_changes_node(state)

        assert result["infra_changes"] == "Top 3 changes: ..."

    def test_event_source_passed_to_tool(self):
        """event_source forwarded to RAG tool for focused retrieval."""
        from agent.graph import _fetch_infra_changes_node

        state = {
            "event": {}, "features": [], "event_source": "s3.amazonaws.com",
            "infra_changes": "", "verdict": "", "confidence": 0.0, "reason": ""
        }
        with patch("agent.graph.get_infra_changes_since_training") as mock_tool:
            mock_tool.invoke.return_value = "changes"
            _fetch_infra_changes_node(state)

        call_arg = mock_tool.invoke.call_args[0][0]
        assert call_arg["event_source"] == "s3.amazonaws.com"

    def test_empty_event_source_handled(self):
        """Missing event_source uses empty string (state.get fallback)."""
        from agent.graph import _fetch_infra_changes_node

        state = {
            "event": {}, "features": [],
            # event_source intentionally absent
            "infra_changes": "", "verdict": "", "confidence": 0.0, "reason": ""
        }
        with patch("agent.graph.get_infra_changes_since_training") as mock_tool:
            mock_tool.invoke.return_value = "none"
            result = _fetch_infra_changes_node(state)

        assert result["infra_changes"] == "none"


class TestTriageAppCompilation:

    def test_triage_app_compiles(self):
        """triage_app is a compiled LangGraph graph."""
        from agent.graph import triage_app
        assert triage_app is not None

    def test_ops_app_compiles(self):
        from agent.graph import ops_app
        assert ops_app is not None
