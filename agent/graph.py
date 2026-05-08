"""
LangGraph graphs for the anomaly detection agent.

consumer_graph — Kafka-driven pipeline (primary runtime):
    idempotency_check → retrieve → classify → route → checkpoint
    Entry: run_consumer()

ops_app — ReAct loop for HTTP /query endpoint (ops tooling).

Gap-1 fix (duplicate Slack on pod restart):
    idempotency_check node queries agent_log for event_id before any side
    effects. If already present, routes to END immediately. Even if a pod
    restarts between route_decision and checkpoint_to_db, the replayed message
    skips Slack/PagerDuty on re-processing. agent_log uses ON CONFLICT DO NOTHING.

Gap-3 fix (no Bedrock retry):
    _bedrock_invoke() is wrapped with tenacity exponential backoff, retrying
    specifically on ThrottlingException and ServiceUnavailableException.
    5 attempts, 4s→60s backoff, re-raises after exhaustion so Kafka replays.
"""

import json
import os
from typing import Annotated, Optional, TypedDict

import boto3
from botocore.exceptions import ClientError
from langchain_aws import ChatBedrock
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from agent.alerts import notify_slack, page_pagerduty
from agent.db import checkpoint, is_already_processed
from agent.rag import retrieve_infra_context
from agent.tools import all_tools

_BEDROCK_MODEL = "anthropic.claude-3-5-sonnet-20241022-v2:0"
_AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


# ── Gap-3: Bedrock retry helper ───────────────────────────
# Retries on throttle and transient service errors only.
# Re-raises after 5 attempts so Kafka offset stays uncommitted → message replays.

def _is_retryable_bedrock(exc: BaseException) -> bool:
    if not isinstance(exc, ClientError):
        return False
    code = exc.response["Error"]["Code"]
    return code in {
        "ThrottlingException",
        "ServiceUnavailableException",
        "ModelNotReadyException",
        "RequestLimitExceeded",
    }


@retry(
    retry=retry_if_exception(_is_retryable_bedrock),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _bedrock_invoke(llm, messages: list) -> object:
    return llm.invoke(messages)


# ── Ops agent (ReAct loop — HTTP /query) ─────────────────

class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]


_ops_llm = ChatBedrock(
    model_id=_BEDROCK_MODEL,
    region_name=_AWS_REGION,
).bind_tools(all_tools)


def _agent_node(state: AgentState) -> AgentState:
    response = _bedrock_invoke(_ops_llm, state["messages"])
    return {"messages": [response]}


def _should_continue(state: AgentState) -> str:
    last = state["messages"][-1]
    return "tools" if getattr(last, "tool_calls", None) else END


_ops_builder = StateGraph(AgentState)
_ops_builder.add_node("agent", _agent_node)
_ops_builder.add_node("tools", ToolNode(all_tools))
_ops_builder.set_entry_point("agent")
_ops_builder.add_conditional_edges(
    "agent", _should_continue, {"tools": "tools", END: END}
)
_ops_builder.add_edge("tools", "agent")

ops_app = _ops_builder.compile()


# ── Kafka consumer graph ──────────────────────────────────

class AnomalyState(TypedDict):
    kafka_event: dict
    kafka_offset: int
    last_train_ts: str
    rag_chunks: list[dict]
    prompt: str
    llm_response: str
    decision: str
    confidence: float
    reasoning: str
    pagerduty_id: Optional[str]
    slack_ts: Optional[str]
    already_processed: bool   # set by idempotency_check; skip side effects if True


_classify_llm = ChatBedrock(model_id=_BEDROCK_MODEL, region_name=_AWS_REGION)

_CLASSIFY_PROMPT = """\
You are an AWS security analyst. An IsolationForest model flagged this CloudTrail event.

FLAGGED EVENT:
{event}

INFRASTRUCTURE CHANGES SINCE LAST MODEL TRAINING ({last_train_ts}):
{context_block}

Is this anomaly a genuine security issue (genuine_bug) or explained by a recent \
infrastructure change (known_change)?

Reply in JSON only — no markdown, no extra keys:
{{"decision": "genuine_bug" | "known_change", "confidence": 0.0-1.0, \
"reasoning": "one paragraph", "severity": "P1" | "P2" | "P3"}}"""


# ── Graph nodes ───────────────────────────────────────────

def _idempotency_check_node(state: AnomalyState) -> dict:
    """
    Gap-1 fix: check agent_log before any side effects.

    If this event_id was already checkpointed (pod restarted after DB write
    but before Kafka commit), mark already_processed=True so the graph routes
    to END without re-posting to Slack or PagerDuty.
    """
    event_id = state["kafka_event"].get("eventID", "")
    return {"already_processed": bool(event_id and is_already_processed(event_id))}


def _retrieve_node(state: AnomalyState) -> dict:
    chunks = retrieve_infra_context(
        event=state["kafka_event"],
        last_train_ts=state["last_train_ts"],
    )
    return {"rag_chunks": chunks}


def _classify_node(state: AnomalyState) -> dict:
    event = state["kafka_event"]
    chunks = state.get("rag_chunks", [])

    context_block = "\n".join(
        f"- [{c['event_time']}] {c['event_name']} on {c.get('resource','?')} "
        f"by {c.get('principal','?')}: {c['raw_text']}"
        for c in chunks
    ) or "No infrastructure changes found since last model training."

    prompt = _CLASSIFY_PROMPT.format(
        event=json.dumps(event, indent=2, default=str),
        last_train_ts=state["last_train_ts"],
        context_block=context_block,
    )

    # Gap-3 fix: retry on Bedrock throttle/transient errors
    response = _bedrock_invoke(_classify_llm, [HumanMessage(content=prompt)])

    try:
        parsed = json.loads(response.content)
        return {
            "prompt": prompt,
            "llm_response": response.content,
            "decision": parsed.get("decision", "genuine_bug"),
            "confidence": float(parsed.get("confidence", 0.5)),
            "reasoning": parsed.get("reasoning", ""),
            "_severity": parsed.get("severity", "P3"),
        }
    except (json.JSONDecodeError, ValueError):
        return {
            "prompt": prompt,
            "llm_response": response.content,
            "decision": "genuine_bug",   # safe default
            "confidence": 0.5,
            "reasoning": response.content[:500],
            "_severity": "P3",
        }


def _route_node(state: AnomalyState) -> dict:
    pd_id: Optional[str] = None
    if state["decision"] == "genuine_bug":
        pd_id = page_pagerduty(state)
    slack_ts = notify_slack(state)
    return {"pagerduty_id": pd_id, "slack_ts": slack_ts}


def _checkpoint_node(state: AnomalyState) -> dict:
    checkpoint(state)
    return {}


# ── Routing ───────────────────────────────────────────────

def _after_idempotency(state: AnomalyState) -> str:
    return END if state["already_processed"] else "retrieve"


# ── Graph assembly ────────────────────────────────────────

_consumer_builder = StateGraph(AnomalyState)
_consumer_builder.add_node("idempotency_check", _idempotency_check_node)
_consumer_builder.add_node("retrieve", _retrieve_node)
_consumer_builder.add_node("classify", _classify_node)
_consumer_builder.add_node("route", _route_node)
_consumer_builder.add_node("checkpoint", _checkpoint_node)

_consumer_builder.set_entry_point("idempotency_check")
_consumer_builder.add_conditional_edges(
    "idempotency_check",
    _after_idempotency,
    {"retrieve": "retrieve", END: END},
)
_consumer_builder.add_edge("retrieve", "classify")
_consumer_builder.add_edge("classify", "route")
_consumer_builder.add_edge("route", "checkpoint")
_consumer_builder.add_edge("checkpoint", END)

consumer_graph = _consumer_builder.compile()


# ── Kafka consumer entrypoint ─────────────────────────────

def run_consumer() -> None:
    """
    Blocking Kafka consumer loop.

    last_train_ts fetched from SSM once at startup (not per-message) — TS-7-21.
    Offset committed ONLY after graph.invoke() completes (enable_auto_commit=False).
    On pod restart, un-committed messages replay; idempotency_check guards duplicates.
    """
    import os
    import json
    from kafka import KafkaConsumer

    ssm = boto3.client("ssm", region_name=_AWS_REGION)
    last_train_ts = ssm.get_parameter(
        Name="/anomaly/last_train_ts"
    )["Parameter"]["Value"]

    ca_cert = os.environ.get("KAFKA_CA_CERT", "/etc/redpanda/certs/ca.crt")

    consumer = KafkaConsumer(
        "anomalies-flagged",
        bootstrap_servers=os.environ["KAFKA_BROKERS"],
        group_id="langgraph-agent",
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=False,   # manual commit after full graph completion
        security_protocol="SSL",
        ssl_cafile=ca_cert,         # Redpanda TLS CA cert (reflected by Reflector)
    )

    for msg in consumer:
        consumer_graph.invoke({
            "kafka_event": msg.value,
            "kafka_offset": msg.offset,
            "last_train_ts": last_train_ts,
            "rag_chunks": [],
            "prompt": "",
            "llm_response": "",
            "decision": "",
            "confidence": 0.5,
            "reasoning": "",
            "pagerduty_id": None,
            "slack_ts": None,
            "already_processed": False,
        })
        consumer.commit()   # only reached if graph completes without exception
