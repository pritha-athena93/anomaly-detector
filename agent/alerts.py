"""
Alerting — PagerDuty Events API v2 + Slack Incoming Webhooks.

Both functions are best-effort: they return None on failure rather than raising,
so a Slack/PagerDuty outage doesn't crash the graph or block Kafka commits.
Secrets fetched from Vault-injected files first, Secrets Manager as fallback.
"""

import json
import os
from typing import Optional

import boto3
import requests


def _get_secret(secret_id: str, vault_key: Optional[str] = None) -> str:
    """Read from Vault-injected file first, fall back to Secrets Manager."""
    if vault_key:
        vault_file = f"/vault/secrets/{vault_key}"
        if os.path.exists(vault_file):
            for line in open(vault_file):
                k, _, v = line.strip().partition("=")
                if k == vault_key.upper():
                    return v
    return boto3.client("secretsmanager").get_secret_value(
        SecretId=secret_id
    )["SecretString"]


_SEVERITY_MAP = {"P1": "critical", "P2": "error", "P3": "warning"}


def page_pagerduty(state: dict) -> Optional[str]:
    """
    Create a PagerDuty incident for genuine_bug decisions.
    Returns dedup_key string, or None if PagerDuty call fails.
    """
    try:
        key = _get_secret("anomaly/pagerduty-key", "pagerduty_key")
        event = state["kafka_event"]
        severity = _SEVERITY_MAP.get(state.get("_severity", "P3"), "warning")

        resp = requests.post(
            "https://events.pagerduty.com/v2/enqueue",
            json={
                "routing_key": key,
                "event_action": "trigger",
                "dedup_key": event["eventID"],   # PD-side idempotency
                "payload": {
                    "summary": (
                        f"Anomaly: {event.get('eventName','?')} by "
                        f"{event.get('userIdentity', {}).get('arn', 'unknown')}"
                    ),
                    "severity": severity,
                    "source": "anomaly-detector",
                    "timestamp": event.get("eventTime"),
                    "custom_details": {
                        "score": event.get("score"),
                        "reasoning": state.get("reasoning", ""),
                        "region": event.get("awsRegion"),
                        "error": event.get("errorCode"),
                    },
                },
            },
            timeout=5,
        )
        return resp.json().get("dedup_key")
    except Exception:
        return None


def notify_slack(state: dict) -> Optional[str]:
    """
    Post to Slack #anomalies channel.
    Returns Slack message timestamp, or None on failure.

    Gap-1 note: this is called only after the idempotency_check node confirms
    the event_id is NOT already in agent_log, preventing duplicate posts on replay.
    """
    try:
        webhook = _get_secret("anomaly/slack-webhook", "slack_webhook")
        event = state["kafka_event"]
        decision = state["decision"]
        is_bug = decision == "genuine_bug"
        icon = ":rotating_light:" if is_bug else ":white_check_mark:"
        color = "#FF0000" if is_bug else "#36a64f"
        title = "Genuine Anomaly" if is_bug else "Known Change"

        resp = requests.post(
            webhook,
            json={
                "attachments": [{
                    "color": color,
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"{icon} {title}: {event.get('eventName', '?')}",
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*Score:*\n{event.get('score', '?'):.4f}"},
                                {"type": "mrkdwn", "text": f"*Time:*\n{event.get('eventTime', '?')}"},
                                {"type": "mrkdwn", "text": f"*Principal:*\n{event.get('userIdentity', {}).get('arn', '?')}"},
                                {"type": "mrkdwn", "text": f"*Region:*\n{event.get('awsRegion', '?')}"},
                            ],
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Reasoning:*\n{state.get('reasoning', 'n/a')}",
                            },
                        },
                    ],
                }]
            },
            timeout=5,
        )
        return resp.headers.get("x-slack-message-ts")
    except Exception:
        return None
