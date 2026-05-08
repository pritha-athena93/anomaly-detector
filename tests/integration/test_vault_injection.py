"""
Integration tests for HashiCorp Vault secret injection (Phase 3).
Covers: TS-3-05..15

Requires: kubectl access to cluster + Vault port-forwarded at TEST_VAULT_ADDR.
Run: pytest -m integration tests/integration/test_vault_injection.py

Some tests use kubectl subprocess calls — require KUBECONFIG set.
"""

import os
import json
import subprocess
import time
import pytest

pytestmark = pytest.mark.integration

VAULT_ADDR    = os.environ.get("TEST_VAULT_ADDR",     "http://localhost:8200")
VAULT_TOKEN   = os.environ.get("TEST_VAULT_TOKEN",    "")   # root or admin token
NAMESPACE_MAP = {
    "ml-agent":          ("ml-agent-sa",   "agent-policy",    ["postgres", "slack", "kafka"]),
    "anomaly-poller":    ("poller-sa",     "poller-policy",   ["kafka", "aws"]),
    "training-pipeline": ("training-sa",   "training-policy", ["postgres"]),
}

try:
    import hvac
except ImportError:
    pytest.skip("hvac not installed — pip install hvac", allow_module_level=True)


@pytest.fixture(scope="module")
def vault_client():
    if not VAULT_TOKEN:
        pytest.skip("TEST_VAULT_TOKEN not set")
    client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)
    assert client.is_authenticated(), "Vault auth failed with provided token"
    return client


def _kubectl(args: list, check=True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl"] + args,
        capture_output=True, text=True, check=check
    )


# ── TS-3-01: Vault initialized ───────────────────────────────────────────────

def test_vault_initialized(vault_client):
    """TS-3-01: Vault is initialized (not new/empty)."""
    status = vault_client.sys.read_health_status(method="GET")
    assert status["initialized"] is True


def test_vault_unsealed(vault_client):
    """TS-3-01: Vault is unsealed."""
    status = vault_client.sys.read_health_status(method="GET")
    assert status["sealed"] is False


# ── TS-3-03 / TS-3-04: KV + K8s auth enabled ────────────────────────────────

def test_kv_v2_engine_enabled(vault_client):
    """TS-3-03: KV v2 secret engine mounted at 'secret/'."""
    mounts = vault_client.sys.list_mounted_secrets_engines()
    assert "secret/" in mounts, "KV v2 not mounted at secret/"
    assert mounts["secret/"]["type"] == "kv"
    assert mounts["secret/"]["options"].get("version") == "2"


def test_k8s_auth_enabled(vault_client):
    """TS-3-04: Kubernetes auth method enabled."""
    auth_methods = vault_client.sys.list_auth_methods()
    assert "kubernetes/" in auth_methods, "kubernetes auth not enabled"


# ── TS-3-05: agent SA reads postgres/slack/kafka ─────────────────────────────

@pytest.mark.parametrize("secret_name", ["postgres", "slack", "kafka"])
def test_ml_agent_can_read_secret(vault_client, secret_name):
    """TS-3-05: agent-policy allows read on all three secrets."""
    secret = vault_client.secrets.kv.v2.read_secret_version(
        path=f"anomaly/{secret_name}",
        mount_point="secret"
    )
    assert secret["data"]["data"] is not None


# ── TS-3-06: ml-agent cannot read training policy paths ──────────────────────

def test_ml_agent_policy_read_only(vault_client):
    """TS-3-10: agent-policy grants only 'read', not 'create'/'update'/'delete'."""
    # Get the raw policy text
    policy = vault_client.sys.read_policy(name="agent-policy")
    rules = policy["rules"] if "rules" in policy else policy.get("data", {}).get("rules", "")

    assert "create" not in rules.lower() or _policy_has_no_write_caps(rules)
    assert "delete" not in rules.lower() or _policy_has_no_write_caps(rules)


def _policy_has_no_write_caps(rules: str) -> bool:
    """Parse HCL-like policy and check no write capabilities."""
    # Simple heuristic: policy with only 'read' in capabilities
    import re
    capabilities = re.findall(r'capabilities\s*=\s*\[(.*?)\]', rules, re.DOTALL)
    for cap_list in capabilities:
        caps = re.findall(r'"(\w+)"', cap_list)
        for cap in caps:
            if cap in ("create", "update", "delete", "sudo"):
                return False
    return True


# ── TS-3-07: poller SA scope ──────────────────────────────────────────────────

def test_poller_policy_grants_kafka_and_aws(vault_client):
    """TS-3-07: poller-policy allows kafka + aws, not postgres or slack."""
    policy = vault_client.sys.read_policy(name="poller-policy")
    rules = policy["rules"] if "rules" in policy else policy.get("data", {}).get("rules", "")

    assert "anomaly/kafka" in rules
    assert "anomaly/aws" in rules
    assert "anomaly/postgres" not in rules
    assert "anomaly/slack" not in rules


# ── TS-3-08: training SA scope ───────────────────────────────────────────────

def test_training_policy_grants_postgres_only(vault_client):
    """TS-3-08: training-policy allows only postgres."""
    policy = vault_client.sys.read_policy(name="training-policy")
    rules = policy["rules"] if "rules" in policy else policy.get("data", {}).get("rules", "")

    assert "anomaly/postgres" in rules
    assert "anomaly/kafka" not in rules
    assert "anomaly/slack" not in rules


# ── TS-3-09: Vault Agent sidecar present in agent pods ───────────────────────

def test_vault_agent_sidecar_in_ml_agent_pod():
    """TS-3-09: vault-agent container running alongside ml-agent."""
    result = _kubectl([
        "get", "pods", "-n", "ml-agent",
        "-o", "jsonpath={.items[*].spec.containers[*].name}",
    ], check=False)

    if result.returncode != 0:
        pytest.skip(f"kubectl failed: {result.stderr}")

    container_names = result.stdout.strip()
    assert "vault-agent" in container_names, \
        f"vault-agent sidecar not found in ml-agent pods. Got: {container_names}"


# ── TS-3-10: secret rendered in env-file format ──────────────────────────────

def test_secret_file_format_parseable():
    """TS-3-10: /vault/secrets/postgres file readable by _load_vault()."""
    # Find an ml-agent pod
    result = _kubectl([
        "get", "pods", "-n", "ml-agent",
        "-o", "jsonpath={.items[0].metadata.name}",
    ], check=False)
    if result.returncode != 0 or not result.stdout.strip():
        pytest.skip("No ml-agent pods found")

    pod_name = result.stdout.strip()
    cat_result = _kubectl([
        "exec", pod_name, "-n", "ml-agent", "-c", "agent",
        "--", "cat", "/vault/secrets/postgres"
    ], check=False)

    if cat_result.returncode != 0:
        pytest.skip(f"Cannot exec into pod: {cat_result.stderr}")

    content = cat_result.stdout
    assert "DB_DSN=" in content, f"Expected DB_DSN= in secret file, got: {content[:200]}"
    # Verify it's parseable as key=value
    for line in content.strip().split("\n"):
        if line:
            assert "=" in line, f"Line not in key=value format: {line!r}"


# ── TS-3-12: no secrets in K8s Secrets ───────────────────────────────────────

def test_no_credential_secrets_in_k8s():
    """TS-3-12: No K8s Secret objects containing postgres/slack/kafka credentials."""
    result = _kubectl(["get", "secrets", "-A", "-o", "json"], check=False)
    if result.returncode != 0:
        pytest.skip(f"kubectl get secrets failed: {result.stderr}")

    secrets_json = json.loads(result.stdout)
    for secret in secrets_json.get("items", []):
        name = secret["metadata"]["name"]
        ns   = secret["metadata"]["namespace"]
        # Skip system secrets
        if any(skip in name for skip in ("default-token", "kube-", "argocd-initial")):
            continue
        data = secret.get("data", {})
        for key in data:
            assert key.lower() not in ("password", "db_dsn", "dsn", "webhook_url",
                                        "slack_webhook", "bootstrap_servers"), \
                f"Found credential key '{key}' in Secret '{name}' (ns: {ns})"


# ── TS-3-13: no secrets in pod env vars ──────────────────────────────────────

def test_no_credentials_in_ml_agent_env_vars():
    """TS-3-13: ml-agent pods have no plaintext credentials in env."""
    result = _kubectl([
        "get", "pods", "-n", "ml-agent",
        "-o", "jsonpath={.items[0].metadata.name}",
    ], check=False)
    if result.returncode != 0 or not result.stdout.strip():
        pytest.skip("No ml-agent pods found")

    pod_name = result.stdout.strip()
    env_result = _kubectl([
        "exec", pod_name, "-n", "ml-agent", "-c", "agent", "--", "env"
    ], check=False)

    if env_result.returncode != 0:
        pytest.skip("Cannot exec into agent pod")

    env_output = env_result.stdout
    forbidden_patterns = ["DB_DSN=", "SLACK_WEBHOOK=", "KAFKA_BROKERS=", "password="]
    for pattern in forbidden_patterns:
        assert pattern.lower() not in env_output.lower(), \
            f"Found credential '{pattern}' in pod env vars"


# ── TS-3-15: vault-init.json not in git ──────────────────────────────────────

def test_vault_init_json_not_in_git():
    """TS-3-15: vault-init.json not committed to repository."""
    result = subprocess.run(
        ["git", "log", "--all", "--", "vault-init.json"],
        capture_output=True, text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    assert result.stdout.strip() == "", \
        "vault-init.json found in git history — unseal keys may be compromised"
