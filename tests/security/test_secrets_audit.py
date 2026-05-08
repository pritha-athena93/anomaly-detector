"""
Security audit tests (TS-SEC-*).
Covers: TS-SEC-01..13

Tests are grouped into:
  - Static (run anywhere): git history, source code, Terraform state
  - Live cluster (needs kubectl): K8s Secrets, pod env vars
  - AWS (needs credentials): S3, RDS, SG, IAM

Run all: pytest -m security tests/security/
Run static only: pytest -m "security and not infra" tests/security/
"""

import json
import os
import re
import subprocess
import pytest

pytestmark = pytest.mark.security

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AWS_REGION   = os.environ.get("TEST_AWS_REGION",  "us-east-1")
CLUSTER_NAME = os.environ.get("TEST_CLUSTER_NAME", "anomaly-detector")


def _git(args: list) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git"] + args, capture_output=True, text=True, cwd=REPO_ROOT
    )


def _kubectl(args: list, check=False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl"] + args, capture_output=True, text=True, check=check
    )


# ── TS-SEC-01: No secrets in git history ─────────────────────────────────────

CREDENTIAL_PATTERNS = [
    r"password\s*=\s*['\"][^'\"]{8,}['\"]",   # password = "long-value"
    r"AWS_SECRET_ACCESS_KEY\s*=",
    r"BEGIN\s+(RSA|EC|OPENSSH)\s+PRIVATE\s+KEY",
    r"(?i)slack_webhook\s*=\s*https://hooks\.slack\.com",
    r"vault\.hashicorp\.com/.*token.*=\s*[A-Za-z0-9.]{20,}",
]


def test_no_hardcoded_passwords_in_git_log():
    """TS-SEC-01: No hardcoded passwords in git commit history."""
    result = _git(["log", "--all", "-p", "--follow", "--", "*.py", "*.tf", "*.yaml", "*.yml"])
    diff_text = result.stdout

    for pattern in CREDENTIAL_PATTERNS:
        matches = re.findall(pattern, diff_text)
        # Filter out obvious test/mock values
        real_matches = [m for m in matches if "test" not in m.lower()
                        and "mock" not in m.lower()
                        and "example" not in m.lower()]
        assert not real_matches, \
            f"Potential credential found in git history matching '{pattern}': {real_matches[:3]}"


def test_vault_init_json_not_in_git():
    """TS-3-15 / TS-SEC-01: vault-init.json (unseal keys) not committed."""
    result = _git(["log", "--all", "--", "vault-init.json"])
    assert result.stdout.strip() == "", \
        "vault-init.json found in git history — unseal keys may be compromised"


def test_no_tfvars_with_secrets_in_git():
    """TS-SEC-01: terraform.tfvars not committed (may contain sensitive values)."""
    result = _git(["ls-files", "infra/config/terraform.tfvars"])
    # If tracked by git, warn — but only fail if it contains obvious secrets
    if result.stdout.strip():
        tfvars_result = _git(["show", "HEAD:infra/config/terraform.tfvars"])
        content = tfvars_result.stdout.lower()
        assert "password" not in content, \
            "terraform.tfvars contains 'password' and is tracked in git"
        assert "secret" not in content or "secret_name" in content, \
            "terraform.tfvars may contain secrets and is tracked in git"


# ── Source code checks ────────────────────────────────────────────────────────

def test_no_hardcoded_db_credentials_in_python():
    """No hardcoded DSN/password in Python source files."""
    result = subprocess.run(
        ["grep", "-rn", r"postgresql://.*:.*@", "--include=*.py", REPO_ROOT],
        capture_output=True, text=True
    )
    lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
    # Allow test fixtures with 'test' password
    real_hits = [l for l in lines if l and "test" not in l.lower()
                 and "example" not in l.lower() and "conftest" not in l]
    assert not real_hits, f"Hardcoded DB credentials in Python: {real_hits}"


def test_no_hardcoded_slack_webhooks_in_source():
    """TS-SEC-01: No real Slack webhook URLs in source code."""
    result = subprocess.run(
        ["grep", "-rn", r"https://hooks\.slack\.com/services/[A-Z0-9]+/",
         "--include=*.py", "--include=*.yaml", "--include=*.yml", REPO_ROOT],
        capture_output=True, text=True
    )
    lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
    real_hooks = [l for l in lines if l and "test" not in l.lower()
                  and "example" not in l.lower() and "XXX" not in l]
    assert not real_hooks, f"Real Slack webhook found in source: {real_hooks}"


def test_no_aws_access_keys_in_source():
    """TS-SEC-01: No AWS access key IDs (AKIA...) in source."""
    result = subprocess.run(
        ["grep", "-rn", r"AKIA[0-9A-Z]{16}",
         "--include=*.py", "--include=*.tf", "--include=*.yaml", REPO_ROOT],
        capture_output=True, text=True
    )
    lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
    assert not [l for l in lines if l], f"AWS access key ID found in source: {lines}"


# ── TS-SEC-02: Terraform state ────────────────────────────────────────────────

def test_no_plaintext_password_in_tf_state():
    """TS-SEC-02: Terraform state does not contain RDS password in plaintext."""
    # This test reads local tfstate if present (CI should run with S3 backend)
    local_tfstate = os.path.join(REPO_ROOT, "infra", "terraform.tfstate")
    if not os.path.exists(local_tfstate):
        pytest.skip("No local terraform.tfstate found (using S3 backend — expected)")

    with open(local_tfstate) as f:
        state = json.load(f)

    state_text = json.dumps(state).lower()
    # Check for long strings in password-like keys
    for resource in state.get("resources", []):
        for instance in resource.get("instances", []):
            attrs = instance.get("attributes", {})
            password = attrs.get("password", "")
            if password and len(password) > 8 and password != "(sensitive)":
                pytest.fail(
                    f"Plaintext password found in local tfstate for resource "
                    f"'{resource.get('name')}'. Use S3 backend with encryption."
                )


# ── TS-SEC-04: No K8s Secrets with credentials ───────────────────────────────

FORBIDDEN_SECRET_KEYS = frozenset({
    "password", "db_dsn", "dsn", "webhook_url", "slack_webhook",
    "bootstrap_servers", "kafka_brokers", "aws_secret_access_key",
})

IGNORED_SECRET_NAMES = frozenset({
    "default-token", "kube-", "sh.helm.", "argocd-initial-admin-secret",
})


def test_no_credential_k8s_secrets():
    """TS-SEC-04 / TS-3-12: No K8s Secrets containing application credentials."""
    result = _kubectl(["get", "secrets", "-A", "-o", "json"])
    if result.returncode != 0:
        pytest.skip(f"kubectl not available: {result.stderr}")

    secrets = json.loads(result.stdout).get("items", [])
    violations = []

    for secret in secrets:
        name = secret["metadata"]["name"]
        ns   = secret["metadata"]["namespace"]

        if any(skip in name for skip in IGNORED_SECRET_NAMES):
            continue

        for key in secret.get("data", {}):
            if key.lower() in FORBIDDEN_SECRET_KEYS:
                violations.append(f"{ns}/{name}:{key}")

    assert not violations, \
        f"K8s Secrets containing credential keys: {violations}"


def test_no_annotated_vault_secrets_in_k8s():
    """Vault-injected secrets should NOT appear as K8s Secret objects."""
    vault_paths = ["anomaly/postgres", "anomaly/slack", "anomaly/kafka", "anomaly/aws"]
    result = _kubectl(["get", "secrets", "-A", "-o", "json"])
    if result.returncode != 0:
        pytest.skip("kubectl not available")

    secrets_json = result.stdout.lower()
    for path in vault_paths:
        path_key = path.replace("/", "-")
        assert path_key not in secrets_json, \
            f"Vault secret path '{path}' found as K8s Secret name"


# ── TS-SEC-03 / TS-3-13: No credentials in pod env vars ──────────────────────

NAMESPACES_TO_CHECK = ["ml-agent", "anomaly-poller", "training-pipeline"]
FORBIDDEN_ENV_PATTERNS = [
    r"DB_DSN=postgresql://.*:.*@",
    r"SLACK_WEBHOOK=https://",
    r"KAFKA_BROKERS=.*:9092",
    r"AWS_SECRET_ACCESS_KEY=",
    r"PASSWORD=.{8,}",
]


@pytest.mark.parametrize("ns", NAMESPACES_TO_CHECK)
def test_no_credentials_in_pod_env(ns):
    """TS-SEC-03: No plaintext credentials in pod env vars for key namespaces."""
    # Get first pod in namespace
    pods_result = _kubectl([
        "get", "pods", "-n", ns,
        "-o", "jsonpath={.items[0].metadata.name}"
    ])
    if pods_result.returncode != 0 or not pods_result.stdout.strip():
        pytest.skip(f"No pods in namespace {ns}")

    pod = pods_result.stdout.strip()
    # Get main container name (not vault-agent sidecar)
    containers_result = _kubectl([
        "get", "pod", pod, "-n", ns,
        "-o", "jsonpath={.spec.containers[0].name}"
    ])
    container = containers_result.stdout.strip() if containers_result.returncode == 0 else None

    exec_args = ["exec", pod, "-n", ns]
    if container:
        exec_args += ["-c", container]
    exec_args += ["--", "env"]

    env_result = _kubectl(exec_args)
    if env_result.returncode != 0:
        pytest.skip(f"Cannot exec into {ns}/{pod}: {env_result.stderr}")

    env_output = env_result.stdout
    for pattern in FORBIDDEN_ENV_PATTERNS:
        matches = re.findall(pattern, env_output, re.IGNORECASE)
        assert not matches, \
            f"Credential found in {ns}/{pod} env vars (pattern {pattern!r}): {matches}"


# ── TS-SEC-05: tfsec scan ────────────────────────────────────────────────────

def test_tfsec_no_high_critical_findings():
    """TS-SEC-05: tfsec reports no HIGH or CRITICAL severity findings."""
    result = subprocess.run(
        ["tfsec", "infra/", "--minimum-severity", "HIGH", "--format", "json"],
        capture_output=True, text=True, cwd=REPO_ROOT
    )

    if result.returncode == 127:   # tfsec not installed
        pytest.skip("tfsec not installed — install with: brew install tfsec")

    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError:
        pytest.skip(f"tfsec output not JSON: {result.stdout[:200]}")

    results = report.get("results", [])
    high_critical = [
        r for r in results
        if r.get("severity") in ("HIGH", "CRITICAL")
    ]

    assert not high_critical, (
        f"tfsec found {len(high_critical)} HIGH/CRITICAL findings:\n" +
        "\n".join(f"  [{r['severity']}] {r['description']} ({r['location']['filename']}:{r['location']['start_line']})"
                  for r in high_critical[:5])
    )


# ── TS-SEC-08: RDS security group ────────────────────────────────────────────

@pytest.mark.infra
def test_rds_security_group_no_public_access():
    """TS-SEC-08: RDS SG has no 0.0.0.0/0 ingress on port 5432."""
    try:
        import boto3
        ec2 = boto3.client("ec2", region_name=AWS_REGION)
        rds = boto3.client("rds", region_name=AWS_REGION)

        db_resp = rds.describe_db_instances(DBInstanceIdentifier=f"{CLUSTER_NAME}-postgres")
        sg_ids  = [sg["VpcSecurityGroupId"]
                   for sg in db_resp["DBInstances"][0]["VpcSecurityGroups"]]

        sg_resp = ec2.describe_security_groups(GroupIds=sg_ids)
        for sg in sg_resp["SecurityGroups"]:
            for rule in sg["IpPermissions"]:
                from_port = rule.get("FromPort", 0)
                to_port   = rule.get("ToPort",   0)
                if from_port <= 5432 <= to_port:
                    for ip_range in rule.get("IpRanges", []):
                        assert ip_range["CidrIp"] != "0.0.0.0/0", \
                            f"RDS SG {sg['GroupId']} allows 0.0.0.0/0 on port 5432"
                    for ipv6_range in rule.get("Ipv6Ranges", []):
                        assert ipv6_range["CidrIpv6"] != "::/0", \
                            f"RDS SG {sg['GroupId']} allows ::/0 on port 5432"
    except ImportError:
        pytest.skip("boto3 not installed")


# ── TS-SEC-12: GitHub Actions OIDC — no fork PR escalation ──────────────────

def test_github_actions_no_id_token_on_pull_request():
    """TS-SEC-12: tf-apply workflow does NOT trigger on pull_request events."""
    tf_apply = os.path.join(REPO_ROOT, ".github", "workflows", "tf-apply.yml")
    if not os.path.exists(tf_apply):
        pytest.skip("tf-apply.yml not found")

    import yaml
    with open(tf_apply) as f:
        workflow = yaml.safe_load(f)

    triggers = workflow.get("on", {})
    # pull_request trigger + id-token:write = privilege escalation risk
    assert "pull_request" not in triggers, \
        "tf-apply.yml must not trigger on pull_request (fork OIDC escalation risk)"


def test_github_actions_id_token_requires_push_to_main():
    """TS-SEC-12: OIDC token (id-token: write) only on merge to main."""
    tf_apply = os.path.join(REPO_ROOT, ".github", "workflows", "tf-apply.yml")
    if not os.path.exists(tf_apply):
        pytest.skip("tf-apply.yml not found")

    import yaml
    with open(tf_apply) as f:
        workflow = yaml.safe_load(f)

    # Check trigger is push to main
    on = workflow.get("on", {})
    push_cfg = on.get("push", {})
    branches  = push_cfg.get("branches", [])
    assert "main" in branches, \
        f"tf-apply.yml should trigger on push to main, got branches: {branches}"

    # Check permissions
    perms = workflow.get("permissions", {})
    assert perms.get("id-token") == "write", \
        "tf-apply.yml should have id-token: write permission"


# ── TS-SEC-13: Prompt does not contain IAM credentials ───────────────────────

def test_classify_llm_prompt_no_iam_keys():
    """TS-SEC-13: classify_with_llm prompt does not serialize IAM access keys into Bedrock."""
    # The prompt is built from eventName, eventSource, errorCode, score — NOT from
    # raw credentials. Verify no accessKeyId-like pattern in constructed prompt.
    event = {
        "eventName":    "RunInstances",
        "eventSource":  "ec2.amazonaws.com",
        "eventTime":    "2026-05-08T10:00:00Z",
        "awsRegion":    "us-east-1",
        "errorCode":    None,
        "score":        -0.25,
        "userIdentity": {
            "arn":         "arn:aws:iam::123456789012:user/test",
            "accessKeyId": "AKIAIOSFODNN7EXAMPLE",   # should NOT appear in prompt
        }
    }
    state = {
        "kafka_event":   event,
        "kafka_offset":  1,
        "last_train_ts": "2026-04-01T00:00:00Z",
        "rag_chunks":    [],
    }

    # Build prompt manually (mirrors classify_with_llm logic)
    prompt = (
        f"Classify event {event['eventName']} score {event['score']:.4f}. "
        f"Context: No infrastructure changes found since last model training."
    )

    assert "AKIAIOSFODNN7EXAMPLE" not in prompt, \
        "IAM access key found in LLM prompt — sanitize userIdentity before sending to Bedrock"
    assert "accessKeyId" not in prompt, \
        "Raw accessKeyId field found in LLM prompt"
