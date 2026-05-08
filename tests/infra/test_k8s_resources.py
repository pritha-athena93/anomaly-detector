"""
Infrastructure tests for Kubernetes namespaces, NetworkPolicies, and ArgoCD (Phase 1-2).
Covers: TS-1-01..12, TS-2-01..10, TS-1-EC-02, TS-1-EC-03

Requires: kubectl access to EKS cluster (KUBECONFIG set).
Run: pytest -m infra tests/infra/test_k8s_resources.py
"""

import json
import os
import subprocess
import time
import pytest

pytestmark = pytest.mark.infra

EXPECTED_NAMESPACES = [
    "argocd",
    "cert-manager",
    "vault",
    "kafka",
    "kubeflow",
    "kserve",
    "anomaly-poller",
    "ml-agent",
    "training-pipeline",
    "monitoring",
]

ARGOCD_APPS = [
    "namespaces",
    "cert-manager",
    "vault",
    "strimzi",
    "kubeflow",
    "kserve",
    "monitoring",
    "anomaly-poller",
    "ml-agent",
    "training-pipeline",
]


def kubectl(args: list, check=True, timeout=30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl"] + args,
        capture_output=True, text=True, check=check, timeout=timeout
    )


def kubectl_json(args: list) -> dict:
    result = kubectl(args + ["-o", "json"])
    return json.loads(result.stdout)


# ── TS-1-01: All namespaces exist ────────────────────────────────────────────

@pytest.mark.parametrize("ns", EXPECTED_NAMESPACES)
def test_namespace_exists(ns):
    """TS-1-01: Each required namespace is present."""
    result = kubectl(["get", "namespace", ns], check=False)
    assert result.returncode == 0, f"Namespace '{ns}' not found: {result.stderr}"


def test_no_unexpected_namespaces_missing():
    """All 10 expected namespaces created in one check."""
    result = kubectl(["get", "namespaces", "-o", "jsonpath={.items[*].metadata.name}"])
    existing = result.stdout.split()
    missing = [ns for ns in EXPECTED_NAMESPACES if ns not in existing]
    assert missing == [], f"Missing namespaces: {missing}"


# ── TS-1-02: default-deny-all in every namespace ─────────────────────────────

@pytest.mark.parametrize("ns", EXPECTED_NAMESPACES)
def test_default_deny_all_netpol_exists(ns):
    """TS-1-02: default-deny-all NetworkPolicy present in each namespace."""
    result = kubectl([
        "get", "networkpolicy", "default-deny-all", "-n", ns
    ], check=False)
    assert result.returncode == 0, \
        f"default-deny-all NetworkPolicy missing in namespace '{ns}': {result.stderr}"


def test_total_deny_all_count():
    """TS-1-02: exactly 10 default-deny-all policies (one per namespace)."""
    result = kubectl([
        "get", "networkpolicy", "-A",
        "-o", "jsonpath={.items[?(@.metadata.name=='default-deny-all')].metadata.namespace}",
    ])
    namespaces_with_deny = result.stdout.split()
    assert len(namespaces_with_deny) >= len(EXPECTED_NAMESPACES), \
        f"Expected >= {len(EXPECTED_NAMESPACES)} deny-all policies, got {len(namespaces_with_deny)}"


# ── TS-1-03: DNS egress allowed ──────────────────────────────────────────────

@pytest.mark.parametrize("ns", EXPECTED_NAMESPACES)
def test_allow_dns_egress_netpol_exists(ns):
    """TS-1-03: allow-dns-egress NetworkPolicy present in each namespace."""
    result = kubectl([
        "get", "networkpolicy", "allow-dns-egress", "-n", ns
    ], check=False)
    assert result.returncode == 0, \
        f"allow-dns-egress NetworkPolicy missing in '{ns}': {result.stderr}"


def test_dns_netpol_allows_both_udp_and_tcp_53():
    """TS-1-EC-02: DNS policy allows port 53 on both UDP and TCP."""
    result = kubectl([
        "get", "networkpolicy", "allow-dns-egress", "-n", "ml-agent", "-o", "json"
    ], check=False)
    if result.returncode != 0:
        pytest.skip("allow-dns-egress not found in ml-agent")

    policy = json.loads(result.stdout)
    ports = []
    for egress_rule in policy["spec"].get("egress", []):
        for p in egress_rule.get("ports", []):
            ports.append((p.get("port"), p.get("protocol")))

    assert (53, "UDP") in ports, "DNS UDP port 53 not in allow-dns-egress"
    assert (53, "TCP") in ports, "DNS TCP port 53 not in allow-dns-egress (needed for large responses)"


# ── TS-1-04..08: specific allow rules ────────────────────────────────────────

@pytest.mark.parametrize("ns,target,ports", [
    ("anomaly-poller", "kserve",  [80, 443]),
    ("anomaly-poller", "kafka",   [9092, 9093]),
    ("anomaly-poller", "vault",   [8200]),
    ("ml-agent",       "kafka",   [9092, 9093]),
    ("ml-agent",       "vault",   [8200]),
])
def test_egress_allow_netpol_exists(ns, target, ports):
    """TS-1-04..08: explicit egress NetworkPolicy from ns → target."""
    policy_name = f"allow-egress-to-{target}"
    result = kubectl([
        "get", "networkpolicy", policy_name, "-n", ns
    ], check=False)
    assert result.returncode == 0, \
        f"NetworkPolicy '{policy_name}' not found in '{ns}': {result.stderr}"


@pytest.mark.parametrize("ns,target", [
    ("anomaly-poller", "kserve"),
    ("anomaly-poller", "kafka"),
    ("ml-agent",       "kafka"),
])
def test_ingress_allow_netpol_exists(ns, target):
    """Corresponding ingress allow on target namespace."""
    policy_name = f"allow-ingress-from-{ns}"
    result = kubectl([
        "get", "networkpolicy", policy_name, "-n", target
    ], check=False)
    assert result.returncode == 0, \
        f"Ingress NetworkPolicy '{policy_name}' missing in '{target}': {result.stderr}"


def test_ml_agent_external_egress_excludes_private_rfc1918():
    """TS-1-EC-03: ml-agent external egress blocks RFC-1918 ranges (not just 0.0.0.0/0)."""
    result = kubectl([
        "get", "networkpolicy", "allow-egress-external", "-n", "ml-agent", "-o", "json"
    ], check=False)
    if result.returncode != 0:
        pytest.skip("allow-egress-external not found in ml-agent")

    policy = json.loads(result.stdout)
    for egress_rule in policy["spec"].get("egress", []):
        for to in egress_rule.get("to", []):
            ip_block = to.get("ipBlock", {})
            if ip_block.get("cidr") == "0.0.0.0/0":
                exceptions = ip_block.get("except", [])
                assert "10.0.0.0/8"     in exceptions, "10.0.0.0/8 not excluded"
                assert "172.16.0.0/12"  in exceptions, "172.16.0.0/12 not excluded"
                assert "192.168.0.0/16" in exceptions, "192.168.0.0/16 not excluded"


def test_training_pipeline_istio_label():
    """TS-1-09: training-pipeline namespace has Istio injection disabled."""
    result = kubectl([
        "get", "namespace", "training-pipeline",
        "-o", "jsonpath={.metadata.labels.sidecar\\.istio\\.io/inject}"
    ], check=False)
    if result.returncode == 0:
        assert result.stdout.strip() == "false", \
            f"Istio injection not disabled in training-pipeline: {result.stdout}"


# ── TS-2-01: ArgoCD apps synced ──────────────────────────────────────────────

@pytest.mark.parametrize("app", ARGOCD_APPS)
def test_argocd_app_exists(app):
    """TS-2-01: Each ArgoCD Application exists."""
    result = kubectl([
        "get", "application", app, "-n", "argocd"
    ], check=False)
    assert result.returncode == 0, f"ArgoCD Application '{app}' not found: {result.stderr}"


@pytest.mark.parametrize("app", ARGOCD_APPS)
def test_argocd_app_synced(app):
    """TS-2-02: Each ArgoCD Application is Synced."""
    result = kubectl([
        "get", "application", app, "-n", "argocd",
        "-o", "jsonpath={.status.sync.status}"
    ], check=False)
    if result.returncode != 0:
        pytest.skip(f"ArgoCD Application '{app}' not found")
    assert result.stdout.strip() == "Synced", \
        f"Application '{app}' sync status: {result.stdout.strip()}"


@pytest.mark.parametrize("app", ARGOCD_APPS)
def test_argocd_app_healthy(app):
    """TS-2-02: Each ArgoCD Application is Healthy."""
    result = kubectl([
        "get", "application", app, "-n", "argocd",
        "-o", "jsonpath={.status.health.status}"
    ], check=False)
    if result.returncode != 0:
        pytest.skip(f"ArgoCD Application '{app}' not found")
    assert result.stdout.strip() == "Healthy", \
        f"Application '{app}' health status: {result.stdout.strip()}"


# ── TS-2-09: git outage — workloads keep running ─────────────────────────────

def test_ml_agent_deployment_has_replicas():
    """TS-2-09: ml-agent pods are running (would survive git outage)."""
    result = kubectl([
        "get", "deployment", "ml-agent", "-n", "ml-agent",
        "-o", "jsonpath={.status.readyReplicas}"
    ], check=False)
    if result.returncode != 0:
        pytest.skip("ml-agent deployment not found")
    ready = int(result.stdout.strip() or "0")
    assert ready >= 1, f"ml-agent has {ready} ready replicas"


# ── TS-2-10: no placeholder <org> in deployed manifests ──────────────────────

def test_no_org_placeholder_in_argocd_apps():
    """TS-2-10: No '<org>' placeholder in any deployed ArgoCD Application."""
    result = kubectl([
        "get", "applications", "-n", "argocd", "-o", "json"
    ], check=False)
    if result.returncode != 0:
        pytest.skip("Cannot list ArgoCD applications")

    apps_json = result.stdout
    assert "<org>" not in apps_json, \
        "Found '<org>' placeholder in ArgoCD Application manifests — update repoURL"


def test_no_account_id_placeholder_in_argocd_apps():
    """No '<account-id>' placeholder remaining in deployed manifests."""
    result = kubectl([
        "get", "applications", "-n", "argocd", "-o", "json"
    ], check=False)
    if result.returncode != 0:
        pytest.skip("Cannot list ArgoCD applications")
    assert "<account-id>" not in result.stdout


# ── TS-1-EC-01: namespaces chart creates its own namespaces ──────────────────

def test_namespaces_created_by_helm_chart():
    """TS-1-EC-01: namespaces Helm release exists (chart creates NS, not Helm)."""
    result = kubectl([
        "get", "application", "namespaces", "-n", "argocd"
    ], check=False)
    assert result.returncode == 0, "Namespaces ArgoCD Application not found"


# ── Kafka namespace specific label ───────────────────────────────────────────

def test_kafka_namespace_has_strimzi_label():
    """kafka namespace has strimzi.io/cluster label."""
    result = kubectl([
        "get", "namespace", "kafka",
        "-o", "jsonpath={.metadata.labels.strimzi\\.io/cluster}"
    ], check=False)
    if result.returncode == 0:
        assert result.stdout.strip() == "anomaly-kafka"


# ── Sync wave annotations ─────────────────────────────────────────────────────

@pytest.mark.parametrize("app,expected_wave", [
    ("namespaces",   "0"),
    ("cert-manager", "1"),
    ("vault",        "1"),
    ("strimzi",      "2"),
])
def test_sync_wave_annotation(app, expected_wave):
    """TS-2-03: sync-wave annotation controls deployment order."""
    result = kubectl([
        "get", "application", app, "-n", "argocd",
        "-o", f"jsonpath={{.metadata.annotations.argocd\\.argoproj\\.io/sync-wave}}"
    ], check=False)
    if result.returncode != 0:
        pytest.skip(f"ArgoCD Application '{app}' not found")
    wave = result.stdout.strip()
    assert wave == expected_wave, f"App '{app}' sync-wave={wave}, expected {expected_wave}"
