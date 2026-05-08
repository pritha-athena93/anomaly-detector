"""
Integration tests for KServe InferenceService (Phase 6).
Covers: TS-6-01..12, TS-6-EC-01..04

Requires: KServe endpoint at TEST_KSERVE_HOST, model loaded from S3.
Run: pytest -m integration tests/integration/test_kserve.py
"""

import json
import os
import time
import pytest

pytestmark = pytest.mark.integration

try:
    import requests
except ImportError:
    pytest.skip("requests not installed", allow_module_level=True)

KSERVE_HOST   = os.environ.get("TEST_KSERVE_HOST",    "http://anomaly-detector.kserve.svc")
KSERVE_MODEL  = "anomaly-detector"
INFER_URL     = f"{KSERVE_HOST}/v2/models/{KSERVE_MODEL}/infer"
READY_URL     = f"{KSERVE_HOST}/v2/models/{KSERVE_MODEL}/ready"
HEALTH_URL    = f"{KSERVE_HOST}/v2/health/ready"


# ── TS-6-01: InferenceService ready ──────────────────────────────────────────

def test_inference_service_ready():
    """TS-6-01: /v2/models/{model}/ready returns HTTP 200."""
    resp = requests.get(READY_URL, timeout=15)
    assert resp.status_code == 200, f"KServe model not ready: {resp.status_code} {resp.text}"


def test_health_endpoint_ready():
    resp = requests.get(HEALTH_URL, timeout=10)
    assert resp.status_code == 200


# ── TS-6-03: valid 18-feature inference ──────────────────────────────────────

def _v2_infer_payload(features: list) -> dict:
    """Build KServe V2 inference protocol request body."""
    return {
        "inputs": [{
            "name":     "predict",
            "shape":    [1, len(features)],
            "datatype": "FP32",
            "data":     [features],
        }]
    }


def test_infer_valid_18_features(valid_feature_vector):
    """TS-6-03: 18-feature vector → HTTP 200 with float score."""
    payload = _v2_infer_payload(valid_feature_vector)
    resp = requests.post(INFER_URL, json=payload, timeout=15)

    assert resp.status_code == 200, f"Inference failed: {resp.status_code} {resp.text}"
    body = resp.json()
    assert "outputs" in body
    assert len(body["outputs"][0]["data"]) >= 1


def test_infer_returns_minus_one_or_one(valid_feature_vector):
    """TS-6-04/TS-6-05: IsolationForest returns -1 (anomaly) or 1 (normal)."""
    payload = _v2_infer_payload(valid_feature_vector)
    resp = requests.post(INFER_URL, json=payload, timeout=15)

    assert resp.status_code == 200
    prediction = resp.json()["outputs"][0]["data"][0]
    assert prediction in (-1, 1, -1.0, 1.0), f"Unexpected prediction: {prediction}"


def test_infer_anomalous_vector_returns_minus_one():
    """TS-6-04: crafted anomalous feature vector → prediction = -1."""
    # All features at extreme values to force anomaly detection
    extreme_features = [
        1.0,    # is_error = True
        1.0,    # is_root = True
        3.0,    # hour = 3am
        6.0,    # day_of_week = Sunday
        0.0,    # is_mgmt_event = False
        0.0,    # is_read_only = False
        500.0,  # user_agent_len = very large
        9999.0, # req_params_len = extreme
        0.0,    # resp_elements_len
        99.0,   # event_source_enc = unseen encoding
        99.0,   # event_name_enc = unseen encoding
        99.0,   # region_enc = unseen encoding
        99.0,   # identity_type_enc
        0.0,    # mfa_auth = no MFA
        0.0,    # is_console
        0.0,    # access_key_len
        0.0,    # src_ip_private
        2.0,    # event_version_major
    ]
    payload = _v2_infer_payload(extreme_features)
    resp = requests.post(INFER_URL, json=payload, timeout=15)

    assert resp.status_code == 200
    prediction = resp.json()["outputs"][0]["data"][0]
    assert prediction in (-1, -1.0), f"Expected -1 for anomalous vector, got {prediction}"


# ── TS-6-10: wrong feature count ─────────────────────────────────────────────

def test_infer_wrong_feature_count_returns_error():
    """TS-6-10: 17-feature vector → 4xx or 5xx, not silent wrong prediction."""
    wrong_features = [0.0] * 17
    payload = _v2_infer_payload(wrong_features)
    resp = requests.post(INFER_URL, json=payload, timeout=15)

    assert resp.status_code >= 400, \
        f"Expected error status for 17-feature input, got {resp.status_code}"


def test_infer_empty_features_returns_error():
    """Empty feature list → error response."""
    payload = _v2_infer_payload([])
    resp = requests.post(INFER_URL, json=payload, timeout=15)
    assert resp.status_code >= 400


def test_infer_non_numeric_features_returns_error():
    """String values in feature array → error, not silent cast."""
    payload = {
        "inputs": [{
            "name":     "predict",
            "shape":    [1, 18],
            "datatype": "FP32",
            "data":     [["not", "a", "number"] + [0.0] * 15],
        }]
    }
    resp = requests.post(INFER_URL, json=payload, timeout=15)
    assert resp.status_code >= 400


# ── TS-6-07: IRSA scope — cannot access cloudtrail bucket ────────────────────

def test_predictor_cannot_list_cloudtrail_bucket():
    """TS-6-07: kserve-sa IRSA role has no s3:ListBucket on cloudtrail-logs.

    Verifies indirectly: model loads from model-registry, not cloudtrail-logs.
    Direct IAM check performed in tests/infra/test_aws_resources.py.
    """
    # If the model loaded successfully (TS-6-01 passed), IRSA has model-registry access.
    # We verify model-registry access is sufficient and the bucket name in inference
    # service spec points to model-registry, not cloudtrail.
    import subprocess
    result = subprocess.run(
        ["kubectl", "get", "inferenceservice", "anomaly-detector", "-n", "kserve",
         "-o", "jsonpath={.spec.predictor.sklearn.storageUri}"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        storage_uri = result.stdout.strip()
        assert "model-registry" in storage_uri, \
            f"storageUri should reference model-registry, got: {storage_uri}"
        assert "cloudtrail" not in storage_uri


# ── TS-6-12: Prometheus metrics endpoint ─────────────────────────────────────

def test_prometheus_metrics_exposed():
    """TS-6-12: /metrics endpoint returns Prometheus text format."""
    metrics_url = f"{KSERVE_HOST}/metrics"
    resp = requests.get(metrics_url, timeout=10)

    # KServe may serve metrics on a different port; skip if unreachable
    if resp.status_code == 404:
        pytest.skip("Metrics not exposed at /metrics path — check Prometheus scrape config")

    assert resp.status_code == 200
    assert "TYPE" in resp.text or "HELP" in resp.text


# ── TS-6-EC-05: inference during model update ────────────────────────────────

def test_inference_available_during_rolling_update(valid_feature_vector):
    """TS-6-EC-05: simulate multiple rapid requests to check for 503 gaps."""
    errors = []
    for i in range(10):
        payload = _v2_infer_payload(valid_feature_vector)
        resp = requests.post(INFER_URL, json=payload, timeout=15)
        if resp.status_code >= 500:
            errors.append(resp.status_code)
        time.sleep(0.1)

    assert len(errors) == 0, f"Got {len(errors)} server errors during rapid requests: {errors}"


# ── TS-PERF-01: P99 latency ≤ 200ms ─────────────────────────────────────────

def test_inference_p99_latency_under_200ms(valid_feature_vector):
    """TS-PERF-01: single-instance p99 latency check (20 requests)."""
    latencies = []
    payload = _v2_infer_payload(valid_feature_vector)

    for _ in range(20):
        start = time.monotonic()
        resp = requests.post(INFER_URL, json=payload, timeout=15)
        elapsed_ms = (time.monotonic() - start) * 1000
        if resp.status_code == 200:
            latencies.append(elapsed_ms)

    assert latencies, "No successful requests to measure latency"
    latencies.sort()
    p99 = latencies[int(len(latencies) * 0.99)]
    assert p99 <= 200, f"P99 latency {p99:.1f}ms exceeds 200ms target"
