"""
Generate synthetic CloudTrail-feature parquet files for cluster testing.

Outputs:
  /tmp/anomaly-test-data/processed/features.parquet       — full dataset (trainer)
  /tmp/anomaly-test-data/federated/node1/features.parquet — IAM-heavy node
  /tmp/anomaly-test-data/federated/node2/features.parquet — S3/EC2 node

Features match trainer.py FEATURE_COLS (18 numeric + label).
Normal events: routine API calls (low error rate, business hours, no root).
Anomaly events: mimics attack patterns (errors, root, off-hours, long params).
"""

import numpy as np
import pandas as pd
from pathlib import Path

RNG = np.random.default_rng(42)
OUT = Path("/tmp/anomaly-test-data")

FEATURE_COLS = [
    "is_error", "is_root", "hour", "day_of_week",
    "is_mgmt_event", "is_read_only",
    "user_agent_len", "req_params_len", "resp_elements_len",
    "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
    "mfa_auth", "is_console", "access_key_len",
    "src_ip_private", "event_version_major",
]


def gen_normal(n: int) -> pd.DataFrame:
    """Routine API calls: business hours, low error, read-only, no root."""
    return pd.DataFrame({
        "is_error":           RNG.choice([0, 1], n, p=[0.97, 0.03]).astype(np.float32),
        "is_root":            RNG.choice([0, 1], n, p=[0.995, 0.005]).astype(np.float32),
        "hour":               RNG.integers(8, 18, n).astype(np.float32),
        "day_of_week":        RNG.integers(0, 5, n).astype(np.float32),
        "is_mgmt_event":      RNG.choice([0, 1], n, p=[0.3, 0.7]).astype(np.float32),
        "is_read_only":       RNG.choice([0, 1], n, p=[0.4, 0.6]).astype(np.float32),
        "user_agent_len":     RNG.integers(20, 80, n).astype(np.float32),
        "req_params_len":     RNG.integers(10, 200, n).astype(np.float32),
        "resp_elements_len":  RNG.integers(50, 500, n).astype(np.float32),
        "event_source_enc":   RNG.integers(0, 8, n).astype(np.float32),
        "event_name_enc":     RNG.integers(0, 50, n).astype(np.float32),
        "region_enc":         RNG.integers(0, 5, n).astype(np.float32),
        "identity_type_enc":  RNG.choice([1, 2], n).astype(np.float32),  # IAMUser/AssumedRole
        "mfa_auth":           RNG.choice([0, 1], n, p=[0.3, 0.7]).astype(np.float32),
        "is_console":         RNG.choice([0, 1], n, p=[0.8, 0.2]).astype(np.float32),
        "access_key_len":     RNG.integers(16, 20, n).astype(np.float32),
        "src_ip_private":     RNG.choice([0, 1], n, p=[0.4, 0.6]).astype(np.float32),
        "event_version_major": np.ones(n, dtype=np.float32),
        "label":              np.zeros(n, dtype=np.int8),
    })


def gen_anomaly(n: int) -> pd.DataFrame:
    """Attack patterns: off-hours, errors, root, long params, no MFA, public IP."""
    return pd.DataFrame({
        "is_error":           RNG.choice([0, 1], n, p=[0.4, 0.6]).astype(np.float32),
        "is_root":            RNG.choice([0, 1], n, p=[0.5, 0.5]).astype(np.float32),
        "hour":               RNG.choice(list(range(0, 6)) + list(range(22, 24)), n).astype(np.float32),
        "day_of_week":        RNG.integers(0, 7, n).astype(np.float32),
        "is_mgmt_event":      np.ones(n, dtype=np.float32),
        "is_read_only":       np.zeros(n, dtype=np.float32),
        "user_agent_len":     RNG.integers(0, 15, n).astype(np.float32),
        "req_params_len":     RNG.integers(500, 2000, n).astype(np.float32),
        "resp_elements_len":  RNG.integers(0, 50, n).astype(np.float32),
        "event_source_enc":   RNG.integers(0, 8, n).astype(np.float32),
        "event_name_enc":     RNG.integers(50, 100, n).astype(np.float32),  # attack event names
        "region_enc":         RNG.integers(5, 10, n).astype(np.float32),    # unusual regions
        "identity_type_enc":  RNG.choice([0, 3], n).astype(np.float32),     # Root/Unknown
        "mfa_auth":           np.zeros(n, dtype=np.float32),
        "is_console":         RNG.choice([0, 1], n, p=[0.5, 0.5]).astype(np.float32),
        "access_key_len":     RNG.integers(0, 5, n).astype(np.float32),
        "src_ip_private":     np.zeros(n, dtype=np.float32),  # public IPs
        "event_version_major": np.ones(n, dtype=np.float32),
        "label":              np.ones(n, dtype=np.int8),
    })


def make_dataset(n_normal: int, n_anomaly: int) -> pd.DataFrame:
    df = pd.concat([gen_normal(n_normal), gen_anomaly(n_anomaly)], ignore_index=True)
    return df.sample(frac=1, random_state=42).reset_index(drop=True)


if __name__ == "__main__":
    # Full dataset: 10k normal, 1k anomaly (~9% contamination)
    full = make_dataset(10_000, 1_000)
    (OUT / "processed").mkdir(parents=True, exist_ok=True)
    full.to_parquet(OUT / "processed/features.parquet", index=False)
    print(f"full: {len(full):,} rows, {full['label'].mean():.1%} anomaly")

    # Node1: IAM-heavy — more root + MFA events
    node1 = make_dataset(4_000, 400)
    (OUT / "federated/node1").mkdir(parents=True, exist_ok=True)
    node1.to_parquet(OUT / "federated/node1/features.parquet", index=False)
    print(f"node1: {len(node1):,} rows, {node1['label'].mean():.1%} anomaly")

    # Node2: S3/EC2-heavy — more read ops
    node2 = make_dataset(4_000, 400)
    node2["is_read_only"] = (node2["is_read_only"] * 0.8 + 0.2).clip(0, 1)
    (OUT / "federated/node2").mkdir(parents=True, exist_ok=True)
    node2.to_parquet(OUT / "federated/node2/features.parquet", index=False)
    print(f"node2: {len(node2):,} rows, {node2['label'].mean():.1%} anomaly")

    print(f"\nData written to {OUT}/")
