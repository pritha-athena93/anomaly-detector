"""
Phase 5: Verify processed dataset meets quality gates before federated training.

Checks:
  - At least 100k rows total
  - Anomaly rate between 3% and 30%
  - Zero null values in any feature column
  - All 18 feature columns present
  - Both federated splits non-empty
  - Feature ranges sane (hours 0-23, day_of_week 0-6, etc.)
"""

from pathlib import Path
import sys
import pandas as pd
import numpy as np

FEATURE_COLS = [
    "is_error", "is_root", "hour", "day_of_week",
    "is_mgmt_event", "is_read_only",
    "user_agent_len", "req_params_len", "resp_elements_len",
    "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
    "mfa_auth", "is_console", "access_key_len",
    "src_ip_private", "event_version_major",
]

PROCESSED = Path("data/processed/features.parquet")
NODE1 = Path("data/federated/node1/features.parquet")
NODE2 = Path("data/federated/node2/features.parquet")

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"

failures = 0


def check(condition: bool, message: str):
    global failures
    status = PASS if condition else FAIL
    print(f"  [{status}] {message}")
    if not condition:
        failures += 1


print("=== Dataset Verification ===\n")

# ---- full dataset ----
print("Full dataset:")
if not PROCESSED.exists():
    print(f"  [{FAIL}] {PROCESSED} not found")
    sys.exit(1)

df = pd.read_parquet(PROCESSED)
anomaly_rate = df["label"].mean()

check(len(df) >= 100_000, f"Row count {len(df):,} >= 100,000")
check(0.03 <= anomaly_rate <= 0.30, f"Anomaly rate {anomaly_rate:.1%} in [3%, 30%]")
check(df[FEATURE_COLS].isnull().sum().sum() == 0, "Zero nulls in feature columns")
check(all(c in df.columns for c in FEATURE_COLS), f"All {len(FEATURE_COLS)} feature columns present")
check(df["hour"].between(0, 23).all(), "hour in [0, 23]")
check(df["day_of_week"].between(0, 6).all(), "day_of_week in [0, 6]")
check(df["is_error"].isin([0, 1]).all(), "is_error is binary")
check(df["is_root"].isin([0, 1]).all(), "is_root is binary")
check(df["label"].isin([0, 1]).all(), "label is binary")

# ---- federated splits ----
print("\nFederated splits:")
for path, name in [(NODE1, "node1"), (NODE2, "node2")]:
    if not path.exists():
        print(f"  [{FAIL}] {path} not found")
        failures += 1
        continue
    split = pd.read_parquet(path)
    check(len(split) >= 1_000, f"{name}: {len(split):,} rows >= 1,000")
    check(split["label"].isin([0, 1]).all(), f"{name}: label is binary")
    check(split[FEATURE_COLS].isnull().sum().sum() == 0, f"{name}: zero nulls")

# ---- summary ----
print(f"\nSummary:")
print(f"  Total rows:    {len(df):,}")
print(f"  Anomaly rate:  {anomaly_rate:.2%}")
print(f"  Features:      {len(FEATURE_COLS)}")
node1 = pd.read_parquet(NODE1) if NODE1.exists() else pd.DataFrame()
node2 = pd.read_parquet(NODE2) if NODE2.exists() else pd.DataFrame()
print(f"  Node1 rows:    {len(node1):,}")
print(f"  Node2 rows:    {len(node2):,}")

print()
if failures:
    print(f"{failures} check(s) FAILED")
    sys.exit(1)
else:
    print("All checks passed.")
