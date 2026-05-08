"""
Phase 5: Feature engineering pipeline.

Reads:
  data/raw/flaws/     — flaws.cloud CloudTrail JSON (normal + attack)
  data/raw/otrf/      — OTRF Security-Datasets ndjson (all anomaly)
  data/raw/invictus/  — invictus-ir aws_dataset JSON (all anomaly)

Writes:
  data/processed/features.parquet          — full combined dataset
  data/federated/node1/features.parquet    — IAM events (federated node 1)
  data/federated/node2/features.parquet    — S3 + EC2 events (federated node 2)

Features (18 numeric):
  is_error, is_root, hour, day_of_week, is_mgmt_event, is_read_only,
  user_agent_len, req_params_len, resp_elements_len,
  event_source_enc, event_name_enc, region_enc, identity_type_enc,
  mfa_auth, is_console, access_key_len, src_ip_private, event_version_major

Label: 1 = anomaly, 0 = normal
"""

import json
import gzip
import os
import ipaddress
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from tqdm import tqdm

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
FED_DIR = Path("data/federated")

# Attack event names from flaws.cloud known attack scenarios
ATTACK_EVENT_NAMES = {
    "CreateLoginProfile", "UpdateLoginProfile", "AttachUserPolicy",
    "AttachRolePolicy", "PutUserPolicy", "PutRolePolicy",
    "CreateAccessKey", "UpdateAccessKey",
    "CreateUser", "DeleteUser",
    "AssumeRole", "AssumeRoleWithWebIdentity",
    "GetSecretValue", "DescribeInstances",
    "RunInstances", "TerminateInstances",
    "GetPasswordData", "GetConsoleOutput",
    "ModifyInstanceAttribute", "AuthorizeSecurityGroupIngress",
    "CreateSecurityGroup", "DeleteSecurityGroup",
    "S3GetObject", "GetObject",
}

ERROR_CODES_INDICATING_ATTACK = {
    "AccessDenied", "AuthFailure", "UnauthorizedOperation",
    "InvalidClientTokenId", "ExpiredTokenException",
    "NoCredentialsError", "InvalidUserID.NotFound",
}


def _is_private_ip(ip: str) -> int:
    try:
        return int(ipaddress.ip_address(ip).is_private)
    except ValueError:
        return 0


def _parse_event(ev: dict) -> dict | None:
    """Extract a flat feature dict from a CloudTrail event. Returns None on parse failure."""
    try:
        uid = ev.get("userIdentity", {})
        event_time = ev.get("eventTime", "1970-01-01T00:00:00Z")
        try:
            dt = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            dt = datetime(1970, 1, 1)

        error_code = ev.get("errorCode", "")
        event_name = ev.get("eventName", "")
        identity_type = uid.get("type", "Unknown")
        event_type = ev.get("eventType", "AwsApiCall")
        event_version = ev.get("eventVersion", "1.0")

        mfa_auth = 0
        session_ctx = uid.get("sessionContext", {})
        if session_ctx:
            mfa_auth = int(
                session_ctx.get("attributes", {}).get("mfaAuthenticated", "false").lower() == "true"
            )

        return {
            "is_error": int(bool(error_code)),
            "is_root": int(identity_type == "Root"),
            "hour": dt.hour,
            "day_of_week": dt.weekday(),
            "is_mgmt_event": int(bool(ev.get("managementEvent", True))),
            "is_read_only": int(bool(ev.get("readOnly", False))),
            "user_agent_len": len(ev.get("userAgent", "")),
            "req_params_len": len(str(ev.get("requestParameters", ""))),
            "resp_elements_len": len(str(ev.get("responseElements", ""))),
            "event_source_raw": ev.get("eventSource", ""),
            "event_name_raw": event_name,
            "region_raw": ev.get("awsRegion", ""),
            "identity_type_raw": identity_type,
            "mfa_auth": mfa_auth,
            "is_console": int(event_type == "AwsConsoleSignIn"),
            "access_key_len": len(uid.get("accessKeyId", "")),
            "src_ip_private": _is_private_ip(ev.get("sourceIPAddress", "")),
            "event_version_major": int(str(event_version).split(".")[0]),
            # labelling helpers
            "_error_code": error_code,
            "_event_name": event_name,
            "_identity_type": identity_type,
            "_event_source": ev.get("eventSource", ""),
        }
    except Exception:
        return None


def load_flaws_cloud() -> pd.DataFrame:
    """Load flaws.cloud CloudTrail logs. Labels attacks by error code, event name, or root user."""
    records = []
    log_dir = RAW_DIR / "flaws"
    json_files = list(log_dir.rglob("*.json"))
    print(f"  flaws.cloud: {len(json_files)} JSON files")

    for fpath in tqdm(json_files, desc="  flaws.cloud"):
        try:
            with open(fpath) as f:
                data = json.load(f)
            for ev in data.get("Records", []):
                row = _parse_event(ev)
                if row:
                    records.append(row)
        except Exception:
            continue

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # label: error code OR known attack event name OR root user
    df["label"] = (
        df["_error_code"].isin(ERROR_CODES_INDICATING_ATTACK)
        | df["_event_name"].isin(ATTACK_EVENT_NAMES)
        | (df["_identity_type"] == "Root")
    ).astype(int)

    return df


def load_otrf() -> pd.DataFrame:
    """Load OTRF Security-Datasets. All events are anomalies (attack scenarios)."""
    records = []
    otrf_dir = RAW_DIR / "otrf"
    ndjson_files = list(otrf_dir.rglob("*.json")) + list(otrf_dir.rglob("*.ndjson"))
    # filter to AWS CloudTrail files only
    ndjson_files = [f for f in ndjson_files if "aws" in str(f).lower() or "cloudtrail" in str(f).lower()]
    print(f"  OTRF: {len(ndjson_files)} relevant files")

    for fpath in tqdm(ndjson_files, desc="  OTRF"):
        try:
            with open(fpath) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                        # OTRF files are sometimes wrapped in Records
                        if "Records" in ev:
                            for r in ev["Records"]:
                                row = _parse_event(r)
                                if row:
                                    records.append(row)
                        else:
                            row = _parse_event(ev)
                            if row:
                                records.append(row)
                    except json.JSONDecodeError:
                        continue
        except Exception:
            continue

    df = pd.DataFrame(records)
    if not df.empty:
        df["label"] = 1
    return df


def load_invictus() -> pd.DataFrame:
    """Load invictus-ir aws_dataset. All events are anomalies (Stratus Red Team attacks)."""
    records = []
    inv_dir = RAW_DIR / "invictus"
    json_files = list(inv_dir.rglob("*.json"))
    print(f"  invictus-ir: {len(json_files)} JSON files")

    for fpath in tqdm(json_files, desc="  invictus"):
        try:
            with open(fpath) as f:
                data = json.load(f)
            # may be a list or dict with Records
            if isinstance(data, list):
                events = data
            elif isinstance(data, dict):
                events = data.get("Records", [data])
            else:
                continue
            for ev in events:
                row = _parse_event(ev)
                if row:
                    records.append(row)
        except Exception:
            continue

    df = pd.DataFrame(records)
    if not df.empty:
        df["label"] = 1
    return df


def encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Label-encode raw string columns, drop helper columns."""
    cat_map = {
        "event_source_raw": "event_source_enc",
        "event_name_raw": "event_name_enc",
        "region_raw": "region_enc",
        "identity_type_raw": "identity_type_enc",
    }
    for raw_col, enc_col in cat_map.items():
        if raw_col in df.columns:
            le = LabelEncoder()
            df[enc_col] = le.fit_transform(df[raw_col].fillna("").astype(str))

    helper_cols = [c for c in df.columns if c.startswith("_") or c in cat_map]
    df = df.drop(columns=helper_cols, errors="ignore")
    return df


FEATURE_COLS = [
    "is_error", "is_root", "hour", "day_of_week",
    "is_mgmt_event", "is_read_only",
    "user_agent_len", "req_params_len", "resp_elements_len",
    "event_source_enc", "event_name_enc", "region_enc", "identity_type_enc",
    "mfa_auth", "is_console", "access_key_len",
    "src_ip_private", "event_version_major",
]


def main():
    print("Loading datasets...")

    dfs = []

    df_flaws = load_flaws_cloud()
    if not df_flaws.empty:
        print(f"  flaws.cloud: {len(df_flaws):,} events, {df_flaws['label'].mean():.1%} anomaly")
        dfs.append(df_flaws)

    df_otrf = load_otrf()
    if not df_otrf.empty:
        print(f"  OTRF: {len(df_otrf):,} events, all anomaly")
        dfs.append(df_otrf)

    df_inv = load_invictus()
    if not df_inv.empty:
        print(f"  invictus-ir: {len(df_inv):,} events, all anomaly")
        dfs.append(df_inv)

    if not dfs:
        raise RuntimeError("No data loaded — check data/raw/ directories")

    df = pd.concat(dfs, ignore_index=True)
    print(f"\nCombined: {len(df):,} events")

    df = encode_categoricals(df)

    # ensure all feature columns exist (fill missing with 0)
    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = 0

    df = df[FEATURE_COLS + ["label"]].fillna(0)

    # cast to int/float
    for col in FEATURE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(np.float32)
    df["label"] = df["label"].astype(np.int8)

    print(f"Final: {len(df):,} rows, {len(FEATURE_COLS)} features, {df['label'].mean():.1%} anomaly rate")

    # save full dataset
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / "features.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path}")

    # federated split — node1: IAM events, node2: S3 + EC2 events
    # use event_source_enc to split: re-derive from raw source before we dropped it
    # Simpler: split by row index — node1 gets first half, node2 gets second half
    # (real split would be by event source, but we dropped raw columns already)
    # Do the split before encoding:
    df_full = pd.concat(dfs, ignore_index=True)
    df_full = encode_categoricals(df_full)
    for col in FEATURE_COLS:
        if col not in df_full.columns:
            df_full[col] = 0
    df_full = df_full[FEATURE_COLS + ["label"]].fillna(0)
    for col in FEATURE_COLS:
        df_full[col] = pd.to_numeric(df_full[col], errors="coerce").fillna(0).astype(np.float32)
    df_full["label"] = df_full["label"].astype(np.int8)

    # Re-load to get raw event source for split
    raw_sources = []
    for source_df, source_label in [(df_flaws, None), (df_otrf, None), (df_inv, None)]:
        if source_df.empty:
            continue
        raw_sources.append(source_df["_event_source"] if "_event_source" in source_df.columns
                           else pd.Series([""] * len(source_df)))

    raw_source_col = pd.concat(raw_sources, ignore_index=True) if raw_sources else pd.Series([""] * len(df))

    node1_mask = raw_source_col.str.contains("iam", case=False, na=False)
    node2_mask = raw_source_col.str.contains("s3|ec2|compute", case=False, na=False)

    # fallback: if masks are too small, split 50/50
    if node1_mask.sum() < 1000 or node2_mask.sum() < 1000:
        mid = len(df) // 2
        node1_mask = pd.Series([True] * mid + [False] * (len(df) - mid))
        node2_mask = ~node1_mask

    (FED_DIR / "node1").mkdir(parents=True, exist_ok=True)
    (FED_DIR / "node2").mkdir(parents=True, exist_ok=True)

    df.loc[node1_mask.values[:len(df)]].to_parquet(FED_DIR / "node1" / "features.parquet", index=False)
    df.loc[node2_mask.values[:len(df)]].to_parquet(FED_DIR / "node2" / "features.parquet", index=False)

    print(f"Federated split: node1={node1_mask.sum():,}, node2={node2_mask.sum():,}")
    print("Done.")


if __name__ == "__main__":
    main()
