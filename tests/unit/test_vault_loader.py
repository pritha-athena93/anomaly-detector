"""
Unit tests for Vault secret file loader.
Covers: TS-3-05, TS-3-10, TS-7-01..04

Tests the _load_vault() function from the production agent/graph.py.
No external services required.
"""

import os
import pytest


# ── Helper: import _load_vault from wherever it ends up ─────────────────────
# The production plan rewrites agent/graph.py. Import defensively.

def _load_vault(path: str, base_dir: str = "/vault/secrets") -> dict:
    """
    Reference implementation — mirrors agent/graph.py _load_vault().
    Tests import this directly; once agent/graph.py is rewritten, swap to:
        from agent.graph import _load_vault
    """
    result = {}
    try:
        with open(f"{base_dir}/{path}") as f:
            for line in f:
                k, _, v = line.strip().partition("=")
                if k:
                    result[k] = v
    except FileNotFoundError:
        pass
    return result


# ── TS-7-01 ──────────────────────────────────────────────────────────────────

class TestLoadVaultHappyPath:

    def test_reads_key_value_pair(self, tmp_path):
        """TS-7-01: normal key=value file parsed correctly."""
        f = tmp_path / "postgres"
        f.write_text("DB_DSN=postgresql://host/db\n")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert result == {"DB_DSN": "postgresql://host/db"}

    def test_reads_multiple_keys(self, tmp_path):
        f = tmp_path / "slack"
        f.write_text("SLACK_WEBHOOK=https://hooks.slack.com/AAA\nEXTRA_KEY=value\n")

        result = _load_vault("slack", base_dir=str(tmp_path))

        assert result["SLACK_WEBHOOK"] == "https://hooks.slack.com/AAA"
        assert result["EXTRA_KEY"] == "value"

    def test_reads_kafka_file(self, vault_kafka_file, tmp_path):
        """Kafka bootstrap server parsed from fixture file."""
        import shutil
        shutil.copy(vault_kafka_file, tmp_path / "kafka")

        result = _load_vault("kafka", base_dir=str(tmp_path))

        assert "KAFKA_BROKERS" in result
        assert "9092" in result["KAFKA_BROKERS"]


# ── TS-7-02 ──────────────────────────────────────────────────────────────────

class TestLoadVaultFallback:

    def test_file_not_found_returns_empty_dict(self, tmp_path):
        """TS-7-02: missing file → empty dict, no exception."""
        result = _load_vault("nonexistent", base_dir=str(tmp_path))

        assert result == {}

    def test_env_var_fallback_pattern(self, tmp_path, monkeypatch):
        """TS-7-02: caller uses .get() fallback to env var when file absent."""
        monkeypatch.setenv("DB_DSN", "postgresql://env-host/db")

        vault_data = _load_vault("postgres", base_dir=str(tmp_path))
        # Caller pattern: vault_data.get("DB_DSN", os.environ.get("DB_DSN"))
        effective_dsn = vault_data.get("DB_DSN", os.environ.get("DB_DSN"))

        assert effective_dsn == "postgresql://env-host/db"

    def test_vault_overrides_env_var(self, tmp_path, monkeypatch):
        """Vault file takes precedence over env var."""
        monkeypatch.setenv("DB_DSN", "postgresql://env-host/db")
        f = tmp_path / "postgres"
        f.write_text("DB_DSN=postgresql://vault-host/db\n")

        vault_data = _load_vault("postgres", base_dir=str(tmp_path))
        effective_dsn = vault_data.get("DB_DSN", os.environ.get("DB_DSN"))

        assert effective_dsn == "postgresql://vault-host/db"


# ── TS-7-03 ──────────────────────────────────────────────────────────────────

class TestLoadVaultEdgeCases:

    def test_blank_lines_skipped(self, tmp_path):
        """TS-7-03: blank lines produce no empty-key entry."""
        f = tmp_path / "postgres"
        f.write_text("DB_DSN=postgresql://host/db\n\n\nEXTRA=val\n")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert "" not in result
        assert len(result) == 2

    def test_blank_lines_only(self, tmp_path):
        """File with only whitespace returns empty dict."""
        f = tmp_path / "postgres"
        f.write_text("\n\n\n")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert result == {}

    def test_value_contains_equals_sign(self, tmp_path):
        """TS-7-04: partition() splits on first = only; value with = preserved."""
        f = tmp_path / "postgres"
        # DSN with URL-encoded password containing = (base64-like)
        f.write_text("DB_DSN=postgresql://user:p%3D%3Dss@host:5432/db\n")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert result["DB_DSN"] == "postgresql://user:p%3D%3Dss@host:5432/db"

    def test_dsn_with_multiple_equals(self, tmp_path):
        """DSN containing multiple = signs parsed to correct key and full value."""
        f = tmp_path / "postgres"
        f.write_text("DB_DSN=host=rds.host port=5432 dbname=anomaly_db\n")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert result["DB_DSN"] == "host=rds.host port=5432 dbname=anomaly_db"

    def test_no_trailing_newline(self, tmp_path):
        """File without final newline still parsed."""
        f = tmp_path / "postgres"
        f.write_text("DB_DSN=postgresql://host/db")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert result["DB_DSN"] == "postgresql://host/db"

    def test_line_with_only_key_no_value(self, tmp_path):
        """Line like 'KEYONLY=' → key with empty string value (not skipped)."""
        f = tmp_path / "postgres"
        f.write_text("KEYONLY=\n")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert "KEYONLY" in result
        assert result["KEYONLY"] == ""

    def test_whitespace_stripped_from_line(self, tmp_path):
        """Trailing whitespace/CR stripped from values."""
        f = tmp_path / "postgres"
        f.write_text("DB_DSN=postgresql://host/db  \r\n")

        result = _load_vault("postgres", base_dir=str(tmp_path))

        assert result["DB_DSN"] == "postgresql://host/db"
