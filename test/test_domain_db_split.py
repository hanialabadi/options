"""
Tests for domain-split DuckDB infrastructure.

Covers: DbDomain enum, get_domain_connection(), attach_domain(),
busy_timeout PRAGMA, migration fallback to pipeline.duckdb.
"""

import duckdb
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from core.shared.data_layer.duckdb_utils import (
    DbDomain,
    _DOMAIN_PATHS,
    _open_with_retry,
    _LOCK_RETRY_ATTEMPTS,
    get_domain_connection,
    get_domain_write_connection,
    attach_domain,
    get_duckdb_connection,
)


# ---------------------------------------------------------------------------
# Phase 0: lock-retry mechanism
# ---------------------------------------------------------------------------
class TestLockRetry:
    def test_open_with_retry_succeeds_on_clean_db(self, tmp_path):
        db_path = tmp_path / "test.duckdb"
        con = _open_with_retry(str(db_path), read_only=False)
        con.execute("CREATE TABLE t (x INTEGER)")
        con.close()

        # Read-only open should work
        con2 = _open_with_retry(str(db_path), read_only=True)
        assert con2.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0
        con2.close()

    def test_retry_count_matches_config(self):
        assert _LOCK_RETRY_ATTEMPTS >= 2, "Need at least 2 attempts for retry"


# ---------------------------------------------------------------------------
# Phase 1: DbDomain enum
# ---------------------------------------------------------------------------
class TestDbDomain:
    def test_all_domains_have_paths(self):
        for domain in DbDomain:
            assert domain in _DOMAIN_PATHS, f"Missing path for {domain}"

    def test_domain_values(self):
        assert DbDomain.SCAN.value == "scan"
        assert DbDomain.MANAGEMENT.value == "management"
        assert DbDomain.CHART.value == "chart"
        assert DbDomain.WAIT.value == "wait"
        assert DbDomain.IV_HISTORY.value == "iv_history"
        assert DbDomain.PIPELINE.value == "pipeline"


# ---------------------------------------------------------------------------
# Phase 1: get_domain_connection
# ---------------------------------------------------------------------------
class TestGetDomainConnection:
    def test_opens_domain_db_when_exists(self, tmp_path):
        """When domain DB exists, opens it directly."""
        domain_db = tmp_path / "chart.duckdb"
        # Create the domain DB with a test table
        con = duckdb.connect(str(domain_db))
        con.execute("CREATE TABLE test_marker (id INTEGER)")
        con.execute("INSERT INTO test_marker VALUES (42)")
        con.close()

        import core.shared.data_layer.duckdb_utils as mod
        orig_paths = dict(mod._DOMAIN_PATHS)
        try:
            mod._DOMAIN_PATHS[DbDomain.CHART] = domain_db
            result_con = get_domain_connection(DbDomain.CHART, read_only=True)
            val = result_con.execute("SELECT id FROM test_marker").fetchone()[0]
            assert val == 42
            result_con.close()
        finally:
            mod._DOMAIN_PATHS = orig_paths

    def test_fallback_to_pipeline_when_domain_missing(self, tmp_path):
        """When domain DB doesn't exist, falls back to pipeline.duckdb."""
        pipeline_db = tmp_path / "pipeline.duckdb"
        missing_chart_db = tmp_path / "chart.duckdb"  # doesn't exist

        # Create pipeline with a marker table
        con = duckdb.connect(str(pipeline_db))
        con.execute("CREATE TABLE fallback_marker (val VARCHAR)")
        con.execute("INSERT INTO fallback_marker VALUES ('from_pipeline')")
        con.close()

        with patch.dict(
            {k: v for k, v in _DOMAIN_PATHS.items()},
            {DbDomain.CHART: missing_chart_db},
        ):
            patched_paths = dict(_DOMAIN_PATHS)
            patched_paths[DbDomain.CHART] = missing_chart_db

            import core.shared.data_layer.duckdb_utils as mod
            orig_paths = mod._DOMAIN_PATHS
            orig_pipeline = mod.PIPELINE_DB_PATH
            try:
                mod._DOMAIN_PATHS = patched_paths
                mod.PIPELINE_DB_PATH = pipeline_db
                result_con = get_domain_connection(DbDomain.CHART, read_only=True)
                val = result_con.execute("SELECT val FROM fallback_marker").fetchone()[0]
                assert val == "from_pipeline"
                result_con.close()
            finally:
                mod._DOMAIN_PATHS = orig_paths
                mod.PIPELINE_DB_PATH = orig_pipeline

    def test_write_connection(self, tmp_path):
        """get_domain_write_connection opens in read-write mode."""
        domain_db = tmp_path / "write_mgmt.duckdb"

        import core.shared.data_layer.duckdb_utils as mod
        orig_paths = mod._DOMAIN_PATHS.copy()
        try:
            mod._DOMAIN_PATHS[DbDomain.MANAGEMENT] = domain_db
            con = get_domain_write_connection(DbDomain.MANAGEMENT)
            con.execute("CREATE TABLE write_test (x INTEGER)")
            con.execute("INSERT INTO write_test VALUES (1)")
            assert con.execute("SELECT x FROM write_test").fetchone()[0] == 1
            con.close()
        finally:
            mod._DOMAIN_PATHS.clear()
            mod._DOMAIN_PATHS.update(orig_paths)


# ---------------------------------------------------------------------------
# Phase 1: attach_domain (cross-DB reads)
# ---------------------------------------------------------------------------
class TestAttachDomain:
    def test_attach_reads_cross_domain(self, tmp_path):
        """ATTACH lets one domain read another's tables."""
        # Create "management" domain DB with data
        mgmt_db = tmp_path / "management.duckdb"
        con = duckdb.connect(str(mgmt_db))
        con.execute("CREATE TABLE management_recommendations (TradeID VARCHAR, Action VARCHAR)")
        con.execute("INSERT INTO management_recommendations VALUES ('T1', 'HOLD')")
        con.close()

        # Create "scan" domain DB — our primary connection
        scan_db = tmp_path / "scan.duckdb"
        scan_con = duckdb.connect(str(scan_db))
        scan_con.execute("CREATE TABLE scan_results (ticker VARCHAR)")

        import core.shared.data_layer.duckdb_utils as mod
        orig_paths = dict(mod._DOMAIN_PATHS)
        try:
            mod._DOMAIN_PATHS[DbDomain.MANAGEMENT] = mgmt_db
            alias = attach_domain(scan_con, DbDomain.MANAGEMENT)
            assert alias == "management"

            # Cross-DB read
            row = scan_con.execute(
                f"SELECT Action FROM {alias}.management_recommendations WHERE TradeID = 'T1'"
            ).fetchone()
            assert row[0] == "HOLD"
        finally:
            mod._DOMAIN_PATHS = orig_paths
            scan_con.close()

    def test_attach_idempotent(self, tmp_path):
        """Attaching the same domain twice doesn't raise."""
        mgmt_db = tmp_path / "management.duckdb"
        duckdb.connect(str(mgmt_db)).close()  # create empty file

        con = duckdb.connect(str(tmp_path / "primary.duckdb"))

        import core.shared.data_layer.duckdb_utils as mod
        orig_paths = dict(mod._DOMAIN_PATHS)
        try:
            mod._DOMAIN_PATHS[DbDomain.MANAGEMENT] = mgmt_db
            attach_domain(con, DbDomain.MANAGEMENT)
            attach_domain(con, DbDomain.MANAGEMENT)  # should not raise
        finally:
            mod._DOMAIN_PATHS = orig_paths
            con.close()

    def test_attach_fallback_when_domain_missing(self, tmp_path):
        """When domain DB doesn't exist, ATTACH falls back to pipeline.duckdb."""
        pipeline_db = tmp_path / "pipeline.duckdb"
        con_pipe = duckdb.connect(str(pipeline_db))
        con_pipe.execute("CREATE TABLE mgmt_table (id INTEGER)")
        con_pipe.execute("INSERT INTO mgmt_table VALUES (99)")
        con_pipe.close()

        primary_con = duckdb.connect(str(tmp_path / "primary.duckdb"))

        import core.shared.data_layer.duckdb_utils as mod
        orig_paths = dict(mod._DOMAIN_PATHS)
        orig_pipeline = mod.PIPELINE_DB_PATH
        try:
            mod._DOMAIN_PATHS[DbDomain.MANAGEMENT] = tmp_path / "nonexistent.duckdb"
            mod.PIPELINE_DB_PATH = pipeline_db
            alias = attach_domain(primary_con, DbDomain.MANAGEMENT)
            val = primary_con.execute(f"SELECT id FROM {alias}.mgmt_table").fetchone()[0]
            assert val == 99
        finally:
            mod._DOMAIN_PATHS = orig_paths
            mod.PIPELINE_DB_PATH = orig_pipeline
            primary_con.close()


# ---------------------------------------------------------------------------
# Migration script basics
# ---------------------------------------------------------------------------
class TestMigrationScript:
    def test_migrate_domain_copies_tables(self, tmp_path):
        """Migration script copies tables from pipeline into domain DB."""
        from scripts.admin.migrate_domain_split import migrate_domain, DOMAIN_TABLES

        # Create a mini pipeline.duckdb
        pipeline_db = tmp_path / "pipeline.duckdb"
        con = duckdb.connect(str(pipeline_db))
        con.execute("CREATE TABLE chart_state_history (ticker VARCHAR, state VARCHAR)")
        con.execute("INSERT INTO chart_state_history VALUES ('AAPL', 'UPTREND')")
        con.execute("INSERT INTO chart_state_history VALUES ('MSFT', 'RANGE')")
        con.close()

        chart_db = tmp_path / "chart.duckdb"

        import scripts.admin.migrate_domain_split as mig
        import core.shared.data_contracts.config as cfg
        orig_pipeline = cfg.PIPELINE_DB_PATH
        orig_chart = cfg.CHART_DB_PATH
        try:
            # Patch paths for test
            mig.PIPELINE_DB_PATH = pipeline_db
            mig.DOMAIN_TABLES["chart"]["path"] = chart_db
            results = migrate_domain("chart", dry_run=False)
            assert results.get("chart_state_history", 0) == 2
        finally:
            cfg.PIPELINE_DB_PATH = orig_pipeline
            cfg.CHART_DB_PATH = orig_chart

    def test_dry_run_no_file_created(self, tmp_path):
        """Dry run reports counts but creates no domain DB file."""
        from scripts.admin.migrate_domain_split import migrate_domain

        pipeline_db = tmp_path / "pipeline.duckdb"
        con = duckdb.connect(str(pipeline_db))
        con.execute("CREATE TABLE wait_list (id INTEGER)")
        con.execute("INSERT INTO wait_list VALUES (1)")
        con.close()

        wait_db = tmp_path / "wait.duckdb"

        import scripts.admin.migrate_domain_split as mig
        orig_pipeline = mig.PIPELINE_DB_PATH
        try:
            mig.PIPELINE_DB_PATH = pipeline_db
            mig.DOMAIN_TABLES["wait"]["path"] = wait_db
            results = migrate_domain("wait", dry_run=True)
            assert results.get("wait_list", 0) == 1
            assert not wait_db.exists(), "Dry run should not create domain DB"
        finally:
            mig.PIPELINE_DB_PATH = orig_pipeline
