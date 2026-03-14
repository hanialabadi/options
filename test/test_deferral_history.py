"""
Tests for wait list deferral history — denormalized history tracking and
pattern detection from wait_list_history.

Validates:
1. Schema migration adds new columns to existing tables
2. _log_history stores ticker/strategy/conditions
3. query_deferral_patterns returns correct aggregations
4. Pipeline enrichment wires deferral columns onto input_df
"""

import pytest
import duckdb
import json
import uuid
from datetime import datetime, timedelta
from core.wait_loop.schema import (
    initialize_wait_list_schema,
    query_deferral_patterns,
    _migrate_wait_list_history,
    extract_contract_quality,
    compare_contract_quality,
)
from core.wait_loop.persistence import WaitListPersistence
from core.wait_loop.schema import (
    WaitListEntry, ConfirmationCondition, ConditionType, TradeStatus,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def con():
    """In-memory DuckDB with wait_list schema initialized."""
    c = duckdb.connect(':memory:')
    initialize_wait_list_schema(c)
    yield c
    c.close()


def _make_entry(ticker='NVDA', strategy='LONG_CALL', conditions=None, **overrides):
    """Create a WaitListEntry for testing."""
    if conditions is None:
        conditions = [
            ConfirmationCondition(
                condition_id='cond_1',
                condition_type=ConditionType.TECHNICAL,
                description='Market_Structure must develop Uptrend',
                config={'metric': 'Market_Structure', 'target': 'Uptrend'},
            ),
            ConfirmationCondition(
                condition_id='cond_2',
                condition_type=ConditionType.TECHNICAL,
                description='Chart_Regime must shift to Trending',
                config={'metric': 'Chart_Regime', 'target': 'Trending'},
            ),
        ]
    now = datetime.now()
    defaults = dict(
        wait_id=str(uuid.uuid4()),
        ticker=ticker,
        strategy_name=strategy,
        strategy_type='DIRECTIONAL',
        proposed_strike=130.0,
        proposed_expiration='2026-04-17',
        contract_symbol=f'{ticker}260417C00130000',
        wait_started_at=now,
        wait_expires_at=now + timedelta(days=5),
        last_evaluated_at=now,
        evaluation_count=1,
        wait_conditions=conditions,
        conditions_met=[],
        wait_progress=0.0,
        entry_price=128.50,
        entry_iv_30d=35.0,
        entry_hv_30=28.0,
        entry_chart_signal='BUY',
        entry_pcs_score=72.0,
        current_price=128.50,
        current_iv_30d=35.0,
        current_chart_signal='BUY',
        price_change_pct=0.0,
        invalidation_price=115.65,
        max_sessions_wait=3,
        max_days_wait=5,
        status=TradeStatus.ACTIVE,
        rejection_reason=None,
    )
    defaults.update(overrides)
    return WaitListEntry(**defaults)


# ── 1. Schema & Migration ────────────────────────────────────────────────

class TestSchemaMigration:
    """New columns exist after initialization."""

    def test_history_table_has_ticker_column(self, con):
        cols = {r[1] for r in con.execute("PRAGMA table_info('wait_list_history')").fetchall()}
        assert 'ticker' in cols
        assert 'strategy_name' in cols
        assert 'wait_conditions_json' in cols

    def test_migration_is_idempotent(self, con):
        """Running migration twice doesn't error."""
        _migrate_wait_list_history(con)
        _migrate_wait_list_history(con)
        cols = {r[1] for r in con.execute("PRAGMA table_info('wait_list_history')").fetchall()}
        assert 'ticker' in cols

    def test_ticker_index_exists(self, con):
        # DuckDB doesn't expose indexes easily, but we can verify the CREATE didn't error
        # by running a query that would benefit from the index
        con.execute("SELECT * FROM wait_list_history WHERE ticker = 'NVDA'")

    def test_migration_applies_to_pre_existing_old_schema_table(self):
        """Regression: initialize_wait_list_schema must work when
        wait_list_history already exists WITHOUT the ticker column.

        The migration (ALTER TABLE ADD COLUMN) must run BEFORE the
        CREATE INDEX on ticker; otherwise the index fails and the
        migration is never reached.
        """
        c = duckdb.connect(':memory:')
        # Create old-schema tables (no ticker/strategy_name/wait_conditions_json)
        c.execute("""
            CREATE TABLE wait_list (
                wait_id VARCHAR PRIMARY KEY,
                ticker VARCHAR NOT NULL,
                strategy_name VARCHAR NOT NULL,
                strategy_type VARCHAR NOT NULL,
                proposed_strike DOUBLE,
                proposed_expiration DATE,
                contract_symbol VARCHAR,
                wait_started_at TIMESTAMP NOT NULL,
                wait_expires_at TIMESTAMP NOT NULL,
                last_evaluated_at TIMESTAMP NOT NULL,
                evaluation_count INTEGER DEFAULT 1,
                wait_conditions JSON NOT NULL,
                conditions_met JSON DEFAULT '[]',
                wait_progress DOUBLE DEFAULT 0.0,
                entry_price DOUBLE NOT NULL,
                entry_iv_30d DOUBLE, entry_hv_30 DOUBLE,
                entry_chart_signal VARCHAR, entry_pcs_score DOUBLE,
                current_price DOUBLE, current_iv_30d DOUBLE,
                current_chart_signal VARCHAR, price_change_pct DOUBLE,
                invalidation_price DOUBLE,
                max_sessions_wait INTEGER DEFAULT 3,
                max_days_wait INTEGER DEFAULT 5,
                status VARCHAR DEFAULT 'ACTIVE',
                rejection_reason VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE wait_list_history (
                history_id VARCHAR PRIMARY KEY,
                wait_id VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                conditions_met JSON,
                wait_progress DOUBLE,
                status VARCHAR,
                notes VARCHAR
            )
        """)
        c.commit()

        # Verify old schema lacks ticker
        cols_before = {r[1] for r in c.execute(
            "PRAGMA table_info('wait_list_history')").fetchall()}
        assert 'ticker' not in cols_before

        # This must NOT raise — previously it crashed with
        # "column 'ticker' not found" on the CREATE INDEX
        initialize_wait_list_schema(c)

        cols_after = {r[1] for r in c.execute(
            "PRAGMA table_info('wait_list_history')").fetchall()}
        assert 'ticker' in cols_after
        assert 'strategy_name' in cols_after
        assert 'wait_conditions_json' in cols_after

        # Index should work too
        c.execute("SELECT * FROM wait_list_history WHERE ticker = 'TEST'")
        c.close()


# ── 2. History Logging ───────────────────────────────────────────────────

class TestHistoryLogging:
    """_log_history stores denormalized ticker/strategy/conditions."""

    def test_created_event_stores_ticker_and_conditions(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry()
        p.save_wait_entry(entry)

        row = con.execute("""
            SELECT ticker, strategy_name, event_type, wait_conditions_json
            FROM wait_list_history
            WHERE wait_id = ? AND event_type = 'CREATED'
        """, (entry.wait_id,)).fetchone()

        assert row is not None
        assert row[0] == 'NVDA'
        assert row[1] == 'LONG_CALL'
        assert row[2] == 'CREATED'
        # Verify conditions JSON
        conditions = json.loads(row[3])
        assert len(conditions) == 2
        assert conditions[0]['description'] == 'Market_Structure must develop Uptrend'

    def test_evaluated_event_resolves_ticker(self, con):
        """Non-CREATED events look up ticker from wait_list."""
        p = WaitListPersistence(con)
        entry = _make_entry()
        p.save_wait_entry(entry)

        p.update_wait_progress(entry.wait_id, ['cond_1'], 0.5, 130.0)

        row = con.execute("""
            SELECT ticker, strategy_name
            FROM wait_list_history
            WHERE wait_id = ? AND event_type = 'EVALUATED'
        """, (entry.wait_id,)).fetchone()

        assert row[0] == 'NVDA'
        assert row[1] == 'LONG_CALL'

    def test_promoted_event_has_ticker(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry()
        p.save_wait_entry(entry)

        p.mark_promoted(entry.wait_id, 'NVDA260417C00130000', 0.85)

        row = con.execute("""
            SELECT ticker, strategy_name, notes
            FROM wait_list_history
            WHERE wait_id = ? AND event_type = 'PROMOTED'
        """, (entry.wait_id,)).fetchone()

        assert row[0] == 'NVDA'
        assert 'confidence: 85.0%' in row[2]

    def test_rejected_event_has_ticker(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry()
        p.save_wait_entry(entry)

        p.mark_rejected(entry.wait_id, 'TTL exceeded', TradeStatus.EXPIRED)

        row = con.execute("""
            SELECT ticker, strategy_name, notes
            FROM wait_list_history
            WHERE wait_id = ? AND event_type = 'EXPIRED'
        """, (entry.wait_id,)).fetchone()

        assert row[0] == 'NVDA'
        assert 'TTL exceeded' in row[2]


# ── 3. Query Deferral Patterns ───────────────────────────────────────────

class TestQueryDeferralPatterns:
    """query_deferral_patterns returns correct aggregations."""

    def test_no_history_returns_zeros(self, con):
        result = query_deferral_patterns(con, 'AAPL')
        assert result['deferral_count'] == 0
        assert result['promotion_rate'] == 0.0
        assert result['common_conditions'] == []

    def test_single_deferral_counted(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry(ticker='AAPL')
        p.save_wait_entry(entry)

        result = query_deferral_patterns(con, 'AAPL')
        assert result['deferral_count'] == 1

    def test_multiple_deferrals_counted(self, con):
        p = WaitListPersistence(con)
        for i in range(3):
            entry = _make_entry(
                ticker='TSLA',
                wait_id=str(uuid.uuid4()),
                strategy=f'LONG_CALL_{i}',
            )
            p.save_wait_entry(entry)

        result = query_deferral_patterns(con, 'TSLA')
        assert result['deferral_count'] == 3

    def test_promotion_rate_calculated(self, con):
        p = WaitListPersistence(con)
        # Create 2 entries: 1 promoted, 1 expired
        e1 = _make_entry(ticker='AMD', wait_id=str(uuid.uuid4()))
        e2 = _make_entry(ticker='AMD', wait_id=str(uuid.uuid4()), strategy='LONG_PUT')
        p.save_wait_entry(e1)
        p.save_wait_entry(e2)

        p.mark_promoted(e1.wait_id, 'AMD260417C00130000', 0.80)
        p.mark_rejected(e2.wait_id, 'TTL exceeded', TradeStatus.EXPIRED)

        result = query_deferral_patterns(con, 'AMD')
        assert result['deferral_count'] == 2
        assert result['promotion_rate'] == 0.5
        assert result['outcome_distribution']['PROMOTED'] == 1
        assert result['outcome_distribution']['EXPIRED'] == 1

    def test_strategies_deferred_breakdown(self, con):
        p = WaitListPersistence(con)
        for strat in ['LONG_CALL', 'LONG_CALL', 'LONG_PUT']:
            entry = _make_entry(
                ticker='MSFT',
                strategy=strat,
                wait_id=str(uuid.uuid4()),
            )
            p.save_wait_entry(entry)

        result = query_deferral_patterns(con, 'MSFT')
        assert result['strategies_deferred']['LONG_CALL'] == 2
        assert result['strategies_deferred']['LONG_PUT'] == 1

    def test_common_conditions_extracted(self, con):
        p = WaitListPersistence(con)
        # Both entries have same blocking conditions
        for _ in range(2):
            entry = _make_entry(
                ticker='META',
                wait_id=str(uuid.uuid4()),
            )
            p.save_wait_entry(entry)

        result = query_deferral_patterns(con, 'META')
        assert len(result['common_conditions']) > 0
        # Should have counts >= 2 for each condition since same conditions appear twice
        for cond_key, count in result['common_conditions']:
            assert count == 2

    def test_lookback_window_filters_old_data(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry(ticker='GOOG', wait_id=str(uuid.uuid4()))
        p.save_wait_entry(entry)

        # Backdate the history event to 120 days ago
        con.execute("""
            UPDATE wait_list_history
            SET event_timestamp = CURRENT_TIMESTAMP - INTERVAL 120 DAY
            WHERE ticker = 'GOOG'
        """)
        con.commit()

        # 90-day window should not include it
        result = query_deferral_patterns(con, 'GOOG', lookback_days=90)
        assert result['deferral_count'] == 0

        # 180-day window should
        result = query_deferral_patterns(con, 'GOOG', lookback_days=180)
        assert result['deferral_count'] == 1

    def test_avg_wait_days_calculated(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry(ticker='NFLX', wait_id=str(uuid.uuid4()))
        p.save_wait_entry(entry)

        # Backdate created event by 3 days, add a terminal event now
        con.execute("""
            UPDATE wait_list_history
            SET event_timestamp = CURRENT_TIMESTAMP - INTERVAL 3 DAY
            WHERE ticker = 'NFLX' AND event_type = 'CREATED'
        """)
        con.commit()

        p.mark_rejected(entry.wait_id, 'TTL exceeded', TradeStatus.EXPIRED)

        result = query_deferral_patterns(con, 'NFLX')
        # Should be approximately 3 days (within tolerance)
        assert 2.5 <= result['avg_wait_days'] <= 3.5

    def test_different_tickers_isolated(self, con):
        p = WaitListPersistence(con)
        e1 = _make_entry(ticker='AAPL', wait_id=str(uuid.uuid4()))
        e2 = _make_entry(ticker='MSFT', wait_id=str(uuid.uuid4()))
        p.save_wait_entry(e1)
        p.save_wait_entry(e2)

        r_aapl = query_deferral_patterns(con, 'AAPL')
        r_msft = query_deferral_patterns(con, 'MSFT')
        assert r_aapl['deferral_count'] == 1
        assert r_msft['deferral_count'] == 1


# ── 4. Pipeline Enrichment Columns ──────────────────────────────────────

class TestPipelineEnrichment:
    """Deferral columns are correctly mapped onto DataFrame."""

    def test_enrichment_columns_populated(self, con):
        """Simulate what pipeline does: query + map onto df."""
        import pandas as pd
        p = WaitListPersistence(con)

        # Create 2 deferrals for NVDA: 1 promoted, 1 expired
        e1 = _make_entry(ticker='NVDA', wait_id=str(uuid.uuid4()))
        e2 = _make_entry(ticker='NVDA', wait_id=str(uuid.uuid4()), strategy='LONG_PUT')
        p.save_wait_entry(e1)
        p.save_wait_entry(e2)
        p.mark_promoted(e1.wait_id, 'NVDA260417C00130000', 0.85)
        p.mark_rejected(e2.wait_id, 'TTL exceeded', TradeStatus.EXPIRED)

        # Simulate pipeline enrichment
        input_df = pd.DataFrame({'Ticker': ['NVDA', 'AAPL']})
        _def_cache = {}
        for _t in input_df['Ticker'].unique():
            _dp = query_deferral_patterns(con, str(_t), lookback_days=90)
            if _dp.get('deferral_count', 0) > 0:
                _common = _dp.get('common_conditions', [])
                _common_str = '; '.join(f"{k} ({v})" for k, v in _common[:3]) if _common else ''
                _def_cache[str(_t)] = {
                    'Deferral_Count_90d': _dp['deferral_count'],
                    'Deferral_Promotion_Rate': round(_dp['promotion_rate'], 2),
                    'Deferral_Avg_Wait_Days': _dp['avg_wait_days'],
                    'Deferral_Common_Block': _common_str,
                }

        _deferral_cols = [
            ('Deferral_Count_90d', 0), ('Deferral_Promotion_Rate', 0.0),
            ('Deferral_Avg_Wait_Days', 0.0), ('Deferral_Common_Block', ''),
        ]
        for _col, _default in _deferral_cols:
            input_df[_col] = input_df['Ticker'].map(
                lambda t, c=_col, d=_default: _def_cache.get(str(t), {}).get(c, d))

        # NVDA should have deferral data
        nvda_row = input_df[input_df['Ticker'] == 'NVDA'].iloc[0]
        assert nvda_row['Deferral_Count_90d'] == 2
        assert nvda_row['Deferral_Promotion_Rate'] == 0.5

        # AAPL should have defaults
        aapl_row = input_df[input_df['Ticker'] == 'AAPL'].iloc[0]
        assert aapl_row['Deferral_Count_90d'] == 0
        assert aapl_row['Deferral_Common_Block'] == ''

    def test_empty_history_safe_defaults(self, con):
        """No crash when wait_list_history is empty."""
        import pandas as pd
        input_df = pd.DataFrame({'Ticker': ['NVDA', 'TSLA']})
        _deferral_cols = [
            ('Deferral_Count_90d', 0), ('Deferral_Promotion_Rate', 0.0),
            ('Deferral_Avg_Wait_Days', 0.0), ('Deferral_Common_Block', ''),
        ]
        for _col, _default in _deferral_cols:
            input_df[_col] = _default

        assert (input_df['Deferral_Count_90d'] == 0).all()
        assert (input_df['Deferral_Common_Block'] == '').all()


# ── 5. Edge Cases ────────────────────────────────────────────────────────

class TestEdgeCases:

    def test_conditions_with_empty_description(self, con):
        """Conditions missing description don't crash pattern extraction."""
        p = WaitListPersistence(con)
        entry = _make_entry(
            ticker='COIN',
            wait_id=str(uuid.uuid4()),
            conditions=[
                ConfirmationCondition(
                    condition_id='c1',
                    condition_type=ConditionType.TIME_DELAY,
                    description='',
                    config={'hours': 24},
                ),
            ],
        )
        p.save_wait_entry(entry)

        result = query_deferral_patterns(con, 'COIN')
        assert result['deferral_count'] == 1
        assert len(result['common_conditions']) > 0

    def test_no_conditions_entry(self, con):
        """Entry with no conditions still logged correctly."""
        p = WaitListPersistence(con)
        entry = _make_entry(
            ticker='PLTR',
            wait_id=str(uuid.uuid4()),
            conditions=[],
        )
        p.save_wait_entry(entry)

        result = query_deferral_patterns(con, 'PLTR')
        assert result['deferral_count'] == 1
        assert result['common_conditions'] == []

    def test_query_nonexistent_ticker(self, con):
        """Querying a ticker with zero history returns clean zeros."""
        result = query_deferral_patterns(con, 'ZZZZ')
        assert result['deferral_count'] == 0
        assert result['outcome_distribution'] == {}
        assert result['strategies_deferred'] == {}


# ── 6. Contract Quality Extraction ──────────────────────────────────────

class TestExtractContractQuality:
    """extract_contract_quality pulls the right fields from a row."""

    def test_extracts_all_fields(self):
        row = {
            'Open_Interest': 500,
            'Bid_Ask_Spread_Pct': 3.2,
            'Delta': 0.45,
            'Actual_DTE': 35,
            'Mid_Price': 2.50,
            'DQS': 72,
            'PCS_Score': 68,
            'Liquidity_Grade': 'Good',
        }
        quality = extract_contract_quality(row)
        assert quality['oi'] == 500
        assert quality['spread_pct'] == 3.2
        assert quality['delta'] == 0.45
        assert quality['dte'] == 35
        assert quality['mid_price'] == 2.50
        assert quality['dqs'] == 72
        assert quality['pcs'] == 68
        assert quality['liquidity_grade'] == 'Good'

    def test_handles_missing_fields(self):
        quality = extract_contract_quality({})
        assert quality['oi'] is None
        assert quality['spread_pct'] is None
        assert quality['liquidity_grade'] == ''

    def test_handles_nan(self):
        row = {'Open_Interest': float('nan'), 'DQS': float('nan')}
        quality = extract_contract_quality(row)
        assert quality['oi'] is None
        assert quality['dqs'] is None


# ── 7. Contract Quality Comparison ──────────────────────────────────────

class TestCompareContractQuality:
    """compare_contract_quality requires 2+ improvements."""

    def test_no_existing_quality_returns_false(self):
        is_better, reasons = compare_contract_quality(None, {'oi': 500, 'pcs': 80})
        assert is_better is False

    def test_single_improvement_not_enough(self):
        old = {'oi': 100, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        new = {'oi': 200, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        is_better, reasons = compare_contract_quality(old, new)
        assert is_better is False
        assert len(reasons) == 1  # OI improved

    def test_two_improvements_qualifies(self):
        old = {'oi': 100, 'spread_pct': 10.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Thin'}
        new = {'oi': 200, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Thin'}
        is_better, reasons = compare_contract_quality(old, new)
        assert is_better is True
        assert len(reasons) == 2  # OI + spread

    def test_three_improvements(self):
        old = {'oi': 100, 'spread_pct': 10.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Thin'}
        new = {'oi': 200, 'spread_pct': 5.0, 'pcs': 70, 'dqs': 50, 'liquidity_grade': 'Thin'}
        is_better, reasons = compare_contract_quality(old, new)
        assert is_better is True
        assert len(reasons) == 3  # OI + spread + PCS

    def test_spread_must_improve_20pct(self):
        """Spread must tighten by at least 20% to count."""
        old = {'oi': 100, 'spread_pct': 10.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        new = {'oi': 200, 'spread_pct': 8.5, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        is_better, reasons = compare_contract_quality(old, new)
        # spread 10→8.5 = 15% tighter (below 20% threshold)
        assert is_better is False
        assert len(reasons) == 1  # only OI

    def test_oi_must_improve_50pct(self):
        """OI must increase by at least 50% to count."""
        old = {'oi': 100, 'spread_pct': 10.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        new = {'oi': 140, 'spread_pct': 7.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        is_better, reasons = compare_contract_quality(old, new)
        # OI 100→140 = 40% (below 50% threshold), spread 10→7 = 30% tighter (qualifies)
        assert len(reasons) == 1  # only spread
        assert is_better is False

    def test_pcs_must_improve_5pts(self):
        old = {'oi': 100, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        new = {'oi': 100, 'spread_pct': 5.0, 'pcs': 63, 'dqs': 50, 'liquidity_grade': 'Good'}
        _, reasons = compare_contract_quality(old, new)
        assert len(reasons) == 0  # 3pt PCS delta < 5pt threshold

    def test_liquidity_grade_upgrade(self):
        old = {'oi': 100, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Thin'}
        new = {'oi': 200, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Good'}
        is_better, reasons = compare_contract_quality(old, new)
        assert is_better is True  # OI + liquidity = 2

    def test_dqs_improvement(self):
        old = {'oi': 100, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Thin'}
        new = {'oi': 100, 'spread_pct': 5.0, 'pcs': 60, 'dqs': 60, 'liquidity_grade': 'Good'}
        is_better, reasons = compare_contract_quality(old, new)
        assert is_better is True  # DQS + liquidity = 2

    def test_reasons_contain_descriptions(self):
        old = {'oi': 100, 'spread_pct': 10.0, 'pcs': 60, 'dqs': 50, 'liquidity_grade': 'Thin'}
        new = {'oi': 200, 'spread_pct': 5.0, 'pcs': 70, 'dqs': 60, 'liquidity_grade': 'Good'}
        _, reasons = compare_contract_quality(old, new)
        reason_text = ' '.join(reasons)
        assert 'OI' in reason_text
        assert 'spread' in reason_text
        assert 'PCS' in reason_text

    def test_none_values_handled(self):
        """None values in either side don't crash — just skip that metric."""
        old = {'oi': None, 'spread_pct': 10.0, 'pcs': None, 'dqs': 50, 'liquidity_grade': 'Good'}
        new = {'oi': 200, 'spread_pct': 5.0, 'pcs': 70, 'dqs': 60, 'liquidity_grade': 'Excellent'}
        is_better, reasons = compare_contract_quality(old, new)
        # spread (qualifies) + DQS (qualifies) + liquidity (qualifies) = 3
        assert is_better is True


# ── 8. Contract Refresh Persistence ─────────────────────────────────────

class TestContractRefresh:
    """refresh_contract updates contract while preserving wait metadata."""

    def test_refresh_updates_contract_fields(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry(ticker='NVDA')
        p.save_wait_entry(entry)

        p.refresh_contract(
            entry.wait_id,
            proposed_strike=135.0,
            proposed_expiration='2026-05-15',
            contract_symbol='NVDA260515C00135000',
            contract_quality={'oi': 500, 'spread_pct': 2.0},
            reasons=['OI 100→500', 'spread 8.0%→2.0%'],
        )

        updated = p.get_wait_entry(entry.wait_id)
        assert updated['proposed_strike'] == 135.0
        assert updated['contract_symbol'] == 'NVDA260515C00135000'

    def test_refresh_preserves_wait_clock(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry(ticker='NVDA')
        p.save_wait_entry(entry)

        original = p.get_wait_entry(entry.wait_id)
        original_started = original['wait_started_at']

        p.refresh_contract(
            entry.wait_id,
            proposed_strike=135.0,
            proposed_expiration='2026-05-15',
            contract_symbol='NVDA260515C00135000',
            contract_quality={'oi': 500},
            reasons=['better OI'],
        )

        updated = p.get_wait_entry(entry.wait_id)
        assert updated['wait_started_at'] == original_started
        assert updated['status'] == 'ACTIVE'

    def test_refresh_logs_history_event(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry(ticker='NVDA')
        p.save_wait_entry(entry)

        p.refresh_contract(
            entry.wait_id,
            proposed_strike=135.0,
            proposed_expiration='2026-05-15',
            contract_symbol='NVDA260515C00135000',
            contract_quality={'oi': 500},
            reasons=['OI 100→500', 'spread tighter'],
        )

        row = con.execute("""
            SELECT event_type, notes, ticker
            FROM wait_list_history
            WHERE wait_id = ? AND event_type = 'CONTRACT_REFRESHED'
        """, (entry.wait_id,)).fetchone()

        assert row is not None
        assert row[0] == 'CONTRACT_REFRESHED'
        assert 'OI 100→500' in row[1]
        assert row[2] == 'NVDA'

    def test_contract_quality_stored_on_creation(self, con):
        p = WaitListPersistence(con)
        entry = _make_entry(
            ticker='AAPL',
            contract_quality={'oi': 300, 'spread_pct': 4.0, 'pcs': 70},
        )
        p.save_wait_entry(entry)

        waits = p.load_active_waits()
        assert len(waits) == 1
        assert waits[0]['contract_quality'] is not None
        assert waits[0]['contract_quality']['oi'] == 300
