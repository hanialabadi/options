"""
Buy-Write Premium Ledger
========================
Tracks cumulative premiums collected across all call roll cycles for a BUY_WRITE
(or COVERED_CALL) position. Each time a call expires worthless or is rolled, the
credit received reduces the effective stock cost basis.

Design:
  - One DuckDB table: `premium_ledger`
  - One row per call leg per TradeID (indexed by TradeID + LegID)
  - Cumulative sum computed at runtime: Net_Cost_Basis = Stock_Basis - Sum(credits)
  - Called in run_all.py AFTER anchor merge, BEFORE compute_drift_metrics

McMillan Ch.3: The entire point of a buy-write is progressive cost basis reduction.
  "Each successive call written further reduces the effective cost of the shares."

Passarelli Ch.6: Net cost basis determines the true breakeven — not the stock purchase price.
  Rolling at 50% profit is sensible only when the cumulative yield is tracked.

Table schema:
    trade_id        VARCHAR         — TradeID of the BUY_WRITE position (matches entry_anchors)
    leg_id          VARCHAR         — LegID of the OPTION leg that generated the credit
    cycle_number    INTEGER         — 1 for first call written, 2 for first roll, etc.
    credit_received DOUBLE          — per-share credit received (positive = income)
    contracts       INTEGER         — number of contracts (multiplier = 100 shares)
    strike          DOUBLE          — call strike
    expiry          VARCHAR         — expiration date (YYYY-MM-DD)
    opened_at       TIMESTAMP       — when this call leg was frozen (entry anchor TS)
    closed_at       TIMESTAMP       — when it was closed/rolled (NULL if still open)
    status          VARCHAR         — OPEN | EXPIRED | ROLLED | ASSIGNED
    notes           VARCHAR         — optional context
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, date
from typing import Optional

import duckdb
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

_PIPELINE_DB = "data/pipeline.duckdb"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS premium_ledger (
    trade_id        VARCHAR NOT NULL,
    leg_id          VARCHAR PRIMARY KEY,
    cycle_number    INTEGER DEFAULT 1,
    credit_received DOUBLE  NOT NULL,
    close_cost      DOUBLE  DEFAULT 0.0,
    contracts       INTEGER NOT NULL,
    strike          DOUBLE,
    expiry          VARCHAR,
    opened_at       TIMESTAMP,
    closed_at       TIMESTAMP,
    status          VARCHAR DEFAULT 'OPEN',
    notes           VARCHAR,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Migration: add close_cost column to existing tables that pre-date this schema.
_MIGRATE_CLOSE_COST_SQL = """
ALTER TABLE premium_ledger ADD COLUMN IF NOT EXISTS close_cost DOUBLE DEFAULT 0.0
"""


class BuyWriteLedger:
    """
    Manages the premium_ledger table and computes running net cost basis
    for BUY_WRITE / COVERED_CALL positions.

    Usage in run_all.py (after anchor merge, before compute_drift_metrics):
        ledger = BuyWriteLedger()
        df_enriched = ledger.enrich(df_enriched, con)
    """

    def __init__(self, db_path: str = _PIPELINE_DB) -> None:
        self._db_path = db_path

    # ── Public API ────────────────────────────────────────────────────────────

    def enrich(self, df: pd.DataFrame, con=None) -> pd.DataFrame:
        """
        For each BUY_WRITE / COVERED_CALL position:
          1. Ensure open call legs are recorded in premium_ledger
          2. Compute cumulative credits per TradeID
          3. Inject Net_Cost_Basis_Per_Share, Breakeven_Price, Cumulative_Premium_Collected

        con: optional open DuckDB connection; if None, opens its own.
        """
        if df.empty:
            return df

        bw_mask = df['Strategy'].str.upper().isin(['BUY_WRITE', 'COVERED_CALL']) \
            if 'Strategy' in df.columns else pd.Series(False, index=df.index)

        if not bw_mask.any():
            return df

        df = df.copy()
        _ensure_columns(df)

        try:
            if con is not None:
                self._init_table(con)
                self._upsert_open_legs(df[bw_mask], con)
                cumulative   = self._load_cumulative(df[bw_mask], con)
                cycle_counts = self._load_cycle_counts(df[bw_mask], con)
                roll_context = self._load_roll_context(df[bw_mask], con)
            else:
                with duckdb.connect(self._db_path) as _con:
                    self._init_table(_con)
                    self._upsert_open_legs(df[bw_mask], _con)
                    cumulative   = self._load_cumulative(df[bw_mask], _con)
                    cycle_counts = self._load_cycle_counts(df[bw_mask], _con)
                    roll_context = self._load_roll_context(df[bw_mask], _con)

            # Apply cumulative credits to stock legs only
            # The stock leg carries the cost basis; option legs are just the call.
            stock_mask = bw_mask & (df.get('AssetType', pd.Series('', index=df.index)) == 'STOCK')

            if '_cycle_count' not in df.columns:
                df['_cycle_count'] = 0
            if 'Roll_Net_Credit' not in df.columns:
                df['Roll_Net_Credit'] = float('nan')
            if 'Roll_Prior_Credit' not in df.columns:
                df['Roll_Prior_Credit'] = float('nan')

            for idx in df[stock_mask].index:
                trade_id = df.at[idx, 'TradeID']
                cum_info = cumulative.get(trade_id, {})
                total_credit = cum_info.get('net', 0.0)
                gross_credit = cum_info.get('gross', 0.0)
                total_close  = cum_info.get('close_cost', 0.0)
                shares = abs(float(df.at[idx, 'Quantity'] or 0))
                stock_basis_total = abs(float(df.at[idx, 'Basis'] or 0))

                if shares > 0 and stock_basis_total > 0:
                    stock_cost_per_share = stock_basis_total / shares
                    net_cost_per_share   = stock_cost_per_share - total_credit
                    df.at[idx, 'Net_Cost_Basis_Per_Share']     = round(net_cost_per_share, 4)
                    df.at[idx, 'Breakeven_Price']              = round(net_cost_per_share, 4)
                    df.at[idx, 'Cumulative_Premium_Collected'] = round(total_credit, 4)
                    df.at[idx, 'Gross_Premium_Collected']      = round(gross_credit, 4)
                    df.at[idx, 'Total_Close_Cost']             = round(total_close, 4)
                    df.at[idx, 'Has_Debit_Rolls']              = total_close > 0.01
                    df.at[idx, '_cycle_count']                 = cycle_counts.get(trade_id, 1)

                # Roll context (debit/credit of most recent roll)
                rc = roll_context.get(trade_id)
                if rc:
                    df.at[idx, 'Roll_Net_Credit']   = round(rc['net_credit'], 4)
                    df.at[idx, 'Roll_Prior_Credit']  = round(rc['prior_credit'], 4)

            # Also propagate to the option leg of the same TradeID for doctrine access
            opt_mask = bw_mask & (df.get('AssetType', pd.Series('', index=df.index)) == 'OPTION')
            for idx in df[opt_mask].index:
                trade_id = df.at[idx, 'TradeID']
                ticker   = str(df.at[idx, 'Underlying_Ticker'] if 'Underlying_Ticker' in df.columns else '')

                # Pass 1: same-TradeID stock leg (normal BUY_WRITE pairing)
                sibling = df[stock_mask & (df['TradeID'] == trade_id)]

                # Pass 2: any STOCK leg for same ticker (handles COVERED_CALL option-only
                # positions where the stock leg is tracked under a different TradeID)
                if sibling.empty and ticker and 'Underlying_Ticker' in df.columns:
                    ticker_stock = df[
                        (df['Underlying_Ticker'] == ticker) &
                        (df.get('AssetType', pd.Series('', index=df.index)) == 'STOCK') &
                        df['Net_Cost_Basis_Per_Share'].notna()
                    ]
                    if not ticker_stock.empty:
                        sibling = ticker_stock

                if not sibling.empty:
                    sib = sibling.iloc[0]
                    df.at[idx, 'Net_Cost_Basis_Per_Share']     = sib['Net_Cost_Basis_Per_Share']
                    df.at[idx, 'Breakeven_Price']              = sib['Breakeven_Price']
                    df.at[idx, 'Cumulative_Premium_Collected'] = sib['Cumulative_Premium_Collected']
                    df.at[idx, 'Gross_Premium_Collected']      = sib.get('Gross_Premium_Collected', float('nan'))
                    df.at[idx, 'Total_Close_Cost']             = sib.get('Total_Close_Cost', float('nan'))
                    df.at[idx, 'Has_Debit_Rolls']              = bool(sib.get('Has_Debit_Rolls', False))
                    df.at[idx, '_cycle_count']                 = sib['_cycle_count']
                    df.at[idx, 'Roll_Net_Credit']              = sib.get('Roll_Net_Credit', float('nan'))
                    df.at[idx, 'Roll_Prior_Credit']            = sib.get('Roll_Prior_Credit', float('nan'))

        except Exception as e:
            logger.warning(f"[BuyWriteLedger] Enrich failed (non-fatal): {e}")

        return df

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_table(self, con) -> None:
        con.execute(_CREATE_TABLE_SQL)
        # Migrate: add close_cost to tables created before this column existed
        try:
            con.execute(_MIGRATE_CLOSE_COST_SQL)
        except Exception:
            pass  # column already exists or DB is read-only

    def _upsert_open_legs(self, bw_df: pd.DataFrame, con) -> None:
        """
        Two operations per run:

        1. AUTO-EXPIRY: Any leg_id currently OPEN in the ledger that is NOT present in
           the current CSV → it expired or was rolled since last run. Freeze it as EXPIRED
           and preserve its credit permanently. This is how historical cycles accumulate
           without manual intervention after the first backfill.

        2. UPSERT current open legs: insert new call legs or update existing ones.

        McMillan Ch.3: every expired cycle's premium permanently reduces cost basis.
        """
        opt_mask   = (bw_df.get('AssetType', pd.Series('', index=bw_df.index)) == 'OPTION')
        call_mask  = (bw_df.get('Call/Put',  pd.Series('', index=bw_df.index))
                      .str.upper().str.startswith('C'))
        short_mask = (bw_df.get('Quantity',  pd.Series(0, index=bw_df.index)) < 0)

        open_calls = bw_df[opt_mask & call_mask & short_mask]
        now_ts = datetime.utcnow()

        # ── Step 1: Auto-expiry detection ─────────────────────────────────────
        # Get the set of LegIDs currently in the snapshot
        current_leg_ids = set(open_calls['LegID'].dropna().astype(str).tolist()) if not open_calls.empty else set()

        # Get trade_ids in scope for this run, plus all same-ticker relatives in the ledger.
        # This handles the case where a roll creates a new trade_id (e.g. DKNG260306_24p5_CC_5376)
        # but prior cycles are stored under old trade_ids (e.g. DKNG260227_23p0_CC_5376).
        bw_trade_ids = bw_df['TradeID'].dropna().unique().tolist()

        # Expand to include all same-ticker_acct relatives already in the ledger
        ticker_acct_keys = {_ticker_acct_key(tid) for tid in bw_trade_ids if _ticker_acct_key(tid)}
        all_related_trade_ids: list[str] = list(bw_trade_ids)
        if ticker_acct_keys:
            all_rows_in_ledger = con.execute(
                "SELECT DISTINCT trade_id FROM premium_ledger WHERE status = 'OPEN'"
            ).fetchall()
            for (tid,) in all_rows_in_ledger:
                if _ticker_acct_key(tid) in ticker_acct_keys and tid not in all_related_trade_ids:
                    all_related_trade_ids.append(tid)

        today_date = date.today()

        if all_related_trade_ids:
            placeholders = ', '.join('?' * len(all_related_trade_ids))
            stale_rows = con.execute(f"""
                SELECT leg_id, trade_id, expiry, credit_received
                FROM premium_ledger
                WHERE status = 'OPEN'
                  AND trade_id IN ({placeholders})
                  AND leg_id NOT LIKE '%_HIST_%'
            """, all_related_trade_ids).fetchall()

            for leg_id_db, trade_id_db, expiry_db, credit_db in stale_rows:
                # Reason 1: leg no longer in current CSV snapshot
                not_in_snapshot = str(leg_id_db) not in current_leg_ids
                # Reason 2: expiry date has passed (past-expiry auto-close)
                past_expiry = False
                if expiry_db:
                    try:
                        exp_str = str(expiry_db)[:10]  # YYYY-MM-DD
                        past_expiry = date.fromisoformat(exp_str) < today_date
                    except (ValueError, TypeError):
                        pass

                if not_in_snapshot or past_expiry:
                    reason = 'not in snapshot' if not_in_snapshot else 'expiry passed'
                    con.execute("""
                        UPDATE premium_ledger
                        SET status     = 'EXPIRED',
                            closed_at  = ?,
                            updated_at = ?,
                            notes      = COALESCE(notes, '') || ?
                        WHERE leg_id = ? AND status = 'OPEN'
                    """, [now_ts, now_ts, f' [auto-expired: {reason}]', leg_id_db])
                    logger.info(
                        f"[BuyWriteLedger] Auto-expired: {leg_id_db} "
                        f"(${credit_db:.4f}/share) — {reason}"
                    )

        # ── Step 2: Upsert currently open call legs ───────────────────────────
        if open_calls.empty:
            return

        for _, row in open_calls.iterrows():
            leg_id    = str(row.get('LegID', '') or '')
            trade_id  = str(row.get('TradeID', '') or '')
            if not leg_id or not trade_id:
                continue

            # Credit = absolute value of premium per share collected at entry.
            # Premium_Entry is frozen at the time the position was first observed.
            credit = abs(float(row.get('Premium_Entry', 0) or 0))
            if credit == 0:
                # Fallback: use current bid/ask mid if Premium_Entry is missing
                bid = float(row.get('Bid', 0) or 0)
                ask = float(row.get('Ask', 0) or 0)
                credit = (bid + ask) / 2 if bid > 0 else abs(float(row.get('Last', 0) or 0))

            contracts  = abs(int(float(row.get('Quantity', 1) or 1)))
            strike     = float(row.get('Strike', 0) or 0)
            expiry     = str(row.get('Expiration', '') or '')
            opened_at  = row.get('Entry_Snapshot_TS') or row.get('Snapshot_TS')

            # Determine cycle_number: count existing rows for this trade_id
            existing = con.execute(
                "SELECT COUNT(*) FROM premium_ledger WHERE trade_id = ?", [trade_id]
            ).fetchone()[0]
            cycle_num = int(existing) + 1

            # ON CONFLICT: preserve credit_received if it was manually corrected
            # (notes contains '[corrected') — those entries are the authoritative transaction
            # price and must not be overwritten by the frozen Premium_Entry from the broker CSV.
            con.execute("""
                INSERT INTO premium_ledger
                    (trade_id, leg_id, cycle_number, credit_received, contracts,
                     strike, expiry, opened_at, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
                ON CONFLICT (leg_id) DO UPDATE SET
                    credit_received = CASE
                        WHEN COALESCE(premium_ledger.notes, '') LIKE '%[corrected%'
                        THEN premium_ledger.credit_received  -- preserve manual correction
                        ELSE excluded.credit_received
                    END,
                    contracts       = excluded.contracts,
                    strike          = excluded.strike,
                    expiry          = excluded.expiry,
                    status          = 'OPEN',
                    updated_at      = excluded.updated_at
            """, [trade_id, leg_id, cycle_num, credit, contracts, strike, expiry, opened_at, now_ts])

    def _load_cumulative(self, bw_df: pd.DataFrame, con) -> dict[str, dict]:
        """
        Sum all credits in premium_ledger for each active trade_id, including credits
        from prior-cycle trade_ids that share the same ticker+account (e.g. after a roll
        creates a new trade_id like DKNG260306_24p5_CC_5376 while historical cycles are
        stored under DKNG260227_23p0_CC_5376).

        Deduplication: for each ticker_acct_key, if multiple rows share the same expiry date,
        only count the highest credit (HIST/manual backfills take priority over auto-recorded
        OPEN rows for the same cycle). This prevents double-counting when a call appears under
        two different trade_ids.

        Returns {trade_id: {'net': float, 'gross': float, 'close_cost': float}}
        keyed on the CURRENT trade_ids from bw_df.
        """
        trade_ids = bw_df['TradeID'].dropna().unique().tolist()
        if not trade_ids:
            return {}

        # Build ticker_acct_key → [current_trade_id, ...] map
        key_to_current: dict[str, list[str]] = {}
        for tid in trade_ids:
            k = _ticker_acct_key(tid)
            if k:
                key_to_current.setdefault(k, []).append(tid)

        # Fetch ALL premium_ledger rows with expiry for deduplication.
        # net_credit = credit_received - close_cost (cost to buy back the call before expiry).
        # For calls that expire worthless: close_cost = 0, net = full credit.
        # For debit rolls: close_cost > 0, net may be negative (debit roll cost exceeded premium).
        all_pl = con.execute(
            "SELECT trade_id, credit_received, COALESCE(close_cost, 0.0) AS close_cost, expiry, leg_id "
            "FROM premium_ledger"
        ).fetchall()

        # Group by (ticker_acct_key, expiry) and keep the MAX net credit per expiry window.
        # Store (net, gross, close_cost) per slot for each best entry.
        # Key: (ticker_acct_key, expiry_date_str) → (max_net, gross, close_cost)
        key_expiry_best: dict[tuple[str, str], tuple[float, float, float]] = {}

        for (lid_trade_id, credit, close_cost, expiry, leg_id) in all_pl:
            k = _ticker_acct_key(lid_trade_id)
            if not k or k not in key_to_current:
                continue
            exp_str = str(expiry or '')[:10] if expiry else 'unknown'
            slot = (k, exp_str)
            gross = float(credit or 0)
            cc    = float(close_cost or 0)
            net   = gross - cc
            existing = key_expiry_best.get(slot)
            if existing is None or net > existing[0]:
                key_expiry_best[slot] = (net, gross, cc)

        # Aggregate per ticker_acct_key
        key_net:   dict[str, float] = {}
        key_gross: dict[str, float] = {}
        key_cc:    dict[str, float] = {}
        key_count: dict[str, int]   = {}
        for (k, exp_str), (net, gross, cc) in key_expiry_best.items():
            key_net[k]   = key_net.get(k, 0.0)   + net
            key_gross[k] = key_gross.get(k, 0.0)  + gross
            key_cc[k]    = key_cc.get(k, 0.0)     + cc
            key_count[k] = key_count.get(k, 0)    + 1

        # Map back to current trade_ids
        result: dict[str, dict] = {}
        for tid in trade_ids:
            k = _ticker_acct_key(tid)
            if k and k in key_net:
                result[tid] = {
                    'net':        key_net[k],
                    'gross':      key_gross[k],
                    'close_cost': key_cc[k],
                }
            else:
                # Fallback: direct match (trade_id has no ticker_acct_key pattern)
                net_acc = gross_acc = cc_acc = 0.0
                for (lid_trade_id, credit, close_cost, expiry, leg_id) in all_pl:
                    if lid_trade_id == tid:
                        g = float(credit or 0)
                        c = float(close_cost or 0)
                        gross_acc += g
                        cc_acc    += c
                        net_acc   += (g - c)
                result[tid] = {'net': net_acc, 'gross': gross_acc, 'close_cost': cc_acc}

        # Store cache for _load_cycle_counts
        self._key_count_cache = (key_to_current, key_count)
        return result

    def _load_cycle_counts(self, bw_df: pd.DataFrame, con) -> dict[str, int]:
        """
        Returns {trade_id: number_of_cycles} — deduplicated by expiry across all same-ticker rolls.
        Used in doctrine rationale to say "N cycles collected to date."
        Reuses the aggregation cache from _load_cumulative if available.
        """
        trade_ids = bw_df['TradeID'].dropna().unique().tolist()
        if not trade_ids:
            return {}

        # Reuse cache from _load_cumulative if called in the same enrich() pass
        if hasattr(self, '_key_count_cache'):
            key_to_current, key_count = self._key_count_cache
            result: dict[str, int] = {}
            for tid in trade_ids:
                k = _ticker_acct_key(tid)
                result[tid] = key_count.get(k, 1) if k and k in key_count else 1
            return result

        # Fallback: recompute independently (same deduplication logic)
        key_to_current = {}
        for tid in trade_ids:
            k = _ticker_acct_key(tid)
            if k:
                key_to_current.setdefault(k, []).append(tid)

        all_pl = con.execute(
            "SELECT trade_id, COALESCE(close_cost, 0.0) AS close_cost, expiry FROM premium_ledger"
        ).fetchall()

        seen: dict[tuple[str, str], bool] = {}
        key_count: dict[str, int] = {}
        for (lid_trade_id, close_cost, expiry) in all_pl:
            k = _ticker_acct_key(lid_trade_id)
            if not k or k not in key_to_current:
                continue
            exp_str = str(expiry or '')[:10] if expiry else 'unknown'
            slot = (k, exp_str)
            if slot not in seen:
                seen[slot] = True
                key_count[k] = key_count.get(k, 0) + 1

        result = {}
        for tid in trade_ids:
            k = _ticker_acct_key(tid)
            result[tid] = key_count.get(k, 1) if k and k in key_count else 1
        return result

    def _load_roll_context(self, bw_df: pd.DataFrame, con) -> dict[str, dict]:
        """
        Determine net debit/credit of the most recent roll for each trade_id.

        A roll = closing one call (EXPIRED/ROLLED entry) and opening a new one (current OPEN).
        Net credit = new_call_credit - cost_to_close_old_call.

        For simple expiry rolls: the old call expires worthless (cost = 0), so
        net_credit = new_call_credit (always a credit).

        For early rolls (closing at a debit before expiry): we use the premium_ledger's
        most recently auto-expired credit vs the current OPEN credit.
        net_credit = current_open_credit - prior_expired_credit (positive = net credit, negative = net debit)

        Returns {trade_id: {'net_credit': float, 'prior_credit': float, 'is_debit': bool}}
        """
        trade_ids = bw_df['TradeID'].dropna().unique().tolist()
        if not trade_ids:
            return {}

        key_to_current: dict[str, list[str]] = {}
        for tid in trade_ids:
            k = _ticker_acct_key(tid)
            if k:
                key_to_current.setdefault(k, []).append(tid)

        if not key_to_current:
            return {}

        # Fetch all rows ordered by expiry for each key
        all_pl = con.execute(
            "SELECT trade_id, credit_received, expiry, status, leg_id "
            "FROM premium_ledger ORDER BY expiry DESC"
        ).fetchall()

        # For each ticker_acct_key: find the most recent EXPIRED entry and the current OPEN entry
        key_open:    dict[str, tuple[float, str]] = {}  # key → (credit, expiry)
        key_expired: dict[str, tuple[float, str]] = {}  # key → (credit, expiry) of most recent expired

        for (lid_tid, credit, expiry, status, leg_id) in all_pl:
            k = _ticker_acct_key(lid_tid)
            if not k or k not in key_to_current:
                continue
            c = float(credit or 0)
            exp_str = str(expiry or '')[:10]

            if status == 'OPEN' and k not in key_open:
                key_open[k] = (c, exp_str)
            elif status in ('EXPIRED', 'ROLLED') and k not in key_expired:
                key_expired[k] = (c, exp_str)

        result: dict[str, dict] = {}
        for tid in trade_ids:
            k = _ticker_acct_key(tid)
            if not k or k not in key_open:
                continue

            open_credit, open_expiry     = key_open[k]
            prior_credit, prior_expiry   = key_expired.get(k, (0.0, ''))

            # Net credit of the roll:
            # If the old call expired worthless → prior_credit = 0 (kept entire premium)
            # If rolled early → prior_credit is what was originally received on the closed leg.
            # Cost to close = prior_credit - (residual value at close). We don't have residual
            # directly, so we use the most conservative proxy: net = open - prior.
            # Negative = net debit roll (new call premium < cost to close old).
            net = open_credit - prior_credit

            result[tid] = {
                'net_credit':   net,
                'prior_credit': prior_credit,
                'is_debit':     net < -0.005,   # allow $0.005 tolerance for rounding
            }

        return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_columns(df: pd.DataFrame) -> None:
    for col in ('Net_Cost_Basis_Per_Share', 'Breakeven_Price', 'Cumulative_Premium_Collected',
                'Gross_Premium_Collected', 'Total_Close_Cost', 'Has_Debit_Rolls',
                'Roll_Net_Credit', 'Roll_Prior_Credit'):
        if col not in df.columns:
            df[col] = np.nan if col not in ('Has_Debit_Rolls',) else False


_TICKER_ACCT_RE = re.compile(r'^([A-Z]+)\d{6}.*?(\d{4})(?:_.*)?$')


def _ticker_acct_key(trade_id: str) -> Optional[str]:
    """
    Extract '{TICKER}_{ACCT}' from a trade_id like 'DKNG260306_24p5_CC_5376'.
    Returns None when the format doesn't match (don't cross-aggregate).
    """
    m = _TICKER_ACCT_RE.match(str(trade_id or ''))
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    return None
