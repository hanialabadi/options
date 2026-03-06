"""
cli/backfill_premium_cycles.py
================================
Manually backfill historical call cycles into premium_ledger for BUY_WRITE positions.

Use this when a position has prior expired/rolled calls that aren't in the current
Fidelity CSV — the system can't auto-detect them because they no longer appear.

McMillan Ch.3: every call cycle written against the stock reduces the effective
cost basis. Without this history, the hard stop trigger fires at the wrong price.

Usage:
    # Backfill DKNG's three cycles
    python cli/backfill_premium_cycles.py \\
        --trade-id DKNG260227_23p0_CC_5376 \\
        --cycles \\
            "DKNG260130C30.5|623.26|10|30.5|2026-01-30|EXPIRED" \\
            "DKNG260213C29|1203.26|10|29.0|2026-02-13|EXPIRED" \\
            "DKNG260220C22|209.03|10|22.0|2026-02-20|EXPIRED"

    # Show current ledger for a trade
    python cli/backfill_premium_cycles.py --show DKNG260227_23p0_CC_5376

    # Show all trades with premium history
    python cli/backfill_premium_cycles.py --list

Cycle format:  "ContractSymbol|CreditPerShare|Contracts|Strike|Expiry|Status"
    ContractSymbol : any unique identifier (e.g. DKNG260130C30.5)
    CreditPerShare : premium collected PER SHARE (not total dollars)
    Contracts      : number of contracts
    Strike         : call strike price
    Expiry         : YYYY-MM-DD
    Status         : EXPIRED | ROLLED | ASSIGNED (default EXPIRED)
"""

from __future__ import annotations

import argparse
import sys
import logging
from datetime import datetime, date
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

_PIPELINE_DB = "data/pipeline.duckdb"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS premium_ledger (
    trade_id        VARCHAR NOT NULL,
    leg_id          VARCHAR PRIMARY KEY,
    cycle_number    INTEGER DEFAULT 1,
    credit_received DOUBLE  NOT NULL,
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


def _parse_cycle(raw: str, trade_id: str, cycle_num: int) -> dict:
    """Parse 'Symbol|credit|contracts|strike|expiry|status' into a dict."""
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 5:
        raise ValueError(
            f"Cycle must have at least 5 fields: Symbol|credit|contracts|strike|expiry  got: {raw!r}"
        )
    symbol   = parts[0]
    credit   = float(parts[1])
    contracts = int(parts[2])
    strike   = float(parts[3])
    expiry   = parts[4]
    status   = parts[5].upper() if len(parts) > 5 else "EXPIRED"

    if status not in ("EXPIRED", "ROLLED", "ASSIGNED", "OPEN"):
        raise ValueError(f"Status must be EXPIRED | ROLLED | ASSIGNED | OPEN, got: {status!r}")

    leg_id = f"{trade_id}_HIST_{symbol}"
    closed_at = None
    if status != "OPEN":
        try:
            closed_at = datetime.strptime(expiry, "%Y-%m-%d")
        except ValueError:
            closed_at = None

    return {
        "trade_id":       trade_id,
        "leg_id":         leg_id,
        "cycle_number":   cycle_num,
        "credit_received": credit,
        "contracts":      contracts,
        "strike":         strike,
        "expiry":         expiry,
        "opened_at":      None,
        "closed_at":      closed_at,
        "status":         status,
        "notes":          f"Manual backfill: {symbol}",
    }


def backfill(trade_id: str, cycles: list[str], db_path: str = _PIPELINE_DB) -> None:
    con = duckdb.connect(db_path)
    con.execute(_CREATE_TABLE_SQL)

    # Determine current max cycle_number for this trade
    existing_max = con.execute(
        "SELECT COALESCE(MAX(cycle_number), 0) FROM premium_ledger WHERE trade_id = ?",
        [trade_id]
    ).fetchone()[0]

    inserted = 0
    skipped = 0

    for i, raw in enumerate(cycles):
        cycle_num = existing_max + i + 1
        try:
            rec = _parse_cycle(raw, trade_id, cycle_num)
        except ValueError as e:
            logger.error(f"  Skipping cycle {i+1}: {e}")
            skipped += 1
            continue

        # Check if already exists (by leg_id)
        exists = con.execute(
            "SELECT 1 FROM premium_ledger WHERE leg_id = ?", [rec["leg_id"]]
        ).fetchone()

        if exists:
            logger.info(f"  Cycle {cycle_num} already exists (leg_id={rec['leg_id']!r}) — skipping")
            skipped += 1
            continue

        con.execute("""
            INSERT INTO premium_ledger
                (trade_id, leg_id, cycle_number, credit_received, contracts,
                 strike, expiry, opened_at, closed_at, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            rec["trade_id"], rec["leg_id"], rec["cycle_number"],
            rec["credit_received"], rec["contracts"],
            rec["strike"], rec["expiry"],
            rec["opened_at"], rec["closed_at"],
            rec["status"], rec["notes"],
        ])
        inserted += 1
        logger.info(
            f"  ✅ Cycle {cycle_num}: {rec['notes']} — "
            f"${rec['credit_received']:.2f}/share × {rec['contracts']} contracts "
            f"(${rec['credit_received'] * rec['contracts'] * 100:,.2f} total) | {rec['status']}"
        )

    # Show updated summary
    total = con.execute(
        "SELECT SUM(credit_received), COUNT(*) FROM premium_ledger WHERE trade_id = ?",
        [trade_id]
    ).fetchone()
    total_credit = float(total[0] or 0)
    total_cycles = int(total[1] or 0)

    logger.info(f"\n{'─'*60}")
    logger.info(f"  TradeID : {trade_id}")
    logger.info(f"  Cycles  : {total_cycles} ({inserted} inserted, {skipped} skipped)")
    logger.info(f"  Total premium/share collected : ${total_credit:.4f}")

    # Show net cost if stock basis available
    try:
        anchors = con.execute("""
            SELECT Basis, Quantity FROM entry_anchors
            WHERE TradeID = ? AND LegType = 'STOCK'
            LIMIT 1
        """, [trade_id]).fetchone()
        if anchors:
            basis, qty = float(anchors[0] or 0), abs(float(anchors[1] or 1))
            stock_cost = basis / qty if qty > 0 else 0
            net_cost   = stock_cost - total_credit
            hard_stop  = net_cost * 0.80
            logger.info(f"  Stock cost/share              : ${stock_cost:.2f}")
            logger.info(f"  Net cost/share (after premium): ${net_cost:.2f}")
            logger.info(f"  Hard stop price (-20%)        : ${hard_stop:.2f}")
    except Exception:
        pass

    con.close()


def show(trade_id: str, db_path: str = _PIPELINE_DB) -> None:
    con = duckdb.connect(db_path, read_only=True)
    con.execute(_CREATE_TABLE_SQL)
    df = con.execute("""
        SELECT cycle_number, leg_id, credit_received, contracts,
               strike, expiry, status, notes, opened_at, closed_at
        FROM premium_ledger
        WHERE trade_id = ?
        ORDER BY cycle_number
    """, [trade_id]).df()
    con.close()

    if df.empty:
        logger.info(f"No premium_ledger entries for trade_id={trade_id!r}")
        return

    total_credit = df["credit_received"].sum()
    pd.set_option("display.max_colwidth", 50)
    pd.set_option("display.width", 120)
    print(f"\nPremium Ledger — {trade_id}")
    print("─" * 80)
    print(df.to_string(index=False))
    print("─" * 80)
    print(f"  Total credit/share: ${total_credit:.4f}")


def list_all(db_path: str = _PIPELINE_DB) -> None:
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute("""
            SELECT trade_id,
                   COUNT(*)            AS cycles,
                   SUM(credit_received) AS total_credit_per_share,
                   MIN(expiry)         AS first_expiry,
                   MAX(expiry)         AS latest_expiry
            FROM premium_ledger
            GROUP BY trade_id
            ORDER BY trade_id
        """).df()
    except Exception:
        logger.info("premium_ledger table does not exist yet.")
        con.close()
        return
    con.close()

    if df.empty:
        logger.info("No entries in premium_ledger.")
        return

    pd.set_option("display.width", 120)
    print("\nPremium Ledger — All Positions")
    print("─" * 80)
    print(df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill historical premium cycles into premium_ledger",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--trade-id", help="TradeID to backfill (e.g. DKNG260227_23p0_CC_5376)")
    parser.add_argument(
        "--cycles", nargs="+",
        help="Cycle entries: 'Symbol|credit/share|contracts|strike|expiry|status'"
    )
    parser.add_argument("--show", metavar="TRADE_ID", help="Show ledger for a specific trade")
    parser.add_argument("--list", action="store_true", help="List all trades with premium history")
    parser.add_argument("--db", default=_PIPELINE_DB, help="DuckDB path")

    args = parser.parse_args()

    if args.list:
        list_all(args.db)
    elif args.show:
        show(args.show, args.db)
    elif args.trade_id and args.cycles:
        backfill(args.trade_id, args.cycles, args.db)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
