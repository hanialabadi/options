"""
Position Tracker — Intraday snapshots for scaling, exit, and lifespan analysis.

Two data sources (auto-detects):
  A) Schwab Trader API  — full automation (requires trader scope on developer portal)
  B) CSV seed + Quotes  — reads last Fidelity CSV, refreshes prices/Greeks via marketdata API

Both paths store snapshots in DuckDB `position_snapshots` table, enabling:
  - Intraday P&L tracking (how the option behaves during the day)
  - Lifespan analysis (MFE/MAE over the full hold period)
  - Closure detection (position disappears from Trader API or option expires)
  - Scaling signals (position approaching profit target or stop loss)

Market hours only: 9:30 AM – 4:00 PM ET on trading days. No after-hours data.

Usage:
    python scripts/cli/fetch_positions.py                 # Auto-detect source, snapshot
    python scripts/cli/fetch_positions.py --diff           # Show changes since last snapshot
    python scripts/cli/fetch_positions.py --force          # Override market hours check
    python scripts/cli/fetch_positions.py --csv-only       # Save CSV, skip DuckDB
    python scripts/cli/fetch_positions.py --set-active     # Update fidelity_positions.csv symlink

Cron (every 15 min during market hours):
    */15 9-16 * * 1-5 cd /Users/haniabadi/Documents/Github/options && venv/bin/python scripts/cli/fetch_positions.py --quiet 2>>logs/fetch_positions.log
"""

import argparse
import logging
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BROKERAGE_DIR = _PROJECT_ROOT / "data" / "brokerage_inputs"
_DB_PATH = _PROJECT_ROOT / "data" / "pipeline.duckdb"
_SYMLINK_PATH = _BROKERAGE_DIR / "fidelity_positions.csv"

# Eastern timezone
_ET_OFFSET_STD = timezone(timedelta(hours=-5))
_ET_OFFSET_DST = timezone(timedelta(hours=-4))


def _get_et_now() -> datetime:
    utc_now = datetime.now(timezone.utc)
    return utc_now.astimezone(_ET_OFFSET_DST if 3 <= utc_now.month <= 11 else _ET_OFFSET_STD)


def _is_market_hours() -> bool:
    et = _get_et_now()
    if et.weekday() >= 5:
        return False
    t = et.hour * 60 + et.minute
    return 570 <= t < 960  # 9:30 AM – 4:00 PM


# ── OCC symbol parsing ──────────────────────────────────────────────────────

def _parse_occ_symbol(sym: str) -> dict | None:
    """Parse OCC option symbol → underlying, expiration, type, strike.

    Handles multiple formats:
      Standard OCC: "AAPL  260116C00240000"
      Fidelity compact: "AAPL270115C260"
      Fidelity spaced: "AAPL  260116C240"
    """
    sym = str(sym or "").strip()
    if not sym:
        return None

    # Standard OCC with 8-digit padded strike
    m = re.match(r'^([A-Z]{1,6})\s*(\d{6})([CP])(\d{8})$', sym)
    if m:
        return {
            "underlying": m.group(1),
            "exp_str": m.group(2),
            "call_put": m.group(3),
            "strike": float(m.group(4)) / 1000.0,
        }

    # Fidelity compact: "AAPL270115C260" or "EOSE260227C17.5"
    m = re.match(r'^([A-Z]{1,6})(\d{6})([CP])([\d.]+)$', sym)
    if m:
        return {
            "underlying": m.group(1),
            "exp_str": m.group(2),
            "call_put": m.group(3),
            "strike": float(m.group(4)),
        }

    # Spaced: "AAPL  260116C240"
    parts = sym.split()
    if len(parts) >= 2:
        m2 = re.match(r'(\d{6})([CP])([\d.]+)', parts[-1])
        if m2:
            return {
                "underlying": parts[0],
                "exp_str": m2.group(1),
                "call_put": m2.group(2),
                "strike": float(m2.group(3)),
            }

    return None


# ── Source A: Schwab Trader API ──────────────────────────────────────────────

def _try_trader_api(client) -> pd.DataFrame | None:
    """Attempt to fetch positions via Trader API. Returns None if 401."""
    import requests

    try:
        token = client._get_access_token()
        resp = requests.get(
            "https://api.schwabapi.com/trader/v1/accounts",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params={"fields": "positions"},
            timeout=20,
        )
        if resp.status_code == 401:
            return None  # No trader access — fall back to CSV seed
        resp.raise_for_status()

        accounts = resp.json()
        rows = []
        et_now = _get_et_now()
        snapshot_ts = et_now.strftime("%m/%d/%Y at %I:%M:%S %p ET")

        for acct in accounts:
            si = acct.get("securitiesAccount", {})
            acct_num = si.get("accountNumber", "")
            for pos in si.get("positions", []):
                inst = pos.get("instrument", {})
                asset_type = inst.get("assetType", "")
                long_qty = float(pos.get("longQuantity", 0) or 0)
                short_qty = float(pos.get("shortQuantity", 0) or 0)
                quantity = long_qty - short_qty
                avg_price = float(pos.get("averagePrice", 0) or 0)
                market_value = float(pos.get("marketValue", 0) or 0)

                if asset_type == "EQUITY":
                    rows.append({
                        "Symbol": inst.get("symbol", ""),
                        "Quantity": quantity, "UL Last": avg_price, "Last": avg_price,
                        "Basis": abs(avg_price * quantity),
                        "Type": "Equity", "Strike": "", "Call/Put": "", "Expiration": "",
                        "Account": acct_num, "As of Date/Time": snapshot_ts,
                        "Delta": "", "Gamma": "", "Theta": "", "Vega": "",
                    })
                elif asset_type == "OPTION":
                    put_call = inst.get("putCall", "")
                    strike = float(inst.get("strikePrice", 0) or 0)
                    exp_raw = inst.get("expirationDate", "")
                    exp_fmt = ""
                    if exp_raw:
                        try:
                            exp_fmt = datetime.fromisoformat(
                                exp_raw.replace("Z", "+00:00")
                            ).strftime("%m/%d/%Y")
                        except Exception:
                            exp_fmt = exp_raw

                    rows.append({
                        "Symbol": inst.get("symbol", ""),
                        "Quantity": quantity, "UL Last": "", "Last": abs(avg_price),
                        "Basis": "", "Type": "Option",
                        "Strike": strike, "Call/Put": "C" if put_call == "CALL" else "P",
                        "Expiration": exp_fmt,
                        "Account": acct_num, "As of Date/Time": snapshot_ts,
                        "Delta": "", "Gamma": "", "Theta": "", "Vega": "",
                    })

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as e:
        logger.debug(f"Trader API failed: {e}")
        return None


# ── Source B: CSV Seed + Marketdata Quotes ────────────────────────────────────

def _load_csv_seed() -> pd.DataFrame:
    """Load the most recent Fidelity/Schwab positions CSV."""
    if _SYMLINK_PATH.exists():
        target = _SYMLINK_PATH.resolve()
    else:
        # Find latest CSV by modification time
        csvs = sorted(_BROKERAGE_DIR.glob("Positions_*.csv"), key=lambda p: p.stat().st_mtime)
        if not csvs:
            return pd.DataFrame()
        target = csvs[-1]

    try:
        # Fidelity CSVs may have 2 metadata header rows
        with open(target, "r", encoding="utf-8-sig") as f:
            first_line = f.readline()
        if "Symbol" in first_line:
            df = pd.read_csv(target, encoding="utf-8-sig")
        else:
            df = pd.read_csv(target, skiprows=2, encoding="utf-8-sig")
        logger.info(f"Loaded CSV seed: {target.name} ({len(df)} rows)")
        return df
    except Exception as e:
        logger.warning(f"Failed to load CSV seed: {e}")
        return pd.DataFrame()


def _refresh_quotes_from_csv(df_seed: pd.DataFrame, client) -> pd.DataFrame:
    """
    Take positions from CSV seed and refresh prices + Greeks via marketdata API.
    Uses get_quotes() for stocks and get_chains() for options.
    """
    if df_seed.empty:
        return df_seed

    df = df_seed.copy()
    et_now = _get_et_now()
    df["As of Date/Time"] = et_now.strftime("%m/%d/%Y at %I:%M:%S %p ET")

    # Identify option vs stock rows
    # Fidelity uses Call/Put = "Call"/"Put" or "--"/NaN for stocks
    # Also detect options by OCC symbol pattern
    underlyings = set()
    option_tickers = {}  # underlying → list of row indices needing chain refresh

    for idx, row in df.iterrows():
        sym = str(row.get("Symbol", "") or "").strip()
        if not sym:
            continue

        cp = str(row.get("Call/Put", "") or "").strip()
        is_option = cp in ("Call", "Put", "C", "P")

        parsed = _parse_occ_symbol(sym)
        if parsed or is_option:
            # Extract underlying ticker — NOT the OCC symbol
            ul = parsed["underlying"] if parsed else sym
            underlyings.add(ul)
            option_tickers.setdefault(ul, []).append(idx)
        else:
            underlyings.add(sym)

    if not underlyings:
        return df

    # Batch stock quotes (up to 500 per call)
    ticker_list = sorted(underlyings)
    quotes = {}
    try:
        for i in range(0, len(ticker_list), 50):
            batch = ticker_list[i:i+50]
            result = client.get_quotes(batch, fields="quote")
            for sym, data in result.items():
                q = data.get("quote", {})
                quotes[sym] = {
                    "last": float(q.get("lastPrice", 0) or 0),
                    "bid": float(q.get("bidPrice", 0) or 0),
                    "ask": float(q.get("askPrice", 0) or 0),
                    "volume": int(q.get("totalVolume", 0) or 0),
                }
            if i + 50 < len(ticker_list):
                time.sleep(0.3)
    except Exception as e:
        logger.warning(f"Stock quotes failed: {e}")

    # Update stock rows with live prices
    for idx, row in df.iterrows():
        sym = str(row.get("Symbol", "") or "").strip()
        parsed = _parse_occ_symbol(sym)
        if parsed:
            # Option row — update UL Last from underlying quote
            ul = parsed["underlying"]
            if ul in quotes:
                df.at[idx, "UL Last"] = quotes[ul]["last"]
        elif sym in quotes:
            # Stock row
            q = quotes[sym]
            df.at[idx, "Last"] = q["last"]
            df.at[idx, "UL Last"] = q["last"]
            df.at[idx, "Bid"] = q["bid"]
            df.at[idx, "Ask"] = q["ask"]

    # Refresh option Greeks via chains (one chain per underlying, throttled)
    refreshed = 0
    for ul, indices in option_tickers.items():
        try:
            chain_data = client.get_chains(
                symbol=ul, strikeCount=40, range="ALL", strategy="SINGLE"
            )
            time.sleep(0.5)  # Respect rate limits

            # Build lookup: (expDate, strike, C/P) → greeks
            chain_map = {}
            for map_key in ["callExpDateMap", "putExpDateMap"]:
                exp_map = chain_data.get(map_key, {})
                for exp_key, strikes in exp_map.items():
                    # exp_key like "2026-03-20:10"
                    exp_date = exp_key.split(":")[0] if ":" in exp_key else exp_key
                    for strike_str, contracts in strikes.items():
                        for contract in contracts:
                            cp = contract.get("putCall", "")
                            k = (exp_date, float(strike_str), cp)
                            chain_map[k] = contract

            # Match each option row to chain data
            for idx in indices:
                row = df.loc[idx]
                sym = str(row.get("Symbol", "") or "")
                parsed = _parse_occ_symbol(sym)

                # Build match key from either OCC parse or CSV columns
                if parsed:
                    exp_ymd = "20" + parsed["exp_str"][:2] + "-" + parsed["exp_str"][2:4] + "-" + parsed["exp_str"][4:6]
                    cp = "CALL" if parsed["call_put"] == "C" else "PUT"
                    strike_val = parsed["strike"]
                else:
                    # Use CSV columns directly (Fidelity format)
                    exp_raw = str(row.get("Expiration", "") or "")
                    cp_raw = str(row.get("Call/Put", "") or "")
                    strike_val = _safe_float(row.get("Strike"), 0)
                    cp = "CALL" if cp_raw in ("Call", "C") else "PUT"
                    # Parse Fidelity date format: "Jan-15-2027" or "01/15/2027"
                    exp_ymd = ""
                    for fmt in ("%b-%d-%Y", "%m/%d/%Y", "%Y-%m-%d"):
                        try:
                            exp_ymd = datetime.strptime(exp_raw, fmt).strftime("%Y-%m-%d")
                            break
                        except Exception:
                            pass

                if not exp_ymd or not strike_val:
                    continue

                match_key = (exp_ymd, strike_val, cp)
                contract = chain_map.get(match_key)
                if contract:
                    df.at[idx, "Last"] = float(contract.get("last", 0) or 0)
                    df.at[idx, "Bid"] = float(contract.get("bid", 0) or 0)
                    df.at[idx, "Ask"] = float(contract.get("ask", 0) or 0)
                    df.at[idx, "Delta"] = float(contract.get("delta", 0) or 0)
                    df.at[idx, "Gamma"] = float(contract.get("gamma", 0) or 0)
                    df.at[idx, "Theta"] = float(contract.get("theta", 0) or 0)
                    df.at[idx, "Vega"] = float(contract.get("vega", 0) or 0)
                    df.at[idx, "IV"] = float(contract.get("volatility", 0) or 0)
                    df.at[idx, "Open Int"] = int(contract.get("openInterest", 0) or 0)
                    df.at[idx, "Volume"] = int(contract.get("totalVolume", 0) or 0)
                    refreshed += 1

        except Exception as e:
            logger.warning(f"Chain refresh for {ul} failed: {e}")

    logger.info(f"Refreshed {refreshed}/{sum(len(v) for v in option_tickers.values())} option contracts")
    return df


# ── DuckDB Snapshot Storage ──────────────────────────────────────────────────

_DDL_POSITION_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS position_snapshots (
    snapshot_ts     TIMESTAMP NOT NULL,
    symbol          VARCHAR NOT NULL,
    account         VARCHAR,
    asset_type      VARCHAR,
    underlying      VARCHAR,
    quantity        DOUBLE,
    last_price      DOUBLE,
    bid             DOUBLE,
    ask             DOUBLE,
    basis           DOUBLE,
    unrealized_pnl  DOUBLE,
    delta           DOUBLE,
    gamma           DOUBLE,
    theta           DOUBLE,
    vega            DOUBLE,
    iv              DOUBLE,
    strike          DOUBLE,
    expiration      DATE,
    call_put        VARCHAR,
    PRIMARY KEY (snapshot_ts, symbol, account)
)
"""


def _safe_float(val, default=None):
    if val is None or val == "" or val == "--":
        return default
    try:
        import math
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _store_snapshot(df: pd.DataFrame, con) -> int:
    if df.empty:
        return 0

    con.execute(_DDL_POSITION_SNAPSHOTS)
    snapshot_ts = _get_et_now()

    rows = []
    for _, r in df.iterrows():
        symbol = str(r.get("Symbol", "") or "").strip()
        if not symbol:
            continue

        parsed = _parse_occ_symbol(symbol)
        is_option = parsed is not None

        exp_date = None
        exp_str = str(r.get("Expiration", "") or "").strip()
        if exp_str:
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    exp_date = datetime.strptime(exp_str, fmt).date()
                    break
                except Exception:
                    pass

        rows.append({
            "snapshot_ts": snapshot_ts,
            "symbol": symbol,
            "account": str(r.get("Account", "") or ""),
            "asset_type": "OPTION" if is_option else "EQUITY",
            "underlying": parsed["underlying"] if parsed else symbol,
            "quantity": _safe_float(r.get("Quantity"), 0),
            "last_price": _safe_float(r.get("Last")),
            "bid": _safe_float(r.get("Bid")),
            "ask": _safe_float(r.get("Ask")),
            "basis": _safe_float(r.get("Basis")),
            "unrealized_pnl": _safe_float(r.get("$ Total G/L"), 0),
            "delta": _safe_float(r.get("Delta")),
            "gamma": _safe_float(r.get("Gamma")),
            "theta": _safe_float(r.get("Theta")),
            "vega": _safe_float(r.get("Vega")),
            "iv": _safe_float(r.get("IV")),
            "strike": _safe_float(r.get("Strike")),
            "expiration": exp_date,
            "call_put": str(r.get("Call/Put", "") or "") or None,
        })

    if not rows:
        return 0

    snap_df = pd.DataFrame(rows)
    con.execute("INSERT OR IGNORE INTO position_snapshots SELECT * FROM snap_df")
    return len(rows)


# ── Closure Detection ────────────────────────────────────────────────────────

def _detect_closures(df_current: pd.DataFrame, con) -> list[dict]:
    """
    Positions in previous snapshot but absent now → closed/expired/assigned.
    Also flags options past their expiration date.
    """
    if df_current.empty:
        return []

    try:
        con.execute(_DDL_POSITION_SNAPSHOTS)
        prev = con.execute("""
            SELECT DISTINCT symbol, account, quantity, snapshot_ts, expiration, asset_type
            FROM position_snapshots
            WHERE snapshot_ts = (
                SELECT MAX(snapshot_ts) FROM position_snapshots
                WHERE snapshot_ts < CURRENT_TIMESTAMP - INTERVAL '1 minute'
            )
        """).fetchdf()

        if prev.empty:
            return []

        current_symbols = set(
            (str(r["Symbol"]).strip(), str(r.get("Account", "")).strip())
            for _, r in df_current.iterrows()
            if r.get("Symbol")
        )

        closed = []
        today = _get_et_now().date()

        for _, r in prev.iterrows():
            key = (str(r["symbol"]).strip(), str(r["account"]).strip())

            # Check 1: position disappeared from current data
            gone = key not in current_symbols

            # Check 2: option past expiration date
            expired = False
            if r.get("asset_type") == "OPTION" and r.get("expiration") is not None:
                try:
                    exp_d = pd.Timestamp(r["expiration"]).date()
                    expired = exp_d < today
                except Exception:
                    pass

            if gone or expired:
                reason = "expired" if expired else "closed/assigned"
                closed.append({
                    "symbol": r["symbol"],
                    "account": r["account"],
                    "last_seen": r["snapshot_ts"],
                    "quantity": r["quantity"],
                    "reason": reason,
                })

        return closed

    except Exception as e:
        logger.debug(f"Closure detection failed (non-fatal): {e}")
        return []


# ── Diff Report ──────────────────────────────────────────────────────────────

def _diff_report(df_current: pd.DataFrame, con) -> str:
    lines = []
    try:
        con.execute(_DDL_POSITION_SNAPSHOTS)
        prev = con.execute("""
            SELECT symbol, account, quantity, last_price, unrealized_pnl, delta, iv, snapshot_ts
            FROM position_snapshots
            WHERE snapshot_ts = (SELECT MAX(snapshot_ts) FROM position_snapshots)
        """).fetchdf()

        if prev.empty:
            return "No previous snapshot — this is the first fetch."

        prev_ts = prev["snapshot_ts"].iloc[0]
        lines.append(f"Comparing to last snapshot: {prev_ts}")
        lines.append("")

        prev_map = {(str(r["symbol"]), str(r["account"])): r for _, r in prev.iterrows()}
        curr_map = {}
        for _, r in df_current.iterrows():
            sym = str(r.get("Symbol", "")).strip()
            acct = str(r.get("Account", "")).strip()
            if sym:
                curr_map[(sym, acct)] = r

        new = set(curr_map) - set(prev_map)
        gone = set(prev_map) - set(curr_map)
        common = set(curr_map) & set(prev_map)

        if new:
            lines.append(f"NEW ({len(new)}):")
            for k in sorted(new):
                lines.append(f"  + {k[0]:30s}  qty={curr_map[k].get('Quantity', '?')}")
            lines.append("")

        if gone:
            lines.append(f"GONE ({len(gone)}):")
            for k in sorted(gone):
                lines.append(f"  - {k[0]:30s}  qty={prev_map[k].get('quantity', '?')}")
            lines.append("")

        changes = []
        for k in sorted(common):
            p, c = prev_map[k], curr_map[k]
            p_price = float(p.get("last_price", 0) or 0)
            c_price = _safe_float(c.get("Last"), 0)
            p_delta = float(p.get("delta", 0) or 0)
            c_delta = _safe_float(c.get("Delta"), 0)
            price_chg = c_price - p_price if p_price and c_price else 0

            if abs(price_chg) > 0.01 or _safe_float(c.get("Quantity")) != float(p.get("quantity", 0) or 0):
                pct = (price_chg / p_price * 100) if p_price else 0
                changes.append((k[0], p_price, c_price, price_chg, pct, p_delta, c_delta))

        if changes:
            lines.append(f"PRICE CHANGES ({len(changes)}):")
            for sym, pp, cp, chg, pct, pd_, cd_ in changes:
                delta_note = f"  Δ{cd_:.2f}" if cd_ else ""
                lines.append(f"  ~ {sym:30s}  ${pp:.2f}→${cp:.2f} ({chg:+.2f}, {pct:+.1f}%){delta_note}")
            lines.append("")

        if not new and not gone and not changes:
            lines.append("No changes since last snapshot.")

    except Exception as e:
        lines.append(f"Diff failed: {e}")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Position tracker — intraday snapshots")
    parser.add_argument("--force", action="store_true", help="Skip market hours check")
    parser.add_argument("--csv-only", action="store_true", help="Save CSV only, no DuckDB")
    parser.add_argument("--diff", action="store_true", help="Show changes since last snapshot")
    parser.add_argument("--set-active", action="store_true", help="Update symlink to new CSV")
    parser.add_argument("--quiet", action="store_true", help="Minimal output (for cron)")
    args = parser.parse_args()

    # Market hours guard
    if not args.force and not _is_market_hours():
        et = _get_et_now()
        if not args.quiet:
            print(f"Market closed ({et.strftime('%I:%M %p ET %A')}). Use --force to override.")
        return

    # Auth
    try:
        from scan_engine.loaders.schwab_api_client import SchwabClient
        client = SchwabClient()
    except RuntimeError as e:
        print(f"Auth error: {e}")
        sys.exit(1)

    # Try Source A: Trader API (full automation)
    df = _try_trader_api(client)
    source = "Schwab Trader API"

    if df is None:
        # Fall back to Source B: CSV seed + marketdata quotes
        if not args.quiet:
            print("Trader API not available — using CSV seed + live quotes")
        df_seed = _load_csv_seed()
        if df_seed.empty:
            print("No CSV seed found in data/brokerage_inputs/. Import a Fidelity CSV first.")
            sys.exit(1)
        df = _refresh_quotes_from_csv(df_seed, client)
        source = f"CSV seed ({_SYMLINK_PATH.resolve().name})"

    if df.empty:
        print("No positions found.")
        return

    if not args.quiet:
        # Count options by Call/Put or OCC symbol pattern
        n_opt = sum(
            1 for _, r in df.iterrows()
            if str(r.get("Call/Put", "")).strip() in ("Call", "Put", "C", "P")
            or _parse_occ_symbol(str(r.get("Symbol", ""))) is not None
        )
        n_eq = len(df) - n_opt
        print(f"Source: {source}")
        print(f"Positions: {len(df)} ({n_eq} equities, {n_opt} options)")

    # Save CSV
    et = _get_et_now()
    csv_name = f"Positions_Live_{et.strftime('%Y%m%d_%H%M%S')}.csv"
    csv_path = _BROKERAGE_DIR / csv_name
    _BROKERAGE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    if not args.quiet:
        print(f"Saved: {csv_path.name}")

    if args.set_active:
        if _SYMLINK_PATH.is_symlink() or _SYMLINK_PATH.exists():
            _SYMLINK_PATH.unlink()
        _SYMLINK_PATH.symlink_to(csv_name)
        if not args.quiet:
            print(f"Symlink: fidelity_positions.csv → {csv_name}")

    # DuckDB snapshot
    if not args.csv_only:
        try:
            import duckdb
            con = duckdb.connect(str(_DB_PATH))

            if args.diff:
                print()
                print(_diff_report(df, con))

            closed = _detect_closures(df, con)
            if closed and not args.quiet:
                print(f"\nCLOSURES DETECTED ({len(closed)}):")
                for c in closed:
                    print(f"  {c['symbol']:30s}  qty={c['quantity']:>6.0f}  {c['reason']}")

            n = _store_snapshot(df, con)
            con.close()
            if not args.quiet:
                print(f"Stored {n} rows in position_snapshots")

        except Exception as e:
            print(f"DuckDB error (non-fatal): {e}")

    elif args.diff:
        try:
            import duckdb
            con = duckdb.connect(str(_DB_PATH), read_only=True)
            print()
            print(_diff_report(df, con))
            con.close()
        except Exception as e:
            print(f"Diff failed: {e}")


if __name__ == "__main__":
    main()
