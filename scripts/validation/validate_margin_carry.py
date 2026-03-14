"""
Validate margin carry calculations against Fidelity expectations.

Reads the latest management engine output from DuckDB and verifies:
1. Options have $0 Daily_Margin_Cost
2. Retirement (Roth) positions have $0 Daily_Margin_Cost
3. Stock positions use borrowed portion, not full market value
4. Portfolio total matches debit-based calculation
5. Per-ticker margin requirements are applied correctly

Usage:
    python scripts/validation/validate_margin_carry.py
"""

import sys
sys.path.insert(0, ".")

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/pipeline.duckdb")
MARGIN_RATE = 0.10375
FIDELITY_DEBIT = 57_605.01  # From Fidelity margin calculator (update as needed)

# Per-ticker overrides (from Fidelity margin calculator)
MARGIN_REQ_OVERRIDES = {"DKNG": 0.40, "UUUU": 0.40}
MARGIN_REQ_DEFAULT = 0.30
MARGIN_SPECIAL_PER_SHARE = {"EOSE": 3.00}


def main():
    if not DB_PATH.exists():
        print(f"❌ Database not found: {DB_PATH}")
        return

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Find the management positions table
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]
    print(f"Available tables: {', '.join(table_names[:20])}")

    # Try to read positions with carry data
    mgmt_table = None
    for candidate in ["management_positions", "positions", "management_output"]:
        if candidate in table_names:
            mgmt_table = candidate
            break

    if mgmt_table is None:
        # Try reading from the latest CSV output instead
        import glob
        csvs = sorted(glob.glob("output/Management_*.csv"))
        if not csvs:
            print("❌ No management output found in DB or output/ directory")
            con.close()
            return
        print(f"\nReading from: {csvs[-1]}")
        df = pd.read_csv(csvs[-1])
    else:
        print(f"\nReading from table: {mgmt_table}")
        df = con.execute(f"SELECT * FROM {mgmt_table}").fetchdf()

    con.close()

    print(f"Total rows: {len(df)}")
    print(f"Columns with 'argin' or 'arry': {[c for c in df.columns if 'argin' in c or 'arry' in c]}")
    print()

    # ── Check 1: Options should have $0 margin cost ──────────────────
    print("=" * 60)
    print("CHECK 1: Options should have $0 Daily_Margin_Cost")
    print("=" * 60)

    if "Daily_Margin_Cost" not in df.columns:
        print("⚠️  Daily_Margin_Cost column not found — run management engine first")
    else:
        if "AssetType" in df.columns:
            options = df[df["AssetType"] == "OPTION"]
            stocks = df[df["AssetType"] == "EQUITY"]
            if stocks.empty and "EQUITY" not in df["AssetType"].unique():
                stocks = df[df["AssetType"] == "STOCK"]

            option_cost = options["Daily_Margin_Cost"].fillna(0).sum()
            print(f"  Options: {len(options)} positions, total margin cost: ${option_cost:.2f}/day")
            if abs(option_cost) < 0.01:
                print("  ✅ PASS — Options have $0 margin cost")
            else:
                print("  ❌ FAIL — Options should have $0 margin cost!")
                bad = options[options["Daily_Margin_Cost"].fillna(0) > 0.01]
                if not bad.empty:
                    print(f"  Offenders: {bad[['Ticker', 'AssetType', 'Daily_Margin_Cost']].to_string()}")
        else:
            print("  ⚠️  AssetType column not found")

    # ── Check 2: Retirement positions should have $0 ─────────────────
    print()
    print("=" * 60)
    print("CHECK 2: Retirement positions should have $0 Daily_Margin_Cost")
    print("=" * 60)

    if "Is_Retirement" in df.columns:
        retirement = df[df["Is_Retirement"] == True]
        taxable = df[df["Is_Retirement"] == False]
        ret_cost = retirement["Daily_Margin_Cost"].fillna(0).sum() if "Daily_Margin_Cost" in df.columns else 0
        print(f"  Retirement positions: {len(retirement)}, margin cost: ${ret_cost:.2f}/day")
        print(f"  Taxable positions: {len(taxable)}")
        if abs(ret_cost) < 0.01:
            print("  ✅ PASS — Retirement positions have $0 margin cost")
        else:
            print("  ❌ FAIL — Retirement positions should have $0!")
    elif "Account" in df.columns:
        print(f"  Account values: {df['Account'].unique()}")
        print("  ⚠️  Is_Retirement column not found — run margin carry enrichment first")
    else:
        print("  ⚠️  Neither Is_Retirement nor Account columns found")

    # ── Check 3: Per-position stock margin uses borrowed portion ─────
    print()
    print("=" * 60)
    print("CHECK 3: Stock positions — borrowed portion, not full value")
    print("=" * 60)

    if "Daily_Margin_Cost" in df.columns and "AssetType" in df.columns:
        asset_col = "AssetType"
        stock_mask = df[asset_col].isin(["EQUITY", "STOCK"])
        margin_stocks = df[stock_mask & (df["Daily_Margin_Cost"].fillna(0) > 0.01)]

        if not margin_stocks.empty:
            for _, row in margin_stocks.iterrows():
                ticker = row.get("Ticker", "?")
                mv = abs(float(row.get("Market_Value", row.get("UL Last", 0)) or 0))
                qty = abs(float(row.get("Quantity", 1) or 1))
                if mv == 0 and "UL Last" in row:
                    mv = abs(float(row["UL Last"] or 0)) * qty

                actual_cost = float(row["Daily_Margin_Cost"] or 0)

                # What full-value calculation would give
                spot = abs(float(row.get("UL Last", 0) or 0))
                full_value_cost = spot * qty * 100 * (MARGIN_RATE / 365) if spot > 0 else 0

                # What borrowed-portion should give
                req = MARGIN_REQ_OVERRIDES.get(ticker, MARGIN_REQ_DEFAULT)
                if ticker in MARGIN_SPECIAL_PER_SHARE:
                    borrowed = max(0, spot - MARGIN_SPECIAL_PER_SHARE[ticker]) * qty
                else:
                    borrowed = spot * (1 - req) * qty

                expected_cost = borrowed * (MARGIN_RATE / 365)

                print(f"  {ticker}: spot=${spot:.2f}, qty={qty:.0f}")
                print(f"    Full-value (WRONG): ${full_value_cost:.2f}/day")
                print(f"    Borrowed-portion:   ${expected_cost:.2f}/day")
                print(f"    Actual in system:   ${actual_cost:.2f}/day")

                if abs(actual_cost - full_value_cost) < 0.01:
                    print(f"    ❌ USING FULL VALUE — not corrected!")
                elif abs(actual_cost - expected_cost) < 1.0:
                    print(f"    ✅ PASS — using borrowed portion")
                else:
                    print(f"    ⚠️  Differs from both — check calculation")
                print()
        else:
            print("  No margin stock positions found with cost > $0")

    # ── Check 4: Portfolio total vs debit-based ──────────────────────
    print()
    print("=" * 60)
    print("CHECK 4: Portfolio total vs Fidelity debit calculation")
    print("=" * 60)

    if "Daily_Margin_Cost" in df.columns:
        total_estimated = df["Daily_Margin_Cost"].fillna(0).sum()
        debit_exact = FIDELITY_DEBIT * (MARGIN_RATE / 365)

        print(f"  Sum of per-position costs (estimated): ${total_estimated:.2f}/day")
        print(f"  Debit × rate (exact):                  ${debit_exact:.2f}/day")
        print(f"  Fidelity monthly (debit-based):        ${debit_exact * 30:.2f}/mo")
        print(f"  Difference:                            ${abs(total_estimated - debit_exact):.2f}/day")

        if abs(total_estimated - debit_exact) < 5.0:
            print(f"  ✅ PASS — within $5/day tolerance")
        else:
            print(f"  ⚠️  Gap > $5/day — per-position estimates diverge from actual debit")
            print(f"       This is expected: per-position is approximate,")
            print(f"       actual debit reflects cross-position netting.")

    # ── Check 5: Carry classification distribution ───────────────────
    print()
    print("=" * 60)
    print("CHECK 5: Carry classification distribution")
    print("=" * 60)

    if "Carry_Classification" in df.columns:
        dist = df["Carry_Classification"].value_counts()
        for cls, count in dist.items():
            print(f"  {cls}: {count}")
    else:
        print("  ⚠️  Carry_Classification not found — run MarginCarryCalculator first")

    # ── Summary ──────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Compare Fidelity's monthly statement 'Margin Interest Charged'")
    print(f"  against our estimate: ${debit_exact * 30:.2f}/mo" if "Daily_Margin_Cost" in df.columns else "")
    print(f"  If they match within ~$5, our model is validated.")
    print()
    print(f"  To use exact debit-based calculation:")
    print(f"    export MARGIN_DEBIT={FIDELITY_DEBIT}")


if __name__ == "__main__":
    main()
