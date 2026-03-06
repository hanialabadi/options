import argparse
from datetime import datetime
import duckdb

from scan_engine.pipeline import (
    PipelineContext,
    _step_minus_1_reevaluate_wait_list,
    _step2_load_data,
    _step3_filter_tickers,
    _step_insert_technical_indicators,
    _step5_6_enrich_and_validate,
    _step7_recommend_strategies,
    _step9_select_contracts,
    _step10_recalibrate_pcs,
    _step11_evaluate_strategies,
    _step12_8_acceptance_and_sizing,
)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True, help="Snapshot path")
    ap.add_argument("--to", type=int, default=12)
    args = ap.parse_args()

    print("🚀 Initializing context...")
    ctx = PipelineContext(snapshot_path=args.path)
    con = duckdb.connect("data/pipeline.duckdb")

    run_ts = datetime.now()

    print("\n=== STEP -1 ===")
    _step_minus_1_reevaluate_wait_list(ctx, con)

    if args.to >= 2:
        print("\n=== STEP 2 ===")
        _step2_load_data(ctx, con)
        print("Rows after step2:", len(ctx.df))

    if args.to >= 3:
        print("\n=== STEP 3 ===")
        _step3_filter_tickers(ctx)
        print("Rows after step3:", len(ctx.df))

    if args.to >= 5:
        print("\n=== STEP 5/6 ===")
        _step_insert_technical_indicators(ctx, con)
        _step5_6_enrich_and_validate(ctx)
        print("Rows after step6:", len(ctx.df))

    if args.to >= 7:
        print("\n=== STEP 7 ===")
        _step7_recommend_strategies(ctx)
        print("Rows after step7:", len(ctx.df))

    if args.to >= 9:
        print("\n=== STEP 9 ===")
        _step9_select_contracts(ctx)
        print("Rows after step9:", len(ctx.df))

    if args.to >= 10:
        print("\n=== STEP 10 ===")
        _step10_recalibrate_pcs(ctx)

    if args.to >= 11:
        print("\n=== STEP 11 ===")
        _step11_evaluate_strategies(ctx)

    if args.to >= 12:
        print("\n=== STEP 12 ===")
        _step12_8_acceptance_and_sizing(ctx, run_ts, con)

    print("\n✅ DONE")
    print("Final rows:", len(ctx.df))

if __name__ == "__main__":
    main()
