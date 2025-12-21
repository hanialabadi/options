def phase6_freeze_pipeline(df_flat, df_master, mode="flat", archive=False):
    """
    Runs full Phase 6 logic on new trades:
    - Detects new
    - Runs chart + PCS engines
    - Freezes _Entry fields
    - Optionally saves to active_master.csv
    """

    from core.phase6_freeze.detect_new_trades import detect_new_trades
    from core.phase4_snapshot.chart_verdict_engine import run_chart_verdict_on_new
    from core.phase3_enrich.run_pcs_on_new import run_pcs_on_new
    from core.phase6_freeze.freeze_all_entry_fields import freeze_all_entry_fields
    from core.phase6_freeze.freeze_archive_export import save_master

    df_flat["IsNewTrade"] = detect_new_trades(df_flat, df_master)

    df_flat = run_chart_verdict_on_new(df_flat)
    df_flat = run_pcs_on_new(df_flat)
    df_flat = freeze_all_entry_fields(df_flat, mode=mode)

    if archive:
        save_master(df_flat)

    return df_flat
