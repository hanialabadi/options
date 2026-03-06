# revalidate.py
# Full Trade Revalidation Pipeline — triggered by Action == "REVALIDATE" flags.
# Replaces the old Tradier-based revalidation. Uses Schwab + DriftEngine + DoctrineAuthority.

import pandas as pd
import logging
from datetime import datetime

from core.shared.data_contracts.master_data import load_active_master, save_active_master
from core.management.cycle2.drift.auto_enrich_hv import auto_enrich_hv_from_schwab
from core.management.cycle2.providers.governed_iv_provider import fetch_governed_sensor_readings
from core.management.cycle2.chart_state.chart_state_engine import compute_chart_state
from core._engines.management_engine.pcs_live import score_pcs_batch
from core._engines.management_engine.recommend import run_v6_overlay

logger = logging.getLogger(__name__)


def market_is_open() -> bool:
    """Check if US equity market hours (09:30–16:00 ET, Mon–Fri)."""
    now = datetime.now()
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    t = now.time()
    return datetime.strptime("09:30", "%H:%M").time() <= t <= datetime.strptime("16:00", "%H:%M").time()


def run_full_revalidation_pipeline(force: bool = False) -> pd.DataFrame:
    """
    Revalidate trades flagged with Action == 'REVALIDATE'.

    Steps:
      1. Load active master
      2. Filter REVALIDATE-flagged rows
      3. Update HV via Schwab (or yfinance fallback)
      4. Update live IV + Greeks via governed IV provider
      5. Update chart states
      6. Re-score PCS
      7. Re-run doctrine overlay
      8. Save updated master

    Args:
        force: If True, revalidate all trades regardless of market hours.

    Returns:
        Updated DataFrame after revalidation, or empty DataFrame if skipped.
    """
    if not force and not market_is_open():
        logger.info("Market is closed — skipping revalidation. Use force=True to override.")
        return pd.DataFrame()

    logger.info("Starting revalidation for flagged trades...")
    df = load_active_master()

    if df.empty:
        logger.warning("active_master is empty — nothing to revalidate.")
        return df

    # Filter to REVALIDATE-flagged rows only
    revalidate_mask = df.get('Action', pd.Series(dtype=str)).astype(str).str.startswith('REVALIDATE')
    df_flagged = df[revalidate_mask]

    if df_flagged.empty:
        logger.info("No trades flagged for revalidation.")
        return df

    logger.info(f"{len(df_flagged)} trade(s) flagged for revalidation.")

    # Attempt live Schwab connection
    schwab_live = False
    try:
        from dotenv import load_dotenv
        load_dotenv()
        from scan_engine.loaders.schwab_api_client import SchwabClient
        client = SchwabClient()
        client.ensure_valid_token()
        schwab_live = True
        logger.info("Schwab live connection established for revalidation.")
    except Exception as e:
        logger.warning(f"Schwab unavailable — falling back to yfinance/cache: {e}")

    # Step 3: Update HV
    df = auto_enrich_hv_from_schwab(df, schwab_live=schwab_live)

    # Step 4: Update live IV + Greeks for option legs
    option_mask = df.get('AssetType', pd.Series(dtype=str)) == 'OPTION'
    option_symbols = df.loc[option_mask, 'Symbol'].unique().tolist()
    if option_symbols:
        try:
            readings = fetch_governed_sensor_readings(option_symbols, schwab_live=schwab_live)
            iv_map = {r['Symbol']: r['IV'] for r in readings if r.get('IV') is not None}
            df.loc[option_mask, 'IV_30D'] = df.loc[option_mask, 'Symbol'].map(iv_map).fillna(
                df.loc[option_mask, 'IV_30D']
            )
            # Update Greeks where fresh Schwab data available
            for r in readings:
                if not r.get('Is_Fallback') and r.get('Source') == 'schwab':
                    mask = df['Symbol'] == r['Symbol']
                    for greek in ['Delta', 'Gamma', 'Vega', 'Theta', 'Rho']:
                        if r.get(greek) is not None:
                            df.loc[mask, greek] = r[greek]
        except Exception as e:
            logger.warning(f"IV/Greek update failed during revalidation: {e}")

    # Step 5: Update chart states
    try:
        schwab_client = None
        if schwab_live:
            from scan_engine.loaders.schwab_api_client import SchwabClient
            schwab_client = SchwabClient()
        df = compute_chart_state(df, client=schwab_client)
    except Exception as e:
        logger.warning(f"Chart state update failed during revalidation: {e}")

    # Step 6: Re-score PCS inline (batch save handled separately)
    from core._engines.management_engine.pcs_live import pcs_engine_v3_2_strategy_aware
    df = pcs_engine_v3_2_strategy_aware(df)

    # Step 7: Re-run doctrine overlay (DriftEngine + DoctrineAuthority)
    df = run_v6_overlay(df)

    # Step 8: Persist
    save_active_master(df)
    logger.info("Revalidation complete — active_master updated.")

    return df


if __name__ == "__main__":
    run_full_revalidation_pipeline(force=True)
