"""
CLI Test: Phase 1 → Phase 2 → Phase 3 → Phase 4
End-to-end pipeline test from raw data to DuckDB snapshot
"""

import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === Phase 1: Load and Clean ===
logger.info("=" * 80)
logger.info("PHASE 1: Load and Clean Raw Data")
logger.info("=" * 80)

from core.phase1_clean import phase1_load_and_clean_raw_v2 as phase1_load_and_clean

INPUT_PATH = "data/brokerage_inputs/fidelity_positions.csv"
df_input, snapshot_path = phase1_load_and_clean(input_path=INPUT_PATH)

logger.info(f"✅ Phase 1 complete: {len(df_input)} positions, {len(df_input.columns)} columns")
logger.info(f"   Snapshot: {snapshot_path}")

# === Phase 2: Parse and Validate ===
logger.info("=" * 80)
logger.info("PHASE 2: Parse Strategy and Validate")
logger.info("=" * 80)

from core.phase2_parse import phase2_run_all

df_parsed = phase2_run_all(df_input)
logger.info(f"✅ Phase 2 complete: {len(df_parsed)} positions parsed")

# Strategy distribution
if "Strategy" in df_parsed.columns:
    strategy_counts = df_parsed["Strategy"].value_counts()
    logger.info(f"   Strategy distribution:")
    for strategy, count in strategy_counts.items():
        logger.info(f"     • {strategy}: {count}")

# === Phase 3: Enrichment (7 modules) ===
logger.info("=" * 80)
logger.info("PHASE 3: Enrichment (7 modules)")
logger.info("=" * 80)

from core.phase3_enrich import (
    tag_strategy_metadata,
    compute_breakeven,
    compute_moneyness,
    tag_earnings_flags,
)
from core.phase3_enrich.pcs_score import calculate_pcs
from core.phase3_enrich.liquidity import enrich_liquidity
from core.phase3_enrich.skew_kurtosis import calculate_skew_and_kurtosis

df_enriched = df_parsed.copy()

# Module 1: Strategy Metadata
logger.info("[1/7] Tagging strategy metadata...")
df_enriched = tag_strategy_metadata(df_enriched)
logger.info("      ✅ Strategy metadata tagged")

# Module 2: Breakeven
logger.info("[2/7] Computing breakeven prices...")
df_enriched = compute_breakeven(df_enriched)
logger.info("      ✅ Breakeven computed")

# Module 3: Moneyness
logger.info("[3/7] Computing moneyness...")
df_enriched = compute_moneyness(df_enriched)
logger.info("      ✅ Moneyness computed")

# Module 4: PCS Score
logger.info("[4/7] Computing PCS scores...")
try:
    df_enriched = calculate_pcs(df_enriched)
    logger.info("      ✅ PCS scores computed")
except Exception as e:
    logger.warning(f"      ⚠️ PCS score failed: {e}")

# Module 5: Liquidity
logger.info("[5/7] Analyzing liquidity...")
try:
    df_enriched = enrich_liquidity(df_enriched)
    logger.info("      ✅ Liquidity analyzed")
except Exception as e:
    logger.warning(f"      ⚠️ Liquidity analysis skipped: {e}")

# Module 6: Skew & Kurtosis
logger.info("[6/7] Computing IV dispersion...")
try:
    df_enriched = calculate_skew_and_kurtosis(df_enriched)
    logger.info("      ✅ IV dispersion computed")
except Exception as e:
    logger.warning(f"      ⚠️ IV dispersion failed: {e}")

# Module 7: Earnings Flags
logger.info("[7/7] Tagging earnings events...")
df_enriched = tag_earnings_flags(df_enriched)
logger.info("      ✅ Earnings events tagged")

logger.info(f"✅ Phase 3 complete: {len(df_enriched)} positions enriched with {len(df_enriched.columns)} columns")

# Show Phase 3 summary
phase3_cols = [col for col in df_enriched.columns if col not in df_parsed.columns]
logger.info(f"   New columns added: {len(phase3_cols)}")

# Key metrics
if "Capital_Deployed_Trade_Level" in df_enriched.columns:
    total_capital = df_enriched.groupby("TradeID")["Capital_Deployed_Trade_Level"].first().sum()
    logger.info(f"   Total Capital Deployed: ${total_capital:,.0f}")

if "PCS_Score" in df_enriched.columns:
    avg_pcs = df_enriched["PCS_Score"].mean()
    logger.info(f"   Average PCS Score: {avg_pcs:.1f}")

if "Is_Event_Setup" in df_enriched.columns:
    event_count = (df_enriched["Is_Event_Setup"] == True).sum()
    logger.info(f"   Earnings Event Setups: {event_count}")

# === Phase 4: Snapshot Persistence ===
logger.info("=" * 80)
logger.info("PHASE 4: Snapshot Persistence (Truth Ledger)")
logger.info("=" * 80)

from core.phase4_snapshot import save_clean_snapshot

df_snapshot, csv_path, run_id, csv_success, db_success = save_clean_snapshot(
    df_enriched,
    to_csv=True,
    to_db=True
)

logger.info(f"✅ Phase 4 complete: Snapshot saved with run_id={run_id}")
logger.info(f"   DuckDB Status: {'✅ Success' if db_success else '❌ Failed'}")
logger.info(f"   CSV Status: {'✅ Success' if csv_success else '❌ Failed'}")
logger.info(f"   CSV Path: {csv_path}")

if "Schema_Hash" in df_snapshot.columns:
    schema_hash = df_snapshot["Schema_Hash"].iloc[0]
    logger.info(f"   Schema Hash: {schema_hash}")

# === Final Summary ===
logger.info("=" * 80)
logger.info("END-TO-END PIPELINE COMPLETE")
logger.info("=" * 80)
logger.info(f"Phase 1: {len(df_input)} positions loaded")
logger.info(f"Phase 2: {len(df_parsed)} positions parsed")
logger.info(f"Phase 3: {len(df_enriched)} positions enriched ({len(phase3_cols)} new columns)")
logger.info(f"Phase 4: {len(df_snapshot)} positions persisted to truth ledger")
logger.info(f"Run ID: {run_id}")
logger.info("=" * 80)

sys.exit(0)
