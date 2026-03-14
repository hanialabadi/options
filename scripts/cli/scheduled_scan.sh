#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# scheduled_scan.sh — Cron-compatible pipeline runner
#
# Safety mechanisms:
#   1. Lockfile (flock) — prevents overlapping runs
#   2. Day-of-week guard — skips Sat/Sun
#   3. Market-hours guard — only runs 6:30 AM - 1:00 PM PST
#
# Usage:
#   bash scripts/cli/scheduled_scan.sh          # normal run
#   bash scripts/cli/scheduled_scan.sh --force  # skip market-hours check
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Configuration ───────────────────────────────────────────────────────────
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOCKFILE="/tmp/options_scan.lock"
LOG_DIR="${PROJECT_ROOT}/logs"
LOG_FILE="${LOG_DIR}/scheduled_scan_$(date +%Y%m%d).log"
VENV_DIR="${PROJECT_ROOT}/venv"
FORCE_RUN=false

if [[ "${1:-}" == "--force" ]]; then
    FORCE_RUN=true
fi

# ── Logging helper ──────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"

log() {
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "[$ts] $*" | tee -a "$LOG_FILE"
}

# ── Lockfile guard (flock) ──────────────────────────────────────────────────
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    log "SKIP: Another scan is already running (lockfile held: $LOCKFILE)"
    exit 0
fi
# Lock acquired — auto-released when script exits (fd 200 closes)

# ── Day-of-week guard ──────────────────────────────────────────────────────
DOW=$(date +%u)  # 1=Mon ... 7=Sun
if [[ "$DOW" -ge 6 ]] && [[ "$FORCE_RUN" == "false" ]]; then
    log "SKIP: Weekend (day=$DOW). Use --force to override."
    exit 0
fi

# ── Market-hours guard (PST) ───────────────────────────────────────────────
# Market hours: 6:30 AM - 1:00 PM PST (= 9:30 AM - 4:00 PM ET)
# Use TZ=America/Los_Angeles for PST/PDT-aware time
CURRENT_HOUR=$(TZ=America/Los_Angeles date +%H)
CURRENT_MIN=$(TZ=America/Los_Angeles date +%M)
CURRENT_TIME_MIN=$(( CURRENT_HOUR * 60 + CURRENT_MIN ))

MARKET_OPEN_MIN=$(( 6 * 60 + 30 ))   # 6:30 AM PST
MARKET_CLOSE_MIN=$(( 13 * 60 ))      # 1:00 PM PST

if [[ "$CURRENT_TIME_MIN" -lt "$MARKET_OPEN_MIN" ]] || [[ "$CURRENT_TIME_MIN" -gt "$MARKET_CLOSE_MIN" ]]; then
    if [[ "$FORCE_RUN" == "false" ]]; then
        log "SKIP: Outside market hours (PST time: ${CURRENT_HOUR}:${CURRENT_MIN}). Use --force to override."
        exit 0
    fi
fi

# ── Activate venv ──────────────────────────────────────────────────────────
if [[ -f "${VENV_DIR}/bin/activate" ]]; then
    source "${VENV_DIR}/bin/activate"
else
    log "ERROR: venv not found at ${VENV_DIR}/bin/activate"
    exit 1
fi

cd "$PROJECT_ROOT"

# ── Step 1: Fetch fresh snapshot ───────────────────────────────────────────
log "START: Fetching fresh snapshot..."
SNAPSHOT_START=$(date +%s)

python -c "
from scan_engine.step0_schwab_snapshot import run_snapshot
df = run_snapshot(test_mode=False, use_cache=True, fetch_iv=True, discovery_mode=False)
print(f'SNAPSHOT_OK: {len(df)} tickers')
" >> "$LOG_FILE" 2>&1

SNAPSHOT_END=$(date +%s)
SNAPSHOT_ELAPSED=$(( SNAPSHOT_END - SNAPSHOT_START ))
log "Snapshot complete (${SNAPSHOT_ELAPSED}s)"

# ── Step 2: Run full pipeline ──────────────────────────────────────────────
log "START: Running full pipeline..."
PIPELINE_START=$(date +%s)

python scripts/cli/run_pipeline_cli.py --full >> "$LOG_FILE" 2>&1

PIPELINE_END=$(date +%s)
PIPELINE_ELAPSED=$(( PIPELINE_END - PIPELINE_START ))
log "Pipeline complete (${PIPELINE_ELAPSED}s)"

# ── Step 3: Summary ────────────────────────────────────────────────────────
# Parse the latest Step12 output for counts
LATEST_STEP12=$(ls -t output/Step12_Acceptance_*.csv 2>/dev/null | head -1)

if [[ -n "$LATEST_STEP12" ]]; then
    SUMMARY=$(python -c "
import pandas as pd
df = pd.read_csv('$LATEST_STEP12')
total = len(df)
ready = (df['Execution_Status'] == 'READY').sum() if 'Execution_Status' in df.columns else 0
await_c = (df['Execution_Status'] == 'AWAIT_CONFIRMATION').sum() if 'Execution_Status' in df.columns else 0
blocked = (df['Execution_Status'] == 'BLOCKED').sum() if 'Execution_Status' in df.columns else 0
print(f'READY={ready} | AWAIT={await_c} | BLOCKED={blocked} | Total={total}')
" 2>/dev/null || echo "SUMMARY_ERROR")
    log "DONE: $SUMMARY (snapshot=${SNAPSHOT_ELAPSED}s, pipeline=${PIPELINE_ELAPSED}s)"
else
    log "DONE: No Step12 output found (pipeline may have failed)"
fi

TOTAL_ELAPSED=$(( PIPELINE_END - SNAPSHOT_START ))
log "Total elapsed: ${TOTAL_ELAPSED}s"
log "───────────────────────────────────────────────"
