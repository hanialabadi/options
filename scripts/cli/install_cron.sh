#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# install_cron.sh — Install/update scheduled scan crontab entries
#
# Idempotent: uses # OPTIONS_SCAN marker to replace existing entries.
# Safe: preserves all non-OPTIONS_SCAN crontab entries.
#
# Schedule (PST, Mon-Fri):
#   6:45 AM  = 9:45 AM ET  — post-open stabilization
#   7:30 AM  = 10:30 AM ET — morning high-volume window
#   8:30 AM  = 11:30 AM ET — pre-lunch
#   10:30 AM = 1:30 PM ET  — post-lunch
#   11:30 AM = 2:30 PM ET  — afternoon session
#   12:30 PM = 3:30 PM ET  — power hour
#
# Usage:
#   bash scripts/cli/install_cron.sh           # install/update
#   bash scripts/cli/install_cron.sh --remove  # remove all OPTIONS_SCAN entries
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_PATH="${PROJECT_ROOT}/scripts/cli/scheduled_scan.sh"
MARKER="# OPTIONS_SCAN"

if [[ ! -f "$SCRIPT_PATH" ]]; then
    echo "ERROR: scheduled_scan.sh not found at $SCRIPT_PATH"
    exit 1
fi

# ── Remove mode ─────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--remove" ]]; then
    echo "Removing all OPTIONS_SCAN crontab entries..."
    crontab -l 2>/dev/null | grep -v "$MARKER" | crontab -
    echo "Done. Current crontab:"
    crontab -l 2>/dev/null || echo "(empty)"
    exit 0
fi

# ── Build new cron entries ──────────────────────────────────────────────────
CRON_ENTRIES="45 6  * * 1-5  ${SCRIPT_PATH} >> /dev/null 2>&1 ${MARKER}
30 7  * * 1-5  ${SCRIPT_PATH} >> /dev/null 2>&1 ${MARKER}
30 8  * * 1-5  ${SCRIPT_PATH} >> /dev/null 2>&1 ${MARKER}
30 10 * * 1-5  ${SCRIPT_PATH} >> /dev/null 2>&1 ${MARKER}
30 11 * * 1-5  ${SCRIPT_PATH} >> /dev/null 2>&1 ${MARKER}
30 12 * * 1-5  ${SCRIPT_PATH} >> /dev/null 2>&1 ${MARKER}"

# ── Merge with existing crontab (remove old OPTIONS_SCAN, add new) ─────────
EXISTING=$(crontab -l 2>/dev/null | grep -v "$MARKER" || true)

{
    if [[ -n "$EXISTING" ]]; then
        echo "$EXISTING"
    fi
    echo "$CRON_ENTRIES"
} | crontab -

echo "Installed 6 scheduled scan entries (PST, Mon-Fri):"
echo ""
echo "  6:45 AM PST  =  9:45 AM ET  — post-open stabilization"
echo "  7:30 AM PST  = 10:30 AM ET  — morning high-volume window"
echo "  8:30 AM PST  = 11:30 AM ET  — pre-lunch"
echo " 10:30 AM PST  =  1:30 PM ET  — post-lunch"
echo " 11:30 AM PST  =  2:30 PM ET  — afternoon session"
echo " 12:30 PM PST  =  3:30 PM ET  — power hour"
echo ""
echo "Current crontab:"
crontab -l
echo ""
echo "To remove: bash $0 --remove"
echo "Logs: ${PROJECT_ROOT}/logs/scheduled_scan_YYYYMMDD.log"
