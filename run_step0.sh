#!/bin/bash
# Run Step 0 to generate fresh snapshot with full ticker universe

cd /Users/haniabadi/Documents/Github/options

# Set Schwab credentials
export SCHWAB_APP_KEY=mwUGlWdyk424GNVCs3vdDGaLrpRVpZuLNsOYOfVwrhMyzs1X
export SCHWAB_APP_SECRET=UZD2InztGAAAz3Ul0Wv0voTh6CKbjtGqH2W8xxBxX7ncbYKKmtp2yOp5WlYeWALo
export SCHWAB_CALLBACK_URL=https://127.0.0.1

# Run Step 0 with full ticker universe
./venv/bin/python -c "from core.scan_engine.step0_schwab_snapshot import main; main(test_mode=False, use_cache=True, fetch_iv=True)"
