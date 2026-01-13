#!/bin/bash
# Load environment variables and run Schwab reauth

cd /Users/haniabadi/Documents/Github/options

# Set Schwab credentials
export SCHWAB_APP_KEY=mwUGlWdyk424GNVCs3vdDGaLrpRVpZuLNsOYOfVwrhMyzs1X
export SCHWAB_APP_SECRET=UZD2InztGAAAz3Ul0Wv0voTh6CKbjtGqH2W8xxBxX7ncbYKKmtp2yOp5WlYeWALo
export SCHWAB_CALLBACK_URL=https://127.0.0.1

# Run the reauth script
./venv/bin/python tools/reauth_schwab.py
