#!/bin/bash
# Run Step 9B: Fetch Option Contracts from Schwab

set -e

# Load environment variables from .env (only valid KEY=VALUE lines)
if [ -f .env ]; then
    while IFS= read -r line; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        # Export valid KEY=VALUE lines
        if [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
            export "$line"
        fi
    done < .env
fi

echo "🚀 Running Step 9B: Fetch Option Contracts..."
echo ""

# Run Step 9B
./venv/bin/python core/scan_engine/step9b_fetch_contracts_schwab.py "$@"
