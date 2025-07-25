#!/bin/bash

echo "🛑 Killing existing Streamlit processes..."
pkill -f "streamlit run" 2>/dev/null

echo "🧹 Clearing Streamlit cache..."
streamlit cache clear

echo "🚀 Launching Streamlit app (without browser)..."
streamlit run streamlit_app/dashboard.py --server.headless true > .streamlit_run.log 2>&1 &

# Wait for server to start
sleep 3

echo "🌐 Opening ONLY Chrome incognito..."
open -na "Google Chrome" --args --incognito http://localhost:8501
