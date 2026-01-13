#!/bin/bash
# Quick launcher for Options Intelligence Platform Dashboard

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Options Intelligence Platform - Launcher${NC}"
echo ""

# Check if in correct directory
if [ ! -f "streamlit_app/dashboard.py" ]; then
    echo -e "${RED}❌ Error: Must run from project root (options/)${NC}"
    exit 1
fi

# Check if venv exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}⚠️  Virtual environment not found. Creating...${NC}"
    python3 -m venv venv
fi

# Activate venv
echo -e "${GREEN}✅ Activating virtual environment...${NC}"
source venv/bin/activate

# Check if dependencies are installed
if ! python -c "import streamlit" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  Installing dependencies...${NC}"
    pip install -q streamlit pandas numpy yfinance python-dotenv
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  .env file not found. Using .env.template as reference.${NC}"
    echo -e "${YELLOW}   Create .env with your actual paths and tokens.${NC}"
    echo ""
fi

# Run the dashboard
echo -e "${GREEN}🎯 Starting Streamlit dashboard...${NC}"
echo -e "${GREEN}   Dashboard will open at: http://localhost:8501${NC}"
echo ""
streamlit run streamlit_app/dashboard.py
