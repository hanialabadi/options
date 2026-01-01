# Options Intelligence Platform - Quick Start Guide

## 🚀 Running the Dashboard

### Prerequisites
1. **Activate virtual environment:**
   ```bash
   source venv/bin/activate
   ```

2. **Install dependencies (if not already installed):**
   ```bash
   pip install streamlit pandas numpy yfinance python-dotenv
   ```

### Setup Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.template .env
```

Edit `.env` and fill in your values:
```
FIDELITY_SNAPSHOT_PATH=/path/to/your/fidelity_ivhv_snapshot.csv
OUTPUT_DIR=./output
TRADIER_TOKEN=your_api_token_here
```

### Run the Dashboard

```bash
streamlit run streamlit_app/dashboard.py
```

The app will open in your browser at `http://localhost:8501`

---

## 📊 Features

### Home View
- Choose between **Scan** (discover trades) or **Manage** (monitor positions)

### Scan View
- **Tab 1: IV/HV Analysis** - Filter tickers by implied vs historical volatility gap
- **Tab 2: Chart Signals** - EMA crossovers, ATR breakouts, regime classification
- **Tab 3: GEM Candidates** - Final filtered candidates with PCS scoring

Configure scan settings in the sidebar and click **"Run Full Scan Pipeline"**

### Manage View
- **Step 1: Load + Clean** - Import positions from Fidelity CSV
- **Step 2: Parse + Tag** - Parse option symbols and tag strategies

---

## 🔧 Pipeline Components

The scan pipeline (`core/scan_pipeline.py`) includes:

1. **Step 2:** Load IV/HV snapshot from Fidelity export
2. **Step 3:** Filter by IVHV gap (≥2.0) and add persona tags
3. **Step 5:** Compute chart signals (EMA, ATR, regime classification)
4. **Step 6:** Filter GEM candidates with Vega/skew enrichment

All steps include:
- ✅ Input validation with informative errors
- ✅ Environment variable support
- ✅ Structured logging
- ✅ Timestamped CSV exports

---

## 📁 Output Files

Results are saved to `OUTPUT_DIR` with timestamps:
- `Step3_Filtered_YYYYMMDD_HHMMSS.csv` - IV/HV filtered tickers
- `Step5_Charted_YYYYMMDD_HHMMSS.csv` - Chart signals added
- `Step6_GEM_YYYYMMDD_HHMMSS.csv` - Final GEM candidates

Download buttons available in each tab.

---

## 🐛 Troubleshooting

**Import errors:**
```bash
# Make sure you're in the project root and venv is active
cd /Users/haniabadi/Documents/Github/options
source venv/bin/activate
python -c "from core.scan_pipeline import run_full_scan_pipeline; print('✅ OK')"
```

**Module 'core' not found:**
- The dashboard automatically adds the parent directory to Python path
- If running standalone scripts, add: `sys.path.insert(0, str(Path(__file__).parent.parent))`

**yfinance rate limiting:**
- Built-in 0.5s delay every 10 tickers
- For large scans (100+ tickers), consider adding caching (TODO item #3)

---

## 🎯 Next Steps (TODO)

High priority improvements tracked in the TODO list:
- [ ] Add yfinance caching for faster repeat scans
- [ ] Add progress bars (`tqdm`) for long operations
- [ ] Create Step 15 audit report
- [ ] Normalize ticker column names across pipeline

See the TODO list in VS Code or run `manage_todo_list` for details.

---

## 📞 Support

For questions or issues:
1. Check logs in terminal output
2. Review error messages in Streamlit UI
3. Verify `.env` configuration
4. Ensure IV/HV snapshot file exists at configured path
