"""
Sector Benchmark Map
=====================
Maps each tracked ticker to its most appropriate sector benchmark ETF.

Used by compute_sector_relative_strength() in thesis_engine.py to compute
a z-score normalized relative performance signal — per Natenberg Ch.8
(volatility-normalized comparison) and McMillan Ch.1 (relative strength context).

Design principle:
  • One benchmark per ticker — the ETF that best represents the sector
    the stock competes in, NOT just the broad-market ETF.
  • "_default" key: fallback for any ticker not explicitly mapped.
  • All ETFs are liquid, free-data accessible (yfinance, no paid feed).

To add a new ticker: add one line.  No logic in engine code.
"""

SECTOR_BENCHMARK_MAP: dict[str, str] = {
    # ── Technology ────────────────────────────────────────────────────────────
    "AAPL":  "QQQ",
    "MSFT":  "QQQ",
    "NVDA":  "QQQ",
    "AMD":   "QQQ",
    "GOOG":  "QQQ",
    "GOOGL": "QQQ",
    "META":  "QQQ",
    "AMZN":  "QQQ",
    "TSLA":  "QQQ",
    "NFLX":  "QQQ",
    "ADBE":  "QQQ",
    "CRM":   "QQQ",
    "ORCL":  "QQQ",
    "INTC":  "QQQ",
    "QCOM":  "QQQ",
    "AVGO":  "QQQ",
    "TXN":   "QQQ",
    "AMAT":  "QQQ",
    "MU":    "QQQ",
    "KLAC":  "QQQ",
    "LRCX":  "QQQ",
    "ASML":  "QQQ",
    "ARM":   "QQQ",
    "PLTR":  "QQQ",
    "SNOW":  "QQQ",
    "NET":   "QQQ",
    "DDOG":  "QQQ",
    "ZS":    "QQQ",
    "CRWD":  "QQQ",
    "SMCI":  "QQQ",
    "DELL":  "QQQ",
    "HPE":   "QQQ",
    "PANW":  "QQQ",

    # ── Crypto / Digital Assets ───────────────────────────────────────────────
    "COIN":  "BITO",   # iShares Bitcoin Trust proxy
    "MSTR":  "BITO",
    "RIOT":  "BITO",
    "MARA":  "BITO",
    "CLSK":  "BITO",
    "BTBT":  "BITO",

    # ── Clean Energy / Power ──────────────────────────────────────────────────
    "EOSE":  "ICLN",   # iShares Global Clean Energy
    "FLNC":  "ICLN",
    "PLUG":  "ICLN",
    "FCEL":  "ICLN",
    "BLDP":  "ICLN",
    "BE":    "ICLN",
    "NEE":   "ICLN",
    "ENPH":  "ICLN",
    "SEDG":  "ICLN",
    "RUN":   "ICLN",
    "ARRY":  "ICLN",

    # ── Financials ────────────────────────────────────────────────────────────
    "JPM":   "XLF",
    "BAC":   "XLF",
    "WFC":   "XLF",
    "GS":    "XLF",
    "MS":    "XLF",
    "C":     "XLF",
    "BLK":   "XLF",
    "SCHW":  "XLF",
    "V":     "XLF",
    "MA":    "XLF",
    "AXP":   "XLF",
    "PYPL":  "XLF",
    "SQ":    "XLF",

    # ── Healthcare / Biotech ──────────────────────────────────────────────────
    "JNJ":   "XLV",
    "UNH":   "XLV",
    "PFE":   "XLV",
    "MRK":   "XLV",
    "ABBV":  "XLV",
    "LLY":   "XLV",
    "BMY":   "XLV",
    "AMGN":  "XLV",
    "GILD":  "XLV",
    "MRNA":  "XLV",
    "BNTX":  "XLV",
    "BIIB":  "XLV",
    "REGN":  "XLV",
    "VRTX":  "XLV",

    # ── Consumer Discretionary ────────────────────────────────────────────────
    "DKNG":  "XLY",
    "BKNG":  "XLY",
    "ABNB":  "XLY",
    "LVS":   "XLY",
    "MGM":   "XLY",
    "WYNN":  "XLY",
    "NKE":   "XLY",
    "SBUX":  "XLY",
    "MCD":   "XLY",
    "CMG":   "XLY",
    "HD":    "XLY",
    "LOW":   "XLY",
    "TGT":   "XLY",
    "WMT":   "XLP",  # Consumer Staples
    "COST":  "XLP",
    "PG":    "XLP",
    "KO":    "XLP",
    "PEP":   "XLP",

    # ── Energy ───────────────────────────────────────────────────────────────
    "XOM":   "XLE",
    "CVX":   "XLE",
    "COP":   "XLE",
    "SLB":   "XLE",
    "EOG":   "XLE",
    "OXY":   "XLE",
    "DVN":   "XLE",

    # ── Industrials ──────────────────────────────────────────────────────────
    "CAT":   "XLI",
    "DE":    "XLI",
    "GE":    "XLI",
    "BA":    "XLI",
    "RTX":   "XLI",
    "LMT":   "XLI",
    "NOC":   "XLI",
    "UPS":   "XLI",
    "FDX":   "XLI",

    # ── Metals & Mining ──────────────────────────────────────────────────────
    "GLD":   "GDX",
    "SLV":   "GDX",
    "UUUU":  "GDX",

    # ── Broad Market fallback ─────────────────────────────────────────────────
    "_default": "SPY",
}


# ── Sector Bucket Map ──────────────────────────────────────────────────────
# Maps benchmark ETF code → human-readable sector name.
# Used by get_sector_bucket() for portfolio concentration analysis.

SECTOR_BUCKET_MAP: dict[str, str] = {
    "QQQ":  "Technology",
    "XLF":  "Financials",
    "XLV":  "Healthcare",
    "XLE":  "Energy",
    "XLI":  "Industrials",
    "XLY":  "Consumer Disc.",
    "XLP":  "Consumer Staples",
    "XLU":  "Utilities",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLC":  "Communication",
    "GDX":  "Metals & Mining",
    "BITO": "Crypto",
    "ICLN": "Clean Energy",
    "SPY":  "Broad Market",
}


def get_sector_bucket(ticker: str) -> str:
    """Return the human-readable sector bucket for a ticker.

    Lookup chain: ticker → SECTOR_BENCHMARK_MAP → ETF → SECTOR_BUCKET_MAP → bucket name.
    Fallback: 'Broad Market' for any unmapped ticker."""
    etf = SECTOR_BENCHMARK_MAP.get(ticker, SECTOR_BENCHMARK_MAP.get("_default", "SPY"))
    return SECTOR_BUCKET_MAP.get(etf, "Broad Market")


# ── ETF Detection ─────────────────────────────────────────────────────────
# ETFs that are both: (a) traded in our universe (tickers.csv) AND
# (b) a recognized benchmark ETF.  Used to skip earnings gates, add
# macro-vol context to CC decisions, and flag positions in the dashboard.

KNOWN_ETFS: frozenset = frozenset({
    "SPY", "QQQ",
    "XLE", "XLF", "XLI", "XLK", "XLU", "XLP", "XLRE", "XLB",
    "GLD", "SLV", "GDX",
})

# Commodity/macro ETFs get extra HV mean-reversion context
_COMMODITY_ETFS: frozenset = frozenset({"GLD", "SLV", "GDX"})


def is_etf(ticker: str) -> bool:
    """True if ticker is a known ETF in our universe."""
    return str(ticker or "").upper().strip() in KNOWN_ETFS


def is_commodity_etf(ticker: str) -> bool:
    """True if ticker is a commodity/metals ETF (stronger HV mean-reversion signal)."""
    return str(ticker or "").upper().strip() in _COMMODITY_ETFS
