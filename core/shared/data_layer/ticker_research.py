"""
Ticker Research — Structured Fundamental Context
=================================================
Two tables in pipeline.duckdb:

1. ``ticker_profiles`` — semi-static thesis context (moat, risks, growth drivers).
   Updated quarterly or when thesis changes. One active row per ticker.

2. ``ticker_events`` — time-bound developments (competitor moves, regulatory,
   guidance changes, analyst actions). Each event has an impact horizon and bias.

The management engine reads both at decision time so doctrine knows *why* you
own something and what has changed since entry.

Data is inserted conversationally (Claude structures and writes) — no API
scraping, no automation. Your research becomes persistent and machine-readable.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Optional, List

import pandas as pd

logger = logging.getLogger(__name__)

_PROFILE_TABLE = "ticker_profiles"
_EVENT_TABLE = "ticker_events"

# Valid enum values
PROFILE_MOAT_TYPES = ("WIDE", "NARROW", "NONE", "UNKNOWN")
EVENT_TYPES = ("COMPETITIVE", "REGULATORY", "PRODUCT", "GUIDANCE", "ANALYST",
               "SECTOR", "MACRO", "EARNINGS", "MANAGEMENT", "OTHER")
IMPACT_BIAS = ("BULLISH", "BEARISH", "NEUTRAL", "UNKNOWN")
IMPACT_HORIZON = ("SHORT", "MEDIUM", "LONG")  # weeks / quarters / years
CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def initialize_ticker_research_tables(con) -> None:
    """Create both tables if they don't exist."""
    # Sequence for event auto-increment (must exist before table referencing it)
    try:
        con.execute("CREATE SEQUENCE IF NOT EXISTS ticker_event_seq START 1")
    except Exception:
        pass

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_PROFILE_TABLE} (
            ticker              VARCHAR NOT NULL,
            -- Thesis
            thesis_summary      VARCHAR,
            ownership_rationale VARCHAR,
            moat_type           VARCHAR DEFAULT 'UNKNOWN',
            moat_description    VARCHAR,
            -- Growth / Financials
            growth_drivers      VARCHAR,
            revenue_segments    VARCHAR,
            competitive_position VARCHAR,
            -- Risks
            key_risks           VARCHAR,
            bear_case           VARCHAR,
            bull_case           VARCHAR,
            -- Sector
            sector              VARCHAR,
            industry            VARCHAR,
            key_competitors     VARCHAR,
            -- Meta
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by          VARCHAR DEFAULT 'claude',
            is_active           BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (ticker)
        )
    """)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {_EVENT_TABLE} (
            id                  INTEGER DEFAULT (nextval('ticker_event_seq')),
            ticker              VARCHAR NOT NULL,
            event_date          DATE NOT NULL,
            event_type          VARCHAR NOT NULL,
            description         VARCHAR NOT NULL,
            impact_bias         VARCHAR DEFAULT 'UNKNOWN',
            impact_horizon      VARCHAR DEFAULT 'MEDIUM',
            confidence          VARCHAR DEFAULT 'MEDIUM',
            source              VARCHAR,
            is_resolved         BOOLEAN DEFAULT FALSE,
            resolved_date       DATE,
            resolved_note       VARCHAR,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by          VARCHAR DEFAULT 'claude'
        )
    """)



# ---------------------------------------------------------------------------
# Profiles — CRUD
# ---------------------------------------------------------------------------

def upsert_ticker_profile(con, ticker: str, **kwargs) -> None:
    """Insert or update a ticker profile. Only non-None kwargs are written."""
    initialize_ticker_research_tables(con)

    # Check if exists
    existing = con.execute(
        f"SELECT ticker FROM {_PROFILE_TABLE} WHERE ticker = ?", [ticker]
    ).fetchone()

    allowed = {
        "thesis_summary", "ownership_rationale", "moat_type", "moat_description",
        "growth_drivers", "revenue_segments", "competitive_position",
        "key_risks", "bear_case", "bull_case",
        "sector", "industry", "key_competitors",
        "updated_by", "is_active",
    }
    fields = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    fields["updated_at"] = datetime.now()

    if existing:
        if not fields:
            return
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [ticker]
        con.execute(
            f"UPDATE {_PROFILE_TABLE} SET {set_clause} WHERE ticker = ?",
            values,
        )
        logger.info("[TickerResearch] Updated profile for %s (%d fields)", ticker, len(fields))
    else:
        fields["ticker"] = ticker
        cols = ", ".join(fields.keys())
        placeholders = ", ".join("?" for _ in fields)
        con.execute(
            f"INSERT INTO {_PROFILE_TABLE} ({cols}) VALUES ({placeholders})",
            list(fields.values()),
        )
        logger.info("[TickerResearch] Created profile for %s", ticker)


def _table_exists(con, table_name: str) -> bool:
    """Check if a table exists without requiring write access."""
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 0")
        return True
    except Exception:
        return False


def get_ticker_profile(con, ticker: str) -> Optional[dict]:
    """Return the active profile for a ticker, or None."""
    if not _table_exists(con, _PROFILE_TABLE):
        return None
    df = con.execute(
        f"SELECT * FROM {_PROFILE_TABLE} WHERE ticker = ? AND is_active = TRUE",
        [ticker],
    ).fetchdf()
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    return {k: (None if isinstance(v, float) and pd.isna(v) else v)
            for k, v in row.items()}


def get_all_profiles(con) -> pd.DataFrame:
    """Return all active ticker profiles."""
    if not _table_exists(con, _PROFILE_TABLE):
        return pd.DataFrame()
    return con.execute(
        f"SELECT * FROM {_PROFILE_TABLE} WHERE is_active = TRUE ORDER BY ticker"
    ).fetchdf()


# ---------------------------------------------------------------------------
# Events — CRUD
# ---------------------------------------------------------------------------

def add_ticker_event(
    con,
    ticker: str,
    event_date: date,
    event_type: str,
    description: str,
    impact_bias: str = "UNKNOWN",
    impact_horizon: str = "MEDIUM",
    confidence: str = "MEDIUM",
    source: Optional[str] = None,
) -> None:
    """Insert a new ticker event."""
    initialize_ticker_research_tables(con)

    con.execute(f"""
        INSERT INTO {_EVENT_TABLE}
            (ticker, event_date, event_type, description,
             impact_bias, impact_horizon, confidence, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [ticker, event_date, event_type, description,
          impact_bias, impact_horizon, confidence, source])
    logger.info("[TickerResearch] Added event for %s: %s (%s)", ticker, event_type, impact_bias)


def get_active_events(
    con, ticker: str, horizon: Optional[str] = None
) -> List[dict]:
    """Return unresolved events for a ticker, optionally filtered by horizon."""
    if not _table_exists(con, _EVENT_TABLE):
        return []

    query = f"""
        SELECT * FROM {_EVENT_TABLE}
        WHERE ticker = ? AND is_resolved = FALSE
    """
    params = [ticker]
    if horizon:
        query += " AND impact_horizon = ?"
        params.append(horizon)
    query += " ORDER BY event_date DESC"

    df = con.execute(query, params).fetchdf()
    if df.empty:
        return []
    return [
        {k: (None if isinstance(v, float) and pd.isna(v) else v)
         for k, v in row.items()}
        for _, row in df.iterrows()
    ]


def get_all_active_events(con, tickers: Optional[List[str]] = None) -> pd.DataFrame:
    """Return all unresolved events, optionally filtered to a ticker list."""
    if not _table_exists(con, _EVENT_TABLE):
        return pd.DataFrame()

    if tickers:
        placeholders = ", ".join("?" for _ in tickers)
        return con.execute(f"""
            SELECT * FROM {_EVENT_TABLE}
            WHERE is_resolved = FALSE AND ticker IN ({placeholders})
            ORDER BY ticker, event_date DESC
        """, tickers).fetchdf()
    return con.execute(f"""
        SELECT * FROM {_EVENT_TABLE}
        WHERE is_resolved = FALSE
        ORDER BY ticker, event_date DESC
    """).fetchdf()


def resolve_event(con, event_id: int, note: Optional[str] = None) -> None:
    """Mark an event as resolved."""
    con.execute(f"""
        UPDATE {_EVENT_TABLE}
        SET is_resolved = TRUE, resolved_date = CURRENT_DATE, resolved_note = ?
        WHERE id = ?
    """, [note, event_id])
    logger.info("[TickerResearch] Resolved event #%d", event_id)


# ---------------------------------------------------------------------------
# Engine-facing queries (called by doctrine at decision time)
# ---------------------------------------------------------------------------

def get_research_context(con, ticker: str) -> dict:
    """
    Return combined profile + active events for a ticker.
    This is what doctrine reads at decision time.

    Returns:
        {
            "profile": dict | None,
            "events": [dict, ...],
            "event_summary": str,       # one-liner for card display
            "thesis_risks": [str, ...], # BEARISH events with HIGH/MEDIUM confidence
            "thesis_catalysts": [str, ...],  # BULLISH events
        }
    """
    profile = get_ticker_profile(con, ticker)
    events = get_active_events(con, ticker)

    # Build summary
    thesis_risks = []
    thesis_catalysts = []
    for e in events:
        label = f"{e['event_type']}: {e['description']}"
        if e.get("impact_bias") == "BEARISH" and e.get("confidence") in ("HIGH", "MEDIUM"):
            thesis_risks.append(label)
        elif e.get("impact_bias") == "BULLISH" and e.get("confidence") in ("HIGH", "MEDIUM"):
            thesis_catalysts.append(label)

    parts = []
    if thesis_risks:
        parts.append(f"{len(thesis_risks)} risk(s)")
    if thesis_catalysts:
        parts.append(f"{len(thesis_catalysts)} catalyst(s)")
    event_summary = ", ".join(parts) if parts else "No active events"

    return {
        "profile": profile,
        "events": events,
        "event_summary": event_summary,
        "thesis_risks": thesis_risks,
        "thesis_catalysts": thesis_catalysts,
    }


def format_research_for_card(con, ticker: str) -> Optional[str]:
    """
    Format research context as a compact string for management card display.
    Returns None if no research data exists.
    """
    ctx = get_research_context(con, ticker)

    if ctx["profile"] is None and not ctx["events"]:
        return None

    parts = []

    # Profile one-liner
    p = ctx["profile"]
    if p:
        if p.get("thesis_summary"):
            parts.append(f"Thesis: {p['thesis_summary']}")
        if p.get("key_risks"):
            parts.append(f"Risks: {p['key_risks']}")

    # Active events
    if ctx["thesis_risks"]:
        parts.append(f"⚠️ Active risks: {'; '.join(ctx['thesis_risks'])}")
    if ctx["thesis_catalysts"]:
        parts.append(f"✅ Catalysts: {'; '.join(ctx['thesis_catalysts'])}")

    return " | ".join(parts) if parts else None
