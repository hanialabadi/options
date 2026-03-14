"""Tests for ticker_research — profile + event CRUD and engine-facing queries."""

import pytest
import duckdb
from datetime import date

from core.shared.data_layer.ticker_research import (
    initialize_ticker_research_tables,
    upsert_ticker_profile,
    get_ticker_profile,
    get_all_profiles,
    add_ticker_event,
    get_active_events,
    get_all_active_events,
    resolve_event,
    get_research_context,
    format_research_for_card,
)


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    initialize_ticker_research_tables(c)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Profile tests
# ---------------------------------------------------------------------------

class TestTickerProfile:
    def test_create_and_read(self, con):
        upsert_ticker_profile(con, "QCOM",
            thesis_summary="AI edge compute + mobile moat",
            moat_type="WIDE",
            key_risks="ARM competition, China export controls",
            sector="Semiconductors",
        )
        p = get_ticker_profile(con, "QCOM")
        assert p is not None
        assert p["ticker"] == "QCOM"
        assert p["thesis_summary"] == "AI edge compute + mobile moat"
        assert p["moat_type"] == "WIDE"
        assert p["key_risks"] == "ARM competition, China export controls"

    def test_upsert_updates_existing(self, con):
        upsert_ticker_profile(con, "AAPL", thesis_summary="Old thesis")
        upsert_ticker_profile(con, "AAPL", thesis_summary="Updated thesis")
        p = get_ticker_profile(con, "AAPL")
        assert p["thesis_summary"] == "Updated thesis"

    def test_missing_profile_returns_none(self, con):
        assert get_ticker_profile(con, "NOPE") is None

    def test_inactive_profile_hidden(self, con):
        upsert_ticker_profile(con, "OLD", thesis_summary="Sold", is_active=False)
        assert get_ticker_profile(con, "OLD") is None

    def test_get_all_profiles(self, con):
        upsert_ticker_profile(con, "AAPL", sector="Tech")
        upsert_ticker_profile(con, "XOM", sector="Energy")
        df = get_all_profiles(con)
        assert len(df) == 2
        assert set(df["ticker"].tolist()) == {"AAPL", "XOM"}


# ---------------------------------------------------------------------------
# Event tests
# ---------------------------------------------------------------------------

class TestTickerEvents:
    def test_add_and_read(self, con):
        add_ticker_event(con, "QCOM", date(2026, 3, 10), "COMPETITIVE",
            "ARM winning Samsung Galaxy S27 modem contract",
            impact_bias="BEARISH", impact_horizon="LONG", confidence="HIGH",
            source="industry reports")
        events = get_active_events(con, "QCOM")
        assert len(events) == 1
        assert events[0]["event_type"] == "COMPETITIVE"
        assert events[0]["impact_bias"] == "BEARISH"

    def test_multiple_events(self, con):
        add_ticker_event(con, "QCOM", date(2026, 3, 1), "REGULATORY",
            "China export controls tightened", impact_bias="BEARISH")
        add_ticker_event(con, "QCOM", date(2026, 3, 5), "PRODUCT",
            "Snapdragon X Elite wins laptop design", impact_bias="BULLISH")
        events = get_active_events(con, "QCOM")
        assert len(events) == 2

    def test_resolve_event(self, con):
        add_ticker_event(con, "AAPL", date(2026, 3, 1), "REGULATORY",
            "EU DMA investigation", impact_bias="BEARISH")
        events = get_active_events(con, "AAPL")
        assert len(events) == 1
        resolve_event(con, events[0]["id"], note="Settled with minor fine")
        assert len(get_active_events(con, "AAPL")) == 0

    def test_filter_by_horizon(self, con):
        add_ticker_event(con, "TSLA", date(2026, 3, 1), "PRODUCT",
            "FSD v13 rollout", impact_bias="BULLISH", impact_horizon="SHORT")
        add_ticker_event(con, "TSLA", date(2026, 3, 1), "COMPETITIVE",
            "BYD global expansion", impact_bias="BEARISH", impact_horizon="LONG")
        short = get_active_events(con, "TSLA", horizon="SHORT")
        assert len(short) == 1
        assert short[0]["impact_horizon"] == "SHORT"

    def test_get_all_active_events_filtered(self, con):
        add_ticker_event(con, "AAPL", date(2026, 3, 1), "PRODUCT", "Vision Pro 2")
        add_ticker_event(con, "QCOM", date(2026, 3, 1), "COMPETITIVE", "ARM threat")
        add_ticker_event(con, "XOM", date(2026, 3, 1), "SECTOR", "Oil price drop")
        df = get_all_active_events(con, tickers=["AAPL", "QCOM"])
        assert len(df) == 2
        assert "XOM" not in df["ticker"].values


# ---------------------------------------------------------------------------
# Engine-facing queries
# ---------------------------------------------------------------------------

class TestResearchContext:
    def test_full_context(self, con):
        upsert_ticker_profile(con, "QCOM",
            thesis_summary="Mobile + AI edge",
            key_risks="ARM, China")
        add_ticker_event(con, "QCOM", date(2026, 3, 10), "COMPETITIVE",
            "ARM winning designs", impact_bias="BEARISH", confidence="HIGH")
        add_ticker_event(con, "QCOM", date(2026, 3, 5), "PRODUCT",
            "Snapdragon X Elite laptop wins", impact_bias="BULLISH", confidence="MEDIUM")

        ctx = get_research_context(con, "QCOM")
        assert ctx["profile"]["thesis_summary"] == "Mobile + AI edge"
        assert len(ctx["events"]) == 2
        assert len(ctx["thesis_risks"]) == 1
        assert len(ctx["thesis_catalysts"]) == 1
        assert "1 risk" in ctx["event_summary"]
        assert "1 catalyst" in ctx["event_summary"]

    def test_empty_ticker(self, con):
        ctx = get_research_context(con, "NOPE")
        assert ctx["profile"] is None
        assert ctx["events"] == []
        assert ctx["event_summary"] == "No active events"

    def test_format_for_card(self, con):
        upsert_ticker_profile(con, "AAPL", thesis_summary="Ecosystem moat")
        add_ticker_event(con, "AAPL", date(2026, 3, 1), "REGULATORY",
            "EU DMA fine risk", impact_bias="BEARISH", confidence="HIGH")
        text = format_research_for_card(con, "AAPL")
        assert text is not None
        assert "Ecosystem moat" in text
        assert "EU DMA" in text

    def test_format_returns_none_for_empty(self, con):
        assert format_research_for_card(con, "NOPE") is None
