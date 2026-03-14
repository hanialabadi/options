"""
risk_view.py — Portfolio Risk & Structure (Cycle 1)

Computable entirely from broker-reported Cycle 1 data.
No Cycle 2/3 enrichment columns referenced.

Sections:
  A. P/L Attribution   — by strategy, by ticker
  B. Assignment Risk   — ITM / near-money short options
  C. Concentration     — position sizing relative to portfolio
  D. Cost Basis Audit  — entry anchor vs current price
"""

import streamlit as st
import pandas as pd
import numpy as np
import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain


# ─────────────────────────────────────────────────────────────────────────────
# Data
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def get_portfolio_data(db_path_str: str = None) -> pd.DataFrame:
    """Load latest snapshot from DuckDB. READ-ONLY.

    The db_path_str parameter is accepted for backward compatibility but
    ignored; reads go through get_domain_connection(DbDomain.PIPELINE).
    """
    try:
        with get_domain_connection(DbDomain.PIPELINE, read_only=True) as con:
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).df()["table_name"].tolist()

            for t in ["enriched_legs_v1", "clean_legs_v2", "clean_legs"]:
                if t in tables:
                    run_id = con.execute(
                        f"SELECT run_id FROM {t} ORDER BY Snapshot_TS DESC LIMIT 1"
                    ).fetchone()
                    if run_id:
                        return con.execute(
                            f"SELECT * FROM {t} WHERE run_id = ?", [run_id[0]]
                        ).df()
    except Exception as e:
        logger.error(f"Failed to load portfolio data: {e}")
    return pd.DataFrame()


def _dte(expiration_series: pd.Series) -> pd.Series:
    today = pd.Timestamp(date.today())
    return (pd.to_datetime(expiration_series, errors="coerce") - today).dt.days


def _moneyness(row: pd.Series) -> str:
    """Classify option moneyness from broker data."""
    try:
        ul = float(row.get("UL Last") or 0)
        strike = float(row.get("Strike") or 0)
        cp = str(row.get("Call/Put") or row.get("OptionType") or "").upper()
        if ul <= 0 or strike <= 0:
            return "—"
        ratio = ul / strike
        if "C" in cp:
            if ratio >= 1.05:
                return "ITM"
            if ratio >= 0.97:
                return "NTM"
            return "OTM"
        elif "P" in cp:
            if ratio <= 0.95:
                return "ITM"
            if ratio <= 1.03:
                return "NTM"
            return "OTM"
    except Exception:
        pass
    return "—"


# ─────────────────────────────────────────────────────────────────────────────
# Section A — P/L Summary
# ─────────────────────────────────────────────────────────────────────────────

def _render_pl_summary(df: pd.DataFrame):
    st.subheader("P/L Summary")

    df = df.copy()
    df["GL_Num"] = pd.to_numeric(df["$ Total G/L"], errors="coerce").fillna(0)

    total_gl = df["GL_Num"].sum()
    winners = (df["GL_Num"] > 0).sum()
    losers = (df["GL_Num"] < 0).sum()
    worst = df.nsmallest(1, "GL_Num")
    best = df.nlargest(1, "GL_Num")

    c1, c2, c3, c4, c5 = st.columns(5)
    pnl_delta = f"+${total_gl:,.0f}" if total_gl >= 0 else f"-${abs(total_gl):,.0f}"
    c1.metric("Total P/L", pnl_delta)
    c2.metric("Winners", int(winners))
    c3.metric("Losers", int(losers))
    c4.metric("Best Position",
              f"{best.iloc[0]['Underlying_Ticker']} +${best.iloc[0]['GL_Num']:,.0f}" if not best.empty else "—")
    c5.metric("Worst Position",
              f"{worst.iloc[0]['Underlying_Ticker']} -${abs(worst.iloc[0]['GL_Num']):,.0f}" if not worst.empty else "—",
              delta_color="inverse")

    st.divider()

    by_ticker = (
        df.groupby("Underlying_Ticker")["GL_Num"]
        .sum()
        .sort_values()
        .reset_index()
    )
    by_ticker.columns = ["Ticker", "Total G/L"]

    col_chart, col_table = st.columns([2, 1])
    with col_chart:
        st.bar_chart(
            by_ticker.set_index("Ticker")["Total G/L"],
            width="stretch",
        )
    with col_table:
        def _style_gl(val):
            if isinstance(val, (int, float)):
                return "color: #09ab3b" if val >= 0 else "color: #ff4b4b"
            return ""

        st.dataframe(
            by_ticker.style.map(_style_gl, subset=["Total G/L"]).format({"Total G/L": "${:,.0f}"}),
            hide_index=True,
            width="stretch",
        )


# ─────────────────────────────────────────────────────────────────────────────
# Section B — Assignment Risk
# ─────────────────────────────────────────────────────────────────────────────

def _render_assignment_risk(df: pd.DataFrame):
    st.subheader("Assignment Risk — Short Options")

    options = df[df["AssetType"] == "OPTION"].copy()
    short_opts = options[pd.to_numeric(options["Quantity"], errors="coerce") < 0].copy()

    if short_opts.empty:
        st.success("No short option positions.")
        return

    short_opts["DTE"] = _dte(short_opts["Expiration"])
    short_opts["Moneyness"] = short_opts.apply(_moneyness, axis=1)
    short_opts["Delta_Abs"] = pd.to_numeric(short_opts["Delta"], errors="coerce").abs()
    short_opts["Last"] = pd.to_numeric(short_opts["Last"], errors="coerce")
    short_opts["Strike_Num"] = pd.to_numeric(short_opts["Strike"], errors="coerce")
    short_opts["UL_Last_Num"] = pd.to_numeric(short_opts["UL Last"], errors="coerce")
    short_opts["Qty_Abs"] = pd.to_numeric(short_opts["Quantity"], errors="coerce").abs()

    def _risk_level(row):
        dte = row.get("DTE")
        moneyness = row.get("Moneyness")
        delta = row.get("Delta_Abs")
        if moneyness == "ITM":
            return "🔴 HIGH — ITM"
        if pd.notna(dte) and dte <= 7 and moneyness == "NTM":
            return "🔴 HIGH — NTM <7d"
        if pd.notna(delta) and delta >= 0.70:
            return "🟠 ELEVATED — High Δ"
        if moneyness == "NTM":
            return "🟠 ELEVATED — NTM"
        if pd.notna(dte) and dte <= 21:
            return "🟡 WATCH — <21d"
        return "🟢 LOW"

    short_opts["Risk Level"] = short_opts.apply(_risk_level, axis=1)

    display = short_opts[[
        "Underlying_Ticker", "Symbol", "Call/Put", "Strike_Num",
        "UL_Last_Num", "DTE", "Delta_Abs", "Moneyness", "Risk Level", "Qty_Abs"
    ]].copy()
    display.columns = [
        "Ticker", "Contract", "Type", "Strike",
        "UL Price", "DTE", "|Δ|", "Moneyness", "Risk Level", "Qty"
    ]
    display = display.sort_values("Risk Level")

    st.dataframe(
        display,
        hide_index=True,
        width='stretch',
        column_config={
            "Strike":   st.column_config.NumberColumn("Strike",   format="$%.1f"),
            "UL Price": st.column_config.NumberColumn("UL Price", format="$%.2f"),
            "|Δ|":      st.column_config.NumberColumn("|Δ|",      format="%.3f"),
            "Qty":      st.column_config.NumberColumn("Qty",      format="%.0f"),
        },
    )

    high_risk    = short_opts[short_opts["Risk Level"].str.startswith("🔴")]
    elevated_risk = short_opts[short_opts["Risk Level"].str.startswith("🟠")]
    watch_risk    = short_opts[short_opts["Risk Level"].str.startswith("🟡")]

    if not high_risk.empty:
        st.error(f"🔴 {len(high_risk)} short option(s) at HIGH assignment risk: "
                 f"{', '.join(high_risk['Underlying_Ticker'].tolist())}")
    if not elevated_risk.empty:
        st.warning(f"🟠 {len(elevated_risk)} short option(s) ELEVATED: "
                   f"{', '.join(elevated_risk['Underlying_Ticker'].tolist())}")
    if not watch_risk.empty:
        st.info(f"🟡 {len(watch_risk)} short option(s) to WATCH: "
                f"{', '.join(watch_risk['Underlying_Ticker'].tolist())}")


# ─────────────────────────────────────────────────────────────────────────────
# Section C — Concentration
# ─────────────────────────────────────────────────────────────────────────────

def _render_concentration(df: pd.DataFrame):
    st.subheader("Position Concentration")

    df = df.copy()
    df["Basis_Num"] = pd.to_numeric(df["Basis"], errors="coerce").fillna(0).abs()
    total_basis = df["Basis_Num"].sum()

    if total_basis <= 0:
        st.info("No basis data available for concentration analysis.")
        return

    by_ticker = (
        df.groupby("Underlying_Ticker")["Basis_Num"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    by_ticker["% of Portfolio"] = by_ticker["Basis_Num"] / total_basis

    concentrated = by_ticker[by_ticker["% of Portfolio"] > 0.20]
    if not concentrated.empty:
        st.warning(f"⚠️ Over-concentrated (>20%): {', '.join(concentrated['Underlying_Ticker'].tolist())}")

    col_chart, col_table = st.columns([1, 1])
    with col_chart:
        st.bar_chart(
            by_ticker.set_index("Underlying_Ticker")["% of Portfolio"],
            width="stretch",
        )
    with col_table:
        st.dataframe(
            by_ticker.rename(columns={"Underlying_Ticker": "Ticker", "Basis_Num": "Capital Deployed"})
            .style.format({
                "Capital Deployed": "${:,.0f}",
                "% of Portfolio": "{:.1%}",
            }),
            hide_index=True,
            width="stretch",
        )


# ─────────────────────────────────────────────────────────────────────────────
# Section D — Cost Basis Audit
# ─────────────────────────────────────────────────────────────────────────────

def _render_cost_basis_audit(df: pd.DataFrame):
    st.subheader("Cost Basis Audit — Entry vs Current")
    st.caption("Compares entry anchor (frozen at first observation) vs today's price.")

    stocks = df[df["AssetType"] == "STOCK"].copy()
    if stocks.empty:
        st.info("No stock positions for basis audit.")
        return

    stocks["UL_Last"] = pd.to_numeric(stocks["UL Last"], errors="coerce")
    stocks["Entry_Price"] = pd.to_numeric(stocks.get("Underlying_Price_Entry"), errors="coerce")
    stocks["Qty"] = pd.to_numeric(stocks["Quantity"], errors="coerce")
    stocks["Basis_Total"] = pd.to_numeric(stocks["Basis"], errors="coerce")
    stocks["Basis_Per_Share"] = stocks["Basis_Total"] / stocks["Qty"].replace(0, np.nan)
    stocks["Current_Value"] = stocks["UL_Last"] * stocks["Qty"]
    stocks["Unrealized_GL"] = stocks["Current_Value"] - stocks["Basis_Total"]
    stocks["GL_Pct"] = stocks["Unrealized_GL"] / stocks["Basis_Total"].replace(0, np.nan)

    display = stocks[[
        "Underlying_Ticker", "Qty", "Entry_Price", "UL_Last",
        "Basis_Per_Share", "Basis_Total", "Current_Value", "Unrealized_GL", "GL_Pct"
    ]].copy()
    display.columns = [
        "Ticker", "Shares", "Entry $", "Current $",
        "Basis/Share", "Total Basis", "Current Value", "Unrealized G/L", "G/L %"
    ]

    def _style_gl(row):
        styles = [""] * len(row)
        try:
            val = row["Unrealized G/L"]
            if pd.notna(val):
                color = "color: #09ab3b" if val >= 0 else "color: #ff4b4b"
                gl_idx = list(display.columns).index("Unrealized G/L")
                pct_idx = list(display.columns).index("G/L %")
                styles[gl_idx] = color
                styles[pct_idx] = color
        except Exception:
            pass
        return styles

    st.dataframe(
        display.style.apply(_style_gl, axis=1).format({
            "Entry $": "${:.2f}",
            "Current $": "${:.2f}",
            "Basis/Share": "${:.2f}",
            "Total Basis": "${:,.0f}",
            "Current Value": "${:,.0f}",
            "Unrealized G/L": "${:,.0f}",
            "G/L %": "{:.2%}",
        }, na_rep="—"),
        hide_index=True,
        width="stretch",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def render_risk_view(core_project_root, sanitize_func):
    st.title("📊 Portfolio Risk & Structure")
    st.caption("Cycle 1 broker-reported data only. No inference or enrichment.")

    from core.shared.data_contracts.config import PIPELINE_DB_PATH
    df = get_portfolio_data(str(PIPELINE_DB_PATH))

    if df.empty:
        st.warning("No position data found. Upload a Fidelity CSV in **Perception (Upload)** first.")
        return

    df = df[df["AssetType"].isin(["OPTION", "STOCK"])].copy()

    if df.empty:
        st.warning("No OPTION or STOCK positions after filtering.")
        return

    tab_pl, tab_assign, tab_conc, tab_basis = st.tabs([
        "💰 P/L", "⚠️ Assignment Risk", "📐 Concentration", "🔍 Cost Basis"
    ])

    with tab_pl:
        _render_pl_summary(df)

    with tab_assign:
        _render_assignment_risk(df)

    with tab_conc:
        _render_concentration(df)

    with tab_basis:
        _render_cost_basis_audit(df)
