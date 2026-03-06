"""
Doctrine Audit View
===================

Answers the five questions of the feedback loop:
  1. Why did this trade win?
  2. Why did this trade lose?
  3. Which gate failed?
  4. Which signal was ignored?
  5. Which condition repeatedly causes losses?

Three sections:
  A. Closed Trade Log       — every closed position with outcome + gate failed
  B. Condition Buckets      — aggregated win rate / avg P&L per entry condition
  C. Feedback Rules         — TIGHTEN / RELAX / REINFORCE suggestions (N-gated)

Statistically guarded: suggestions only appear when N >= 15.
Read-only: nothing in this view writes to the DB.
"""

import streamlit as st
import pandas as pd
import duckdb
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Colour helpers ────────────────────────────────────────────────────────────

_OUTCOME_COLORS = {
    "THESIS_COMPLETION":    "#1a472a",   # deep green
    "THETA_HARVEST":        "#1a472a",
    "PREMATURE_EXIT":       "#7a5c00",   # amber
    "THETA_MISMANAGEMENT":  "#7a5c00",
    "IGNORED_EXIT_SIGNAL":  "#7a5c00",
    "LATE_CYCLE_ENTRY":     "#6b1a1a",   # deep red
    "FALSE_GEM":            "#6b1a1a",
    "VOL_EDGE_MISREAD":     "#6b1a1a",
    "MOMENTUM_MISCLASSIFY": "#6b1a1a",
    "UNCLASSIFIED":         "#333333",
}

_SUGGEST_COLORS = {
    "TIGHTEN":              "#6b1a1a",
    "REINFORCE":            "#1a472a",
    "HOLD":                 "#1a3366",
    "INSUFFICIENT_SAMPLE":  "#444444",
}


def _color_outcome(val):
    c = _OUTCOME_COLORS.get(str(val), "")
    return f"background-color: {c}; color: white;" if c else ""


def _color_suggest(val):
    c = _SUGGEST_COLORS.get(str(val), "")
    return f"background-color: {c}; color: white;" if c else ""


def _pct(v):
    try:
        return f"{float(v):+.1%}"
    except Exception:
        return "—"


def _load_closed_trades(con) -> pd.DataFrame:
    try:
        return con.execute("""
            SELECT
                Underlying_Ticker   AS Ticker,
                Strategy,
                Outcome_Emoji       AS Emoji,
                Outcome_Type,
                PnL_Pct,
                Days_Held,
                MFE_Pct,
                MAE_Pct,
                Entry_MomentumState AS Momentum_At_Entry,
                Entry_IV_HV_Ratio   AS IV_HV_At_Entry,
                Entry_RSI           AS RSI_At_Entry,
                Gate_Failed,
                Outcome_Note,
                Exit_Action,
                Exit_Signal_Followed,
                Entry_TS,
                Exit_TS
            FROM closed_trades
            ORDER BY Exit_TS DESC
        """).fetchdf()
    except Exception as e:
        logger.warning(f"[AuditView] closed_trades query failed: {e}")
        return pd.DataFrame()


def _load_feedback(con) -> pd.DataFrame:
    try:
        return con.execute("""
            SELECT
                condition_label     AS Condition,
                strategy            AS Strategy,
                sample_n            AS N,
                win_rate            AS Win_Rate,
                avg_pnl_pct         AS Avg_PnL,
                avg_days_held       AS Avg_Days,
                avg_mfe_pct         AS Avg_MFE,
                avg_mae_pct         AS Avg_MAE,
                suggested_action    AS Suggestion,
                confidence          AS Confidence,
                last_updated        AS Updated
            FROM doctrine_feedback
            ORDER BY win_rate ASC
        """).fetchdf()
    except Exception as e:
        logger.warning(f"[AuditView] doctrine_feedback query failed: {e}")
        return pd.DataFrame()


def _load_step3_queries(con) -> dict:
    """
    The five structured queries from Step 3 of the doctrine feedback spec.
    Returns a dict of {label: DataFrame}.
    """
    queries = {}
    try:
        queries["LATE_CYCLE entries"] = con.execute("""
            SELECT Underlying_Ticker, Strategy, PnL_Pct, Days_Held, Outcome_Type, Gate_Failed
            FROM closed_trades
            WHERE Entry_MomentumState = 'LATE_CYCLE'
            ORDER BY PnL_Pct ASC
        """).fetchdf()
    except Exception:
        queries["LATE_CYCLE entries"] = pd.DataFrame()

    try:
        queries["RSI > 70 at entry"] = con.execute("""
            SELECT Underlying_Ticker, Strategy, Entry_RSI, PnL_Pct, Days_Held, Outcome_Type
            FROM closed_trades
            WHERE Entry_RSI > 70
            ORDER BY PnL_Pct ASC
        """).fetchdf()
    except Exception:
        queries["RSI > 70 at entry"] = pd.DataFrame()

    try:
        queries["IV > HV long-vol entries"] = con.execute("""
            SELECT Underlying_Ticker, Strategy, Entry_IV_HV_Ratio, PnL_Pct, Days_Held, Outcome_Type
            FROM closed_trades
            WHERE Entry_IV_HV_Ratio > 1.10
              AND Strategy LIKE '%LONG%'
            ORDER BY PnL_Pct ASC
        """).fetchdf()
    except Exception:
        queries["IV > HV long-vol entries"] = pd.DataFrame()

    try:
        queries["EXIT signal ignored"] = con.execute("""
            SELECT Underlying_Ticker, Strategy, PnL_Pct, Days_Held, Exit_Action, Outcome_Type
            FROM closed_trades
            WHERE Exit_Signal_Followed = FALSE
            ORDER BY PnL_Pct ASC
        """).fetchdf()
    except Exception:
        queries["EXIT signal ignored"] = pd.DataFrame()

    try:
        queries["ACCELERATING entries (wins)"] = con.execute("""
            SELECT Underlying_Ticker, Strategy, PnL_Pct, MFE_Pct, Days_Held, Outcome_Type
            FROM closed_trades
            WHERE Entry_MomentumState = 'ACCELERATING'
            ORDER BY PnL_Pct DESC
        """).fetchdf()
    except Exception:
        queries["ACCELERATING entries (wins)"] = pd.DataFrame()

    return queries


# ── Main render ───────────────────────────────────────────────────────────────

def render_audit_view(core_project_root: str, set_view=None) -> None:
    if set_view and st.button("← Back to Home"):
        set_view("home")
        return

    st.title("📚 Doctrine Audit")
    st.caption(
        "Structured feedback loop — deterministic, book-anchored, non-emotional. "
        "Answers: why did this trade win or lose, which gate failed, "
        "which condition repeatedly causes losses."
    )

    db_path = Path(core_project_root) / "data" / "pipeline.duckdb"
    if not db_path.exists():
        st.error(f"Pipeline DB not found at {db_path}")
        return

    try:
        # Attempt read-only; if stale lock from crashed writer, recover via checkpoint.
        try:
            con = duckdb.connect(str(db_path), read_only=True)
        except Exception as lock_err:
            if "Conflicting lock" in str(lock_err) or "lock" in str(lock_err).lower():
                logger.warning(f"[AuditView] Stale DuckDB lock — attempting WAL recovery: {lock_err}")
                _rc = duckdb.connect(str(db_path), read_only=False)
                _rc.execute("CHECKPOINT")
                _rc.close()
                con = duckdb.connect(str(db_path), read_only=True)
            else:
                raise
    except Exception as e:
        st.error(f"Cannot open pipeline DB: {e}")
        return

    closed  = _load_closed_trades(con)
    feedback = _load_feedback(con)

    # ── Summary banner ────────────────────────────────────────────────────────
    if not closed.empty:
        n_total  = len(closed)
        n_win    = int(closed["Outcome_Type"].isin(["THESIS_COMPLETION", "THETA_HARVEST"]).sum())
        n_loss   = int(closed["Outcome_Type"].isin(
            ["LATE_CYCLE_ENTRY", "FALSE_GEM", "VOL_EDGE_MISREAD", "MOMENTUM_MISCLASSIFY"]).sum())
        n_warn   = n_total - n_win - n_loss
        avg_pnl  = closed["PnL_Pct"].mean()

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Closed Trades", n_total)
        c2.metric("Wins ✅", n_win)
        c3.metric("Losses ❌", n_loss)
        c4.metric("Warnings ⚠️", n_warn)
        c5.metric("Avg P&L", f"{avg_pnl:+.1%}" if not pd.isna(avg_pnl) else "—")
        st.divider()
    else:
        st.info("No closed trades recorded yet. Trades are logged automatically when a position disappears from the portfolio.")
        con.close()
        return

    # ── Section A: Closed Trade Log ───────────────────────────────────────────
    with st.expander("📋 Closed Trade Log", expanded=True):
        st.caption("Every closed position with outcome classification and gate analysis.")

        display = closed[[
            "Emoji", "Ticker", "Strategy", "Outcome_Type",
            "PnL_Pct", "MFE_Pct", "MAE_Pct", "Days_Held",
            "Momentum_At_Entry", "IV_HV_At_Entry", "RSI_At_Entry",
            "Gate_Failed", "Exit_Action", "Exit_Signal_Followed",
        ]].copy()

        display["PnL_Pct"] = display["PnL_Pct"].apply(_pct)
        display["MFE_Pct"] = display["MFE_Pct"].apply(_pct)
        display["MAE_Pct"] = display["MAE_Pct"].apply(_pct)
        display["IV_HV_At_Entry"] = display["IV_HV_At_Entry"].apply(
            lambda v: f"{v:.2f}" if pd.notna(v) else "—"
        )
        display["RSI_At_Entry"] = display["RSI_At_Entry"].apply(
            lambda v: f"{v:.0f}" if pd.notna(v) else "—"
        )
        display["Days_Held"] = display["Days_Held"].apply(
            lambda v: f"{v:.0f}d" if pd.notna(v) else "—"
        )
        display["Exit_Signal_Followed"] = display["Exit_Signal_Followed"].map(
            {True: "✅ Yes", False: "❌ No"}
        )

        # Rename "Emoji" → " " for a near-blank header (DuckDB rejects zero-length aliases)
        display = display.rename(columns={"Emoji": " "})

        st.dataframe(
            display.style.map(_color_outcome, subset=["Outcome_Type"]),
            hide_index=True,
            width="stretch",
        )

        # Outcome note for each trade
        if "Outcome_Note" in closed.columns:
            st.markdown("**Gate Analysis:**")
            for _, row in closed.iterrows():
                note  = str(row.get("Outcome_Note") or "")
                gate  = str(row.get("Gate_Failed") or "")
                emoji = str(row.get("Emoji") or "")
                ticker = str(row.get("Ticker") or "")
                otype  = str(row.get("Outcome_Type") or "")
                if note:
                    st.caption(
                        f"{emoji} **{ticker}** ({otype})"
                        + (f" — gate: `{gate}`" if gate else "")
                        + f"  \n{note}"
                    )

    # ── Section B: Condition Buckets ──────────────────────────────────────────
    with st.expander("📊 Condition Bucket Analysis", expanded=True):
        st.caption(
            "Aggregated win rate and average P&L per entry condition. "
            f"Suggestions only appear when N ≥ 15."
        )

        if feedback.empty:
            st.info("No aggregated feedback yet — requires at least one closed trade per condition bucket.")
        else:
            fb_display = feedback[[
                "Condition", "N", "Win_Rate", "Avg_PnL",
                "Avg_MFE", "Avg_MAE", "Avg_Days",
                "Suggestion", "Confidence",
            ]].copy()

            fb_display["Win_Rate"] = fb_display["Win_Rate"].apply(
                lambda v: f"{v:.0%}" if pd.notna(v) else "—"
            )
            fb_display["Avg_PnL"] = fb_display["Avg_PnL"].apply(_pct)
            fb_display["Avg_MFE"] = fb_display["Avg_MFE"].apply(_pct)
            fb_display["Avg_MAE"] = fb_display["Avg_MAE"].apply(_pct)
            fb_display["Avg_Days"] = fb_display["Avg_Days"].apply(
                lambda v: f"{v:.0f}d" if pd.notna(v) else "—"
            )

            st.dataframe(
                fb_display.style.map(_color_suggest, subset=["Suggestion"]),
                hide_index=True,
                width="stretch",
            )

    # ── Section C: Step 3 Diagnostic Queries ─────────────────────────────────
    with st.expander("🔍 Edge Validation Queries", expanded=False):
        st.caption(
            "The five structured queries from the doctrine feedback spec. "
            "These answer: which conditions produce losses reliably?"
        )

        step3 = _load_step3_queries(con)

        for label, df in step3.items():
            st.markdown(f"**{label}** — {len(df)} trades")
            if df.empty:
                st.caption("No data yet.")
                continue

            n = len(df)
            if "PnL_Pct" in df.columns:
                wins   = int((df["PnL_Pct"] > 0).sum())
                avg_pnl = df["PnL_Pct"].mean()
                st.caption(
                    f"Win rate: {wins}/{n} ({wins/n:.0%}) · Avg P&L: {avg_pnl:+.1%}"
                    + (f" · ⚠️ N={n} < 15 — low confidence" if n < 15 else "")
                )

            disp = df.copy()
            if "PnL_Pct" in disp.columns:
                disp["PnL_Pct"] = disp["PnL_Pct"].apply(_pct)
            if "MFE_Pct" in disp.columns:
                disp["MFE_Pct"] = disp["MFE_Pct"].apply(_pct)
            if "Entry_IV_HV_Ratio" in disp.columns:
                disp["Entry_IV_HV_Ratio"] = disp["Entry_IV_HV_Ratio"].apply(
                    lambda v: f"{v:.2f}" if pd.notna(v) else "—"
                )
            if "Entry_RSI" in disp.columns:
                disp["Entry_RSI"] = disp["Entry_RSI"].apply(
                    lambda v: f"{v:.0f}" if pd.notna(v) else "—"
                )

            st.dataframe(
                disp.style.map(_color_outcome, subset=["Outcome_Type"])
                if "Outcome_Type" in disp.columns else disp,
                hide_index=True,
                width="stretch",
            )
            st.divider()

    # ── Section D: What the system recommends ────────────────────────────────
    tighten = feedback[feedback["Suggestion"] == "TIGHTEN"] if not feedback.empty else pd.DataFrame()
    reinforce = feedback[feedback["Suggestion"] == "REINFORCE"] if not feedback.empty else pd.DataFrame()

    if not tighten.empty or not reinforce.empty:
        with st.expander("⚙️ Doctrine Recommendations", expanded=True):
            st.caption(
                "Deterministic rule suggestions based on closed-trade history. "
                "These are observations, not automatic changes. "
                "You decide whether to act on them."
            )

            if not tighten.empty:
                st.markdown("**🔴 TIGHTEN — these conditions have poor outcomes:**")
                for _, row in tighten.iterrows():
                    st.warning(
                        f"**{row['Condition']}** — "
                        f"Win rate {row['Win_Rate']} · Avg P&L {row['Avg_PnL']} · "
                        f"N={row['N']} · Confidence: {row['Confidence']}  \n"
                        f"Suggestion: raise entry threshold, require additional confirmation signals."
                    )

            if not reinforce.empty:
                st.markdown("**🟢 REINFORCE — these conditions produce consistent wins:**")
                for _, row in reinforce.iterrows():
                    st.success(
                        f"**{row['Condition']}** — "
                        f"Win rate {row['Win_Rate']} · Avg P&L {row['Avg_PnL']} · "
                        f"N={row['N']} · Confidence: {row['Confidence']}  \n"
                        f"Suggestion: weight PCS higher when this condition is present at entry."
                    )

    con.close()
