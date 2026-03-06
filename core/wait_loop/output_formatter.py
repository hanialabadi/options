"""
Output Formatter: Three-Tier Display (READY_NOW / WAITLIST / REJECTED)

Formats scan results for terminal and UI display.
"""

from typing import List, Dict, Any
from datetime import datetime
from .ttl import get_time_remaining
import logging

logger = logging.getLogger(__name__)


def format_ready_now(trades: List[Dict[str, Any]]) -> str:
    """
    Format READY_NOW trades for execution.

    Args:
        trades: List of promoted or new READY_NOW trades

    Returns:
        Formatted string for terminal display
    """
    if not trades:
        return """
================================================================================
🟢 EXECUTE NOW (0 trades)
================================================================================
No trades ready for immediate execution.
"""

    output = f"""
================================================================================
🟢 EXECUTE NOW ({len(trades)} trade{'s' if len(trades) != 1 else ''})
================================================================================
"""

    for i, trade in enumerate(trades, 1):
        ticker = trade.get("ticker", "UNKNOWN")
        strategy_name = trade.get("strategy_name", "UNKNOWN")
        strategy_type = trade.get("strategy_type", "")
        strike = trade.get("proposed_strike") or trade.get("contract_symbol", "TBD")
        expiration = trade.get("proposed_expiration", "TBD")
        confidence = trade.get("confidence_score", 0.0)
        origin = "Promoted from WAIT" if trade.get("wait_id") else "New discovery"

        # Rationale (compact)
        wait_progress = trade.get("wait_progress", 0.0)
        conditions_met_count = len(trade.get("conditions_met", []))
        total_conditions = len(trade.get("wait_conditions", []))

        if origin == "Promoted from WAIT":
            rationale = f"All {total_conditions} wait conditions satisfied"
        else:
            rationale = "Passed all execution gates"

        output += f"""
{i}. {ticker} - {strategy_name} ({strategy_type})
   Strike: {strike}
   Expiration: {expiration}
   Rationale: {rationale}
   Confidence: {confidence:.0%}
   Origin: {origin}
"""

    return output


def format_waitlist(trades: List[Dict[str, Any]]) -> str:
    """
    Format WAITLIST trades (AWAIT_CONFIRMATION).

    Args:
        trades: List of active wait entries

    Returns:
        Formatted string for terminal display
    """
    if not trades:
        return """
================================================================================
🟡 WAITLIST (0 trades)
================================================================================
No trades currently waiting on confirmation.
"""

    output = f"""
================================================================================
🟡 WAITLIST ({len(trades)} trade{'s' if len(trades) != 1 else ''})
================================================================================
"""

    for i, trade in enumerate(trades, 1):
        ticker = trade.get("ticker", "UNKNOWN")
        strategy_name = trade.get("strategy_name", "UNKNOWN")
        strategy_type = trade.get("strategy_type", "")
        strike = trade.get("proposed_strike", "TBD")
        expiration = trade.get("proposed_expiration", "TBD")

        # Wait conditions
        wait_conditions = trade.get("wait_conditions", [])
        conditions_met = trade.get("conditions_met", [])
        wait_progress = trade.get("wait_progress", 0.0)

        # TTL info
        time_remaining_info = get_time_remaining(trade)
        sessions_remaining = time_remaining_info["sessions_remaining"]
        sessions_elapsed = time_remaining_info["sessions_elapsed"]
        max_sessions = time_remaining_info["max_sessions"]
        days_remaining = time_remaining_info["days_remaining"]
        is_urgent = time_remaining_info["is_urgent"]

        output += f"""
{i}. {ticker} - {strategy_name} ({strategy_type})
   Strike: {strike} (proposed)
   Expiration: {expiration} (proposed)
   Waiting on: [{len(conditions_met)}/{len(wait_conditions)} conditions met]"""

        # Show condition status
        for condition_dict in wait_conditions:
            condition_id = condition_dict.get("condition_id", "")
            description = condition_dict.get("description", "")
            is_met = condition_id in conditions_met

            status_icon = "✅" if is_met else "⏳"
            output += f"\n     {status_icon} {description}"

        # Progress and TTL
        urgency_flag = " ⚠️ " if is_urgent else ""
        output += f"""
   Progress: {wait_progress:.0%}{urgency_flag}
   TTL: {days_remaining} days remaining (expires in {sessions_remaining}/{max_sessions} sessions)
   Sessions elapsed: {sessions_elapsed}
"""

    return output


def format_rejected(trades: List[Dict[str, Any]]) -> str:
    """
    Format REJECTED trades.

    Args:
        trades: List of rejected trades

    Returns:
        Formatted string for terminal display
    """
    if not trades:
        return """
================================================================================
🔴 REJECTED (0 trades)
================================================================================
No trades rejected in this scan.
"""

    output = f"""
================================================================================
🔴 REJECTED ({len(trades)} trade{'s' if len(trades) != 1 else ''})
================================================================================
"""

    for i, trade in enumerate(trades, 1):
        ticker = trade.get("ticker", "UNKNOWN")
        strategy_name = trade.get("strategy_name", "UNKNOWN")
        strategy_type = trade.get("strategy_type", "")
        rejection_reason = trade.get("rejection_reason") or trade.get("Gate_Reason", "UNKNOWN")

        # Extract gate code if present
        gate_code = "UNKNOWN"
        if ":" in rejection_reason:
            gate_code = rejection_reason.split(":")[0].strip()
            details = rejection_reason.split(":", 1)[1].strip()
        else:
            details = rejection_reason

        output += f"""
{i}. {ticker} - {strategy_name} ({strategy_type})
   Reason: {gate_code}
   Details: {details}
"""

    return output


def format_scan_summary(
    promoted_count: int,
    new_ready_count: int,
    still_waiting_count: int,
    expired_count: int,
    invalidated_count: int,
    new_rejected_count: int,
    total_evaluated: int,
    wait_list_evaluated: int
) -> str:
    """
    Format scan summary statistics.

    Args:
        promoted_count: WAIT → READY_NOW promotions
        new_ready_count: New discoveries → READY_NOW
        still_waiting_count: WAIT entries still active
        expired_count: WAIT entries expired (TTL)
        invalidated_count: WAIT entries invalidated
        new_rejected_count: New discoveries → REJECTED
        total_evaluated: Total strategies evaluated
        wait_list_evaluated: Wait list entries re-evaluated

    Returns:
        Formatted summary string
    """
    total_ready = promoted_count + new_ready_count
    total_rejected = expired_count + invalidated_count + new_rejected_count

    output = f"""
================================================================================
📊 SCAN SUMMARY
================================================================================
Total Strategies Evaluated: {total_evaluated}
  └─ From Wait List: {wait_list_evaluated} re-evaluated
     ├─ Promoted to READY_NOW: {promoted_count}
     ├─ Expired/Invalidated: {expired_count + invalidated_count}
     └─ Still Waiting: {still_waiting_count}
  └─ From New Discovery: {total_evaluated - wait_list_evaluated}
     ├─ READY_NOW: {new_ready_count}
     ├─ AWAIT_CONFIRMATION: 0 (will be shown in next scan)
     └─ REJECTED: {new_rejected_count}

Final Counts:
  🟢 READY_NOW: {total_ready} trade{'s' if total_ready != 1 else ''}
  🟡 AWAIT_CONFIRMATION: {still_waiting_count} trade{'s' if still_waiting_count != 1 else ''} (active in wait loop)
  🔴 REJECTED: {total_rejected} trade{'s' if total_rejected != 1 else ''}
================================================================================
"""

    return output


def format_complete_scan_output(
    ready_now_trades: List[Dict[str, Any]],
    waitlist_trades: List[Dict[str, Any]],
    rejected_trades: List[Dict[str, Any]],
    wait_list_evaluated: int = 0,
    total_evaluated: int = 0,
    promoted_count: int = 0,
    new_ready_count: int = 0,
    expired_count: int = 0,
    invalidated_count: int = 0,
    new_rejected_count: int = 0
) -> str:
    """
    Format complete scan output (all three tiers + summary).

    Args:
        ready_now_trades: READY_NOW trades
        waitlist_trades: WAITLIST trades
        rejected_trades: REJECTED trades
        wait_list_evaluated: Count of wait list entries re-evaluated
        total_evaluated: Total strategies evaluated
        promoted_count: Promoted from WAIT
        new_ready_count: New READY_NOW discoveries
        expired_count: Expired WAIT entries
        invalidated_count: Invalidated WAIT entries
        new_rejected_count: New rejections

    Returns:
        Complete formatted output
    """
    output = ""

    # READY_NOW section
    output += format_ready_now(ready_now_trades)

    # WAITLIST section
    output += "\n"
    output += format_waitlist(waitlist_trades)

    # REJECTED section
    output += "\n"
    output += format_rejected(rejected_trades)

    # Summary
    output += "\n"
    output += format_scan_summary(
        promoted_count=promoted_count,
        new_ready_count=new_ready_count,
        still_waiting_count=len(waitlist_trades),
        expired_count=expired_count,
        invalidated_count=invalidated_count,
        new_rejected_count=new_rejected_count,
        total_evaluated=total_evaluated,
        wait_list_evaluated=wait_list_evaluated
    )

    return output
