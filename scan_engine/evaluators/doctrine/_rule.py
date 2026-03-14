"""
Doctrine Rule primitives.

DoctrineRule  – single threshold check (gt/lt/gte/lte/between/custom).
GraduatedRule – tiered deductions with an optional hard-reject floor.

Every rule carries a RAG citation so auditors can trace back to the book.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional, Tuple


@dataclass(frozen=True)
class DoctrineRule:
    """Immutable evaluation rule backed by a RAG citation.

    Parameters
    ----------
    name : str
        Machine-readable rule id (e.g. ``"rv_iv_edge"``).
    threshold : float | None
        Primary threshold.  ``None`` for custom-only rules.
    comparison : str
        One of ``"gt"``, ``"lt"``, ``"gte"``, ``"lte"``, ``"between"``,
        ``"custom"``.
    high : float | None
        Upper bound when *comparison* is ``"between"``.
    deduction : int
        Points to subtract when the rule **fails** (0 = informational).
    hard_reject : bool
        If ``True``, a failure triggers immediate Reject (skew, VVIX …).
    citation : str
        RAG book reference (e.g. ``"Sinclair Ch.3: sell vol when IV > RV"``).
    note_pass : str
        Human-readable note appended when the rule passes.
    note_fail : str
        Human-readable note appended when the rule fails.
    custom_fn : Callable | None
        For ``"custom"`` comparison – receives keyword args, returns ``bool``.
    """

    name: str
    threshold: Optional[float] = None
    comparison: str = "custom"
    high: Optional[float] = None
    deduction: int = 0
    hard_reject: bool = False
    citation: str = ""
    note_pass: str = ""
    note_fail: str = ""
    custom_fn: Optional[Callable] = field(default=None, repr=False)

    # ------------------------------------------------------------------
    def check(self, value=None, **kwargs) -> bool:
        """Evaluate the rule.

        Returns ``True`` when the rule **passes** (value is acceptable).
        For ``"custom"`` rules, keyword args are forwarded to *custom_fn*.
        """
        if self.comparison == "custom":
            if self.custom_fn is None:
                return False
            return bool(self.custom_fn(**kwargs))

        if value is None:
            return False

        if self.comparison == "gt":
            return value > self.threshold
        if self.comparison == "lt":
            return value < self.threshold
        if self.comparison == "gte":
            return value >= self.threshold
        if self.comparison == "lte":
            return value <= self.threshold
        if self.comparison == "between":
            # Supports one-sided bounds:
            #   threshold=None  →  no lower bound
            #   high=None       →  no upper bound
            if self.threshold is not None and value < self.threshold:
                return False
            if self.high is not None and value > self.high:
                return False
            return True
        return False


@dataclass(frozen=True)
class GraduatedRule:
    """Rule with tiered deductions.

    *tiers* is a tuple of ``(threshold, comparison, deduction, note)`` tuples,
    ordered **highest-to-lowest**.  The first matching tier wins.

    Parameters
    ----------
    name : str
        Machine-readable rule id.
    tiers : tuple
        Sequence of ``(threshold, comparison, deduction, note)`` tuples.
        Use ``(None, None, deduction, note)`` for the catch-all bottom tier.
    citation : str
        RAG book reference.
    hard_reject_below : float | None
        Value strictly below this triggers an immediate Reject.
    """

    name: str
    tiers: tuple  # ((threshold, comparison, deduction, note), ...)
    citation: str = ""
    hard_reject_below: Optional[float] = None

    def evaluate(self, value) -> Tuple[int, str, bool]:
        """Return ``(deduction, note, is_hard_reject)``.

        If *value* is ``None``, returns the bottom tier result.
        """
        if value is None:
            # Use bottom tier (catch-all)
            _, _, ded, note = self.tiers[-1]
            return ded, note, False

        # Hard reject floor
        if self.hard_reject_below is not None and value < self.hard_reject_below:
            _, _, ded, note = self.tiers[-1]
            return ded, note, True

        # Walk tiers top-down; first match wins
        for tier_threshold, tier_cmp, ded, note in self.tiers:
            if tier_threshold is None:
                # Catch-all
                return ded, note, False
            if tier_cmp == "gte" and value >= tier_threshold:
                return ded, note, False
            if tier_cmp == "gt" and value > tier_threshold:
                return ded, note, False
            if tier_cmp == "lte" and value <= tier_threshold:
                return ded, note, False
            if tier_cmp == "lt" and value < tier_threshold:
                return ded, note, False

        # No tier matched → bottom tier
        _, _, ded, note = self.tiers[-1]
        return ded, note, False
