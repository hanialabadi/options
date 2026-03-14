"""
Enforce normalize_iv() as the single authority for IV/HV format conversion.

Prevents future code drift where someone adds an inline lambda or hardcoded
threshold to convert IV between percentage and decimal forms. All such
conversions must go through core.shared.finance_utils.normalize_iv() or
normalize_iv_series().

Background (Mar 2026):
  Schwab stores IV as percentage (22.7 = 22.7%), live_greeks_provider converts
  to decimal (0.227) at extraction. Multiple inline lambdas with threshold 2.0
  caused double-normalization for high-IV stocks (>200% IV), blocking recovery
  paths and corrupting drift calculations. Fixed by centralizing on
  normalize_iv() with threshold 10.0.
"""
import ast
import os
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Files that ARE the authority — allowed to contain the conversion logic
ALLOWED_FILES = {
    "core/shared/finance_utils.py",  # canonical normalize_iv / normalize_iv_series
}

# Directories to scan
SCAN_DIRS = [
    "core/management",
    "scan_engine",
    "config",
    "utils",
    "scripts",
]

# Pattern 1: inline lambda with /100 and threshold (the exact pattern we fixed)
#   lambda x: x / 100.0 if ... and x > 2.0 else x
INLINE_LAMBDA_RE = re.compile(
    r'lambda\s+\w+\s*:\s*\w+\s*/\s*100[.\d]*\s+if\s+.*>\s*[12]\.\d.*else',
    re.IGNORECASE,
)

# Pattern 2: hardcoded IV normalization threshold comparisons
#   if v > 1.0:  v /= 100   (or any threshold < 10 used for IV/HV)
#   if x > 2.0:  x / 100
HARDCODED_THRESHOLD_RE = re.compile(
    r'(?:iv|hv|vol)\S*\s*(?:>|>=)\s*[12]\.\d.*(?:/=?\s*100|/\s*100)',
    re.IGNORECASE,
)

# Pattern 3: raw / 100 on IV-named variables without using normalize_iv
#   _iv_raw / 100  or  iv_entry / 100.0  (but NOT iv_pct / 100 which is a named field)
IV_DIV_100_RE = re.compile(
    r'\b(?:iv_entry|iv_now|hv_\d+d|iv_\d+d)\w*\s*/\s*100',
    re.IGNORECASE,
)


def _python_files_in(dirs):
    """Yield (relative_path, full_path) for all .py files in given dirs."""
    for d in dirs:
        base = ROOT / d
        if not base.exists():
            continue
        for p in base.rglob("*.py"):
            rel = str(p.relative_to(ROOT))
            if rel not in ALLOWED_FILES:
                yield rel, p


class TestNormalizeIVAuthority(unittest.TestCase):
    """Ensure no inline IV/HV normalization exists outside finance_utils.py."""

    def test_no_inline_lambda_normalization(self):
        """No inline lambda x: x/100 if x > 2.0 patterns outside the authority."""
        violations = []
        for rel, path in _python_files_in(SCAN_DIRS):
            try:
                content = path.read_text()
            except Exception:
                continue
            for i, line in enumerate(content.splitlines(), 1):
                if INLINE_LAMBDA_RE.search(line):
                    violations.append(f"  {rel}:{i}: {line.strip()}")

        self.assertEqual(
            violations, [],
            f"Found inline IV/HV normalization lambda(s) — use normalize_iv() instead:\n"
            + "\n".join(violations)
        )

    def test_no_hardcoded_iv_threshold_division(self):
        """No hardcoded 'if iv > 1.0: iv /= 100' patterns."""
        violations = []
        for rel, path in _python_files_in(SCAN_DIRS):
            try:
                content = path.read_text()
            except Exception:
                continue
            for i, line in enumerate(content.splitlines(), 1):
                if HARDCODED_THRESHOLD_RE.search(line):
                    # Exclude comments
                    stripped = line.lstrip()
                    if stripped.startswith("#"):
                        continue
                    violations.append(f"  {rel}:{i}: {line.strip()}")

        self.assertEqual(
            violations, [],
            f"Found hardcoded IV/HV threshold + /100 pattern(s) — use normalize_iv():\n"
            + "\n".join(violations)
        )

    def test_normalize_iv_is_importable(self):
        """The canonical functions exist and are importable."""
        from core.shared.finance_utils import normalize_iv, normalize_iv_series
        self.assertIsNotNone(normalize_iv)
        self.assertIsNotNone(normalize_iv_series)

    def test_normalize_iv_threshold_is_10(self):
        """The threshold must be 10.0 (not 2.0) to handle IV > 200%."""
        from core.shared.finance_utils import normalize_iv
        # 2.27 (decimal 227% IV) must NOT be divided — it's already decimal
        self.assertAlmostEqual(normalize_iv(2.27), 2.27)
        # 5.0 (decimal 500% IV) must NOT be divided
        self.assertAlmostEqual(normalize_iv(5.0), 5.0)
        # 22.7 (percentage 22.7% IV) MUST be divided → 0.227
        self.assertAlmostEqual(normalize_iv(22.7), 0.227)
        # 227.0 (percentage 227% IV) MUST be divided → 2.27
        self.assertAlmostEqual(normalize_iv(227.0), 2.27)

    def test_normalize_iv_edge_cases(self):
        """Edge cases: None, NaN, zero, negative."""
        from core.shared.finance_utils import normalize_iv
        import math
        self.assertIsNone(normalize_iv(None))
        self.assertIsNone(normalize_iv(float('nan')))
        self.assertAlmostEqual(normalize_iv(0.0), 0.0)
        # Boundary: 10.0 exactly is decimal (1000% IV — extreme but plausible)
        self.assertAlmostEqual(normalize_iv(10.0), 10.0)
        # Just above boundary: 10.1 is percentage form
        self.assertAlmostEqual(normalize_iv(10.1), 0.101)

    def test_normalize_iv_series_consistent(self):
        """Series version must use the same threshold as scalar version."""
        import pandas as pd
        from core.shared.finance_utils import normalize_iv, normalize_iv_series
        test_values = [0.25, 2.27, 5.0, 10.0, 22.7, 227.0]
        series = pd.Series(test_values)
        result = normalize_iv_series(series)
        for i, v in enumerate(test_values):
            expected = normalize_iv(v)
            self.assertAlmostEqual(
                result.iloc[i], expected,
                msg=f"Series and scalar diverge at value {v}"
            )


if __name__ == "__main__":
    unittest.main()
