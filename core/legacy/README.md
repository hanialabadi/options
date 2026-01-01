# Legacy / Deprecated Modules

**This directory contains deprecated modules preserved for git history.**

## ❌ DO NOT IMPORT FROM THIS DIRECTORY
## ❌ DO NOT MODIFY THESE FILES

Canonical replacements exist elsewhere in the codebase.

---

## Files and Their Replacements

### Deprecated PCS Engines
- `pcs_engine_v2.py` → **Use:** `core/pcs_engine_v3_unified.py`
- `sus_phase3_pcs_score.py` → **Use:** `core/pcs_engine_v3_unified.py`

### Deprecated Recommendation Engines
- `rec_engine_v4_holistic.py` → **Use:** `core/rec_engine_v6_overlay.py`
- `rec_engine_v5_signal_tuned.py` → **Use:** `core/rec_engine_v6_overlay.py`
- `sus_phase7_rec_engine2222.py` → **Use:** `core/rec_engine_v6_overlay.py`

### Deprecated Drift Logic
- `sus_phase5_drift.py` → **Use:** `core/phase7_drift_engine.py`

### Deprecated Freeze/Greeks
- `Sus_phase3_5_freeze_fields.py` → **Use:** `core/phase6_freeze_and_archive.py`
- `sus_freeze_greeks.py` → **Use:** `utils/greek_extraction.py` + `utils/greek_math.py`

---

## Why These Files Are Here

These modules were superseded during system evolution:
- PCS scoring consolidated from v2 → v3
- Recommendation engine evolved v4 → v5 → v6
- "sus" prefix indicates experimental/superseded code
- Functionality moved to canonical locations

## Preservation Policy

Files are kept for:
- Git history and audit trail
- Reference during debugging
- Understanding system evolution

But should **never be imported** by active code.

---

**Last Updated:** Phase C - January 1, 2026
