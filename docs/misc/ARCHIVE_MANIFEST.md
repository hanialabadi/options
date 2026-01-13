# Archive Manifest - January 1, 2026

## Purpose
Noise-reduction refactor to enable visual inspection of production architecture.

## What Was Archived

### 📁 archive/docs/ (94 files)
All development documentation including:
- STEP*.md - Step-by-step implementation guides
- PHASE*.md - Phase documentation
- *IMPLEMENTATION*.md - Implementation summaries
- *VALIDATION*.md, *TEST*.md - Test reports
- *GUIDE*.md, *ARCHITECTURE*.md - Technical guides
- *AUDIT*.md, *DIAGNOSTIC*.md - Audit reports
- *AUTH*.md, *SCHWAB*.md - Authentication walkthroughs
- *REFACTOR*.md, *SUMMARY*.md - Refactoring notes
- All other dev/status/completion docs

### 🐍 archive/legacy_code/ (64 files)
Debug, test, and validation scripts including:
- test_*.py (40+ test scripts)
- debug_*.py (debug utilities)
- audit_*.py (audit scripts)
- validate_*.py (ad-hoc validation)
- *.sh (development shell scripts)
- *.ipynb (debug notebooks)

### 📝 archive/dev_notes/ (20 files)
Outputs and logs including:
- *_output.txt, *_output.csv
- *_audit.txt, *_audit.csv
- *.log files
- phase_status.json
- Intermediate pipeline outputs

## What Was NOT Archived

### Protected Directories
- `core/scan_engine/` - Phase 1 production logic
- `core/management_engine/` - Phase 2 production logic (NEW)
- `core/data_contracts/` - I/O contracts (NEW)
- `core/legacy/` - Deprecated modules (documented)

### Production Files
- `README.md` - Project documentation
- `requirements.txt` - Dependencies
- `run_*.sh` - Production runners (dashboard, streamlit, CLI)
- `run_pipeline_cli.py`, `run_pipeline_steps_3_to_11.py` - CLI entry points
- `validate_phase_a*.py` - Phase A/B validation (part of refactor)

### Live Execution Paths
- All files imported by dashboard
- All files imported by CLI
- All files in production scan/management engines

## Verification Results

✅ **Core imports validated:**
   - `core.management_engine` ✅
   - `core.data_contracts` ✅
   - `core.scan_engine` ✅

✅ **No imports reference archived files**

✅ **178 files archived, 0 files deleted**

✅ **Zero functional behavior changed**

## Statistics
- Files archived: 178
- Markdown docs: 94
- Python/shell: 64
- Logs/outputs: 20
- Root directory decluttered: ~180 files → ~35 files

## Rollback
All files preserved in `/archive/`. To restore:
```bash
mv archive/docs/* .
mv archive/legacy_code/*.py .
mv archive/dev_notes/* .
```

---
**Confirmation:** This was a structural cleanup. No code logic was modified.
