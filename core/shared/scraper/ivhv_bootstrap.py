import os
from datetime import datetime
import subprocess
from core.shared.data_contracts.config import PROJECT_ROOT

ARCHIVE_DIR = str(PROJECT_ROOT / "data" / "ivhv_archive")
SNAPSHOT_FMT = "ivhv_snapshot_{date}.csv"

def get_today_snapshot_path():
    today = datetime.today().strftime("%Y-%m-%d")
    return os.path.join(ARCHIVE_DIR, SNAPSHOT_FMT.format(date=today))

def ensure_ivhv_snapshot():
    """Check for today's IV/HV snapshot. If missing, run the scraper with correct PYTHONPATH."""
    path = get_today_snapshot_path()
    if os.path.exists(path):
        print(f"✅ IV/HV snapshot found: {path}")
        return path
    else:
        print(f"⚠️ IV/HV snapshot for today NOT found: {path}")
        print("🔄 Running IV/HV scraper...")

        scraper_script = "main.py"  # or "run.py" if that's your entry point

        # --- PATCH: Set PYTHONPATH to include parent of 'core'
        from core.shared.data_contracts.config import PROJECT_ROOT
        scraper_dir = os.path.dirname(os.path.abspath(__file__))            # .../core/shared/scraper
        repo_root = PROJECT_ROOT
        env = dict(os.environ)
        env["PYTHONPATH"] = str(repo_root)

        # Run the scraper with the correct PYTHONPATH
        result = subprocess.run(
            ["python", scraper_script],
            cwd=scraper_dir,
            env=env,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"❌ Scraper failed! Error:\n{result.stderr}")
            raise RuntimeError("IV/HV scraper failed.")
        # After run, check again
        if os.path.exists(path):
            print(f"✅ IV/HV snapshot created: {path}")
            return path
        else:
            raise FileNotFoundError(f"❌ IV/HV snapshot still missing after scraper run: {path}")

def get_latest_ivhv_snapshot():
    """Get the latest available IV/HV snapshot in the archive folder."""
    files = [f for f in os.listdir(ARCHIVE_DIR) if f.startswith("ivhv_snapshot_")]
    if not files:
        raise FileNotFoundError("❌ No IV/HV snapshots found in archive.")
    files = sorted(files, reverse=True)
    latest = os.path.join(ARCHIVE_DIR, files[0])
    print(f"ℹ️ Using latest available IV/HV snapshot: {latest}")
    return latest

# Optional CLI mode
if __name__ == "__main__":
    try:
        ensure_ivhv_snapshot()
    except Exception as e:
        print(str(e))
