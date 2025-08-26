# scripts/paths.py
from pathlib import Path

# repo root = parent of /scripts
REPO_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR   = REPO_ROOT / "data"
PICKS_DIR  = REPO_ROOT / "picks"
REPORTS_DIR= REPO_ROOT / "reports"

# subfolders you use often
SURVIVOR_DIR        = PICKS_DIR / "survivor"
REPORTS_BACKTESTS   = REPORTS_DIR / "backtests"
REPORTS_HANDOFF     = REPORTS_DIR / "handoff"

def ensure_dirs():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_BACKTESTS.mkdir(parents=True, exist_ok=True)
