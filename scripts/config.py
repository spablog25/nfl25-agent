# scripts/config.py
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]

SCHED_CLEAN = ROOT / "data" / "2025_nfl_schedule_cleaned.csv"
ROADMAP     = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"
DVOA_PATH   = ROOT / "data" / "dvoa_data.csv"
ODDS_CACHE  = ROOT / "data" / "odds_cache"
