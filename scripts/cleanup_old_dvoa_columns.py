from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ROADMAP = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

# Old placeholders we used before the FTN pipeline
OLD_COLS = [
    "team_dvoa","opp_dvoa",
    "team_off_dvoa","opp_off_dvoa",
    "team_def_dvoa","opp_def_dvoa",
    # pre‑FTN temporary names
    "team_tot_dvoa","opp_tot_dvoa","dvoa_gap",
]

if not ROADMAP.exists():
    raise FileNotFoundError(ROADMAP)

df = pd.read_csv(ROADMAP)
kept = [c for c in df.columns if c not in OLD_COLS]
removed = [c for c in df.columns if c in OLD_COLS]

df[kept].to_csv(ROADMAP, index=False)
print("✅ Removed columns:", removed)
print("💾 Saved:", ROADMAP)