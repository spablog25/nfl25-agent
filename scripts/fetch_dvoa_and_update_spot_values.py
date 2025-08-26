# scripts/fetch_dvoa_and_update_spot_values.py
from pathlib import Path
import pandas as pd
import numpy as np

from scripts.paths import DATA_DIR, SURVIVOR_DIR
from scripts.utils_read import read_csv_safe
from scripts.utils_io import snapshot_csv, write_csv_atomic

DVOA_PATH  = DATA_DIR / "dvoa_data.csv"
SCHED_PATH = SURVIVOR_DIR / "survivor_schedule_roadmap_expanded.csv"

def main():
    if not DVOA_PATH.exists():
        raise FileNotFoundError(f"Missing DVOA file: {DVOA_PATH}")
    if not SCHED_PATH.exists():
        raise FileNotFoundError(f"Missing schedule file: {SCHED_PATH}")

    dvoa = read_csv_safe(DVOA_PATH.as_posix())  # lowercases headers
    # extra normalization for pesky BOMs / spaces
    dvoa.columns = dvoa.columns.str.replace("\ufeff", "", regex=False).str.strip().str.lower()

    # find team column flexibly
    team_col = next((c for c in dvoa.columns if c in ("team", "team_name", "club")), None)
    if not team_col:
        raise ValueError(f"DVOA file missing a team column. Headers seen: {list(dvoa.columns)}")
    dvoa = dvoa.rename(columns={team_col: "team"})

    # pick up total dvoa (your file uses "tot dvoa"); fall back to Off-Def
    if "tot dvoa" in dvoa.columns:
        dvoa = dvoa.rename(columns={"tot dvoa": "tot_dvoa"})
    else:
        off_col = next((c for c in dvoa.columns if c in ("off dvoa", "offense dvoa", "dvoa_offense", "off")), None)
        def_col = next((c for c in dvoa.columns if c in ("def dvoa", "defense dvoa", "dvoa_defense", "def")), None)
        if not (off_col and def_col):
            raise ValueError(f"Couldn’t find 'tot dvoa' or off/def columns. Headers: {list(dvoa.columns)}")
        dvoa["tot_dvoa"] = (
                pd.to_numeric(dvoa[off_col].astype(str).str.replace("%", "", regex=False), errors="coerce")
                - pd.to_numeric(dvoa[def_col].astype(str).str.replace("%", "", regex=False), errors="coerce")
        )

    # clean values & keep only what we need
    dvoa["team"] = dvoa["team"].astype(str).str.upper().str.strip()
    dvoa["tot_dvoa"] = pd.to_numeric(dvoa["tot_dvoa"].astype(str).str.replace("%", "", regex=False), errors="coerce")
    dvoa = dvoa[["team", "tot_dvoa"]]

    sched = read_csv_safe(SCHED_PATH.as_posix())
    for c in ("team", "opponent"):
        if c not in sched.columns:
            raise ValueError(f"Schedule missing '{c}' column.")
        sched[c] = sched[c].astype(str).str.upper().str.strip()

    # merge team and opponent totals
    sched = sched.merge(dvoa.rename(columns={"team": "team"}),
                        on="team", how="left").rename(columns={"tot_dvoa": "team_tot_dvoa"})
    sched = sched.merge(dvoa.rename(columns={"team": "opponent"}),
                        left_on="opponent", right_on="opponent", how="left").rename(columns={"tot_dvoa": "opp_tot_dvoa"})

    # compute gap (team - opponent); NaNs → 0
    sched["dvoa_gap"] = (sched["team_tot_dvoa"].fillna(0) - sched["opp_tot_dvoa"].fillna(0)).astype(float)

    snapshot_csv(SCHED_PATH, suffix="prewrite")
    write_csv_atomic(sched, SCHED_PATH)
    print(f"✅ DVOA merged and dvoa_gap computed → {SCHED_PATH}")

if __name__ == "__main__":
    main()
