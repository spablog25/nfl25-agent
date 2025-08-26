# scripts/sync_and_validate.py
# --- path bootstrap (MUST be first) ---
# --- path bootstrap (MUST be first) ---
# --- path bootstrap: keep at very top ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ---------------------------------------

import pandas as pd

from scripts.utils_read import read_csv_safe
from scripts.utils_schema import coerce_roadmap_dtypes
from scripts.utils_guardrails import validate_clean_schedule, validate_roadmap
from scripts.utils_io import snapshot_csv, write_csv_atomic

SCHED = ROOT / "data" / "2025_nfl_schedule_cleaned.csv"
ROAD  = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

def build_schedule_long(sched: pd.DataFrame) -> pd.DataFrame:
    s = sched.copy()
    s.columns = s.columns.str.lower()

    home = pd.DataFrame({
        "week": s["week"],
        "team": s["hometm"].astype(str).str.upper().str.strip(),
        "opponent": s["vistm"].astype(str).str.upper().str.strip(),
        "home_or_away": "Home",
        "date": s["date"],
        "time": s["time"],
    })
    away = pd.DataFrame({
        "week": s["week"],
        "team": s["vistm"].astype(str).str.upper().str.strip(),
        "opponent": s["hometm"].astype(str).str.upper().str.strip(),
        "home_or_away": "Away",
        "date": s["date"],
        "time": s["time"],
    })
    out = pd.concat([home, away], ignore_index=True)
    out["week"] = pd.to_numeric(out["week"], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["time"] = out["time"].astype(str)
    return out

def main():
    if not SCHED.exists():
        raise FileNotFoundError(f"Missing schedule: {SCHED}")
    if not ROAD.exists():
        raise FileNotFoundError(f"Missing roadmap: {ROAD}")

    sched = pd.read_csv(SCHED)
    s_errs = validate_clean_schedule(sched)
    if s_errs:
        raise ValueError("Schedule invalid:\n- " + "\n- ".join(s_errs))
    sched_long = build_schedule_long(sched)

    road = read_csv_safe(ROAD.as_posix())
    road = coerce_roadmap_dtypes(road)

    road["team"] = road["team"].astype(str).str.upper().str.strip()
    road["opponent"] = road["opponent"].astype(str).str.upper().str.strip()
    if "home_or_away" in road:
        road["home_or_away"] = road["home_or_away"].astype(str).str.strip().str.capitalize()

    key = ["week","team","opponent","home_or_away"]
    before_dates = road["date"].copy()
    before_times = road["time"].copy()

    merged = road.merge(
        sched_long[key + ["date","time"]],
        on=key, how="left", suffixes=("","_sched")
    )
    unmatched_mask = merged["date_sched"].isna()
    n_unmatched = int(unmatched_mask.sum())

    merged["date"] = merged["date_sched"].fillna(merged["date"])
    merged["time"] = merged["time_sched"].fillna(merged["time"])

    updated_dates = int((merged["date"] != before_dates).sum())
    updated_times = int((merged["time"] != before_times).sum())

    out = merged.drop(columns=[c for c in ["date_sched","time_sched"] if c in merged.columns])
    out = coerce_roadmap_dtypes(out)

    r_errs = validate_roadmap(out)
    if r_errs:
        raise ValueError("Refusing to write corrupted roadmap:\n- " + "\n- ".join(r_errs))

    snapshot_csv(ROAD, suffix="pre_sync")
    write_csv_atomic(out, ROAD)

    print(f"Matched rows: {len(out) - n_unmatched} / {len(out)}  |  Unmatched: {n_unmatched}")
    print(f"Refreshed from schedule → dates: {updated_dates}, times: {updated_times}")
    if n_unmatched:
        sample = out.loc[unmatched_mask, key].head(10)
        print("Sample unmatched keys:")
        print(sample.to_string(index=False))
    print(f"✅ Roadmap synced and validated: {ROAD}")

if __name__ == "__main__":
    main()
