from pathlib import Path
import pandas as pd

from scripts.utils_read import read_csv_safe
from scripts.utils_guardrails import validate_clean_schedule, validate_roadmap

ROOT = Path(__file__).resolve().parents[1]
SCHED = ROOT / "data" / "2025_nfl_schedule_cleaned.csv"
DVOA  = ROOT / "data" / "dvoa_data.csv"
ROAD  = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

def check_exists(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing required file: {p}")

def main():
    # Exists?
    for p in (SCHED, DVOA, ROAD):
        check_exists(p)

    # Cleaned schedule validation
    sched = pd.read_csv(SCHED)
    s_errs = validate_clean_schedule(sched)
    print(f"[schedule] {SCHED}")
    if s_errs:
        print("  ❌ Issues:")
        for e in s_errs: print("   -", e)
        raise SystemExit(1)
    print("  ✅ OK")

    # Roadmap validation
    road = read_csv_safe(ROAD.as_posix())
    r_errs = validate_roadmap(road)
    print(f"[roadmap]  {ROAD}")
    if r_errs:
        print("  ❌ Issues:")
        for e in r_errs: print("   -", e)
        raise SystemExit(1)
    print("  ✅ OK")

    # DVOA quick sanity (columns only; detailed cleaning happens in utils_dvoa)
    dvoa = pd.read_csv(DVOA)
    need_any = {"TEAM","TOT DVOA"}  # minimal check
    print(f"[dvoa]     {DVOA}")
    if not need_any.issubset(set(dvoa.columns)):
        print(f"  ❌ Missing columns (need one of): {sorted(list(need_any))}")
        raise SystemExit(1)
    print("  ✅ OK (columns present)")



    print("\nAll core data validated. ✅")

if __name__ == "__main__":
    main()
