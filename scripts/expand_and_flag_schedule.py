# scripts/expand_and_flag_schedule.py
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

# repo root
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SURV_DIR = ROOT / "picks" / "survivor"

CLEAN_SCHEDULE = DATA_DIR / "2025_nfl_schedule_cleaned.csv"
# 👇 write ONLY to this staging file, never to the live roadmap
OUT_PATH = SURV_DIR / "survivor_schedule_roadmap_expanded.csv"

# io helpers (atomic write + snapshots)
from scripts.utils_io import snapshot_csv, write_csv_atomic

def load_clean() -> pd.DataFrame:
    df = pd.read_csv(CLEAN_SCHEDULE)
    # normalize keys
    for c in ("vistm", "hometm", "team", "opponent"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper().str.strip()
    return df

def expand_long(clean: pd.DataFrame) -> pd.DataFrame:
    """
    Accepts either long (team/opponent) or wide (vistm/hometm) schedule and returns long rows
    with: week, team, opponent, home_or_away
    """
    if {"team", "opponent"}.issubset(clean.columns):
        df = clean.copy()
        if "home_or_away" not in df.columns and {"vistm", "hometm"}.issubset(clean.columns):
            # prefer the provided long form if both exist
            pass
        df = df[["week", "team", "opponent"] + ([c for c in ["home_or_away"] if c in df.columns])]
        df["home_or_away"] = df.get("home_or_away", "").astype(str).str.capitalize()
        df.loc[~df["home_or_away"].isin(["Home", "Away"]), "home_or_away"] = ""
    elif {"vistm", "hometm"}.issubset(clean.columns):
        # build long from wide
        games = clean[["week", "date", "time", "vistm", "hometm"]].copy()
        home = games.assign(team=games["hometm"], opponent=games["vistm"], home_or_away="Home")
        away = games.assign(team=games["vistm"], opponent=games["hometm"], home_or_away="Away")
        df = pd.concat([home, away], ignore_index=True)
        # keep standard columns if present
        for c in ["date", "time"]:
            if c in df.columns:
                df[c] = df[c].astype(str)
        df = df[["week", "date", "time", "team", "opponent", "home_or_away"]]
    else:
        raise ValueError("Expected columns ('team','opponent') or ('vistm','hometm') in cleaned schedule.")
    df["week"] = pd.to_numeric(df["week"], errors="coerce").astype("Int64")
    return df

def add_holiday_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["holiday_flag"] = ""
    # 🔧 Use your actual rules; these mirror your earlier sample output
    df.loc[df["week"].eq(13) & df["team"].isin(["BAL","CIN","DAL","DET","GB","KC"]), "holiday_flag"] = "Thanksgiving"
    df.loc[df["week"].eq(17) & df["team"].isin(["DAL","DET","KC","MIN","WSH","DEN"]), "holiday_flag"] = "Christmas"
    return df

def main():
    ap = argparse.ArgumentParser(description="Expand schedule to long form and add holiday flags.")
    ap.add_argument("--write", action="store_true",
                    help="Actually write survivor_schedule_roadmap_expanded.csv (atomic). Otherwise just snapshot preview.")
    args = ap.parse_args()

    clean = load_clean()
    expanded = add_holiday_flags(expand_long(clean))

    print("\n[Weeks per team after expansion]:")
    print(expanded.groupby("team")["week"].count())

    sample = expanded.loc[expanded["holiday_flag"] != ""].head(12)
    if not sample.empty:
        print("\n[Sample holiday rows]:")
        cols = [c for c in ["week","team","opponent","home_or_away","holiday_flag"] if c in expanded.columns]
        print(sample[cols])

    # Always create a preview snapshot for inspection
    snap = snapshot_csv(OUT_PATH, suffix="pre_sync")  # snapshot existing OUT_PATH if present
    preview = SURV_DIR / "_snapshots" / f"survivor_schedule_roadmap_expanded_prewrite.csv"
    expanded.to_csv(preview, index=False)
    print(f"\n📄 Preview written: {preview}")

    if args.write:
        # Atomic overwrite of the staging file only
        write_csv_atomic(expanded, OUT_PATH)
        print(f"✅ Wrote schedule-expanded file: {OUT_PATH}")
    else:
        print("ℹ️ Skipped writing live file. Re-run with --write to update the staging CSV.")

if __name__ == "__main__":
    main()
