#!/usr/bin/env python3
"""
Convert an Excel staging sheet to the exact CSV used by the Survivor generator:
  picks/survivor/survivor_schedule_roadmap_expanded.csv

Usage (from repo root):
  python -m scripts.xlsx_to_staging_csv --xlsx path/to/staging.xlsx \
    --out picks/survivor/survivor_schedule_roadmap_expanded.csv

Optional:
  --sheet SHEETNAME              # use a specific tab; default = first sheet
  --schedule path/to/clean.csv   # defaults to data/2025_nfl_schedule_cleaned.csv
  --no-infer-opponent            # require 'opponent' in Excel instead of inferring

What it does:
- Reads the Excel file (optionally a specific sheet)
- Normalizes column names (case-insensitive) and maps common synonyms
- Coerces types, UPPERCASEs team/opponent, ensures week is Int
- Infers opponent & home_or_away from the cleaned schedule if Excel lacks them
- Computes dvoa_gap if missing but team/opp DVOA exist
- Drops duplicate (week,team,opponent) rows, keeping the last
- Writes the CSV atomically

Accepted columns (case-insensitive; synonyms in parentheses):
- week (wk)
- team
- opponent (opp, opp_team)  ← optional; inferred if missing
- date (game_date)
- time (kickoff, kickoff_time)
- home_or_away (homeaway, hoa, venue[Home/Away])  ← inferred if missing
- projected_win_prob (win_prob, wp, proj_wp, projected_wp)
- team_tot_dvoa (team_dvoa, dvoa_team, tot_dvoa)
- opp_tot_dvoa (opp_dvoa, dvoa_opp)
- dvoa_gap (auto-computed if missing)
- rating_gap (power_gap, power_diff, pr_gap)
- injury_adjustment (inj_adj, injury_adj)
- future_scarcity_bonus (scarcity_bonus, scarcity)
"""
from __future__ import annotations
import argparse
from pathlib import Path
import sys
import pandas as pd

# --- repo bootstrap ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.utils_io import write_csv_atomic
from scripts.utils_read import read_csv_safe

SURV_DIR = ROOT / "picks" / "survivor"
DATA_DIR = ROOT / "data"
DEFAULT_OUT = SURV_DIR / "survivor_schedule_roadmap_expanded.csv"
DEFAULT_SCHEDULE = DATA_DIR / "2025_nfl_schedule_cleaned.csv"

# canonical names we aim for
CANON = {
    "week": "week",
    "team": "team",
    "opponent": "opponent",
    "date": "date",
    "time": "time",
    "home_or_away": "home_or_away",
    "projected_win_prob": "projected_win_prob",
    "team_tot_dvoa": "team_tot_dvoa",
    "opp_tot_dvoa": "opp_tot_dvoa",
    "dvoa_gap": "dvoa_gap",
    "rating_gap": "rating_gap",
    "injury_adjustment": "injury_adjustment",
    "future_scarcity_bonus": "future_scarcity_bonus",
}

# synonyms (lowercased) → canonical
SYNONYMS = {
    # keys
    "wk": "week",
    "opp": "opponent",
    "opp_team": "opponent",
    # date/time
    "game_date": "date",
    "kickoff": "time",
    "kickoff_time": "time",
    # venue
    "homeaway": "home_or_away",
    "hoa": "home_or_away",
    "hoa_venue": "home_or_away",
    "venue": "home_or_away",  # expects Home/Away values
    # win prob
    "win_prob": "projected_win_prob",
    "wp": "projected_win_prob",
    "proj_wp": "projected_win_prob",
    "projected_wp": "projected_win_prob",
    # dvoa
    "team_dvoa": "team_tot_dvoa",
    "dvoa_team": "team_tot_dvoa",
    "tot_dvoa": "team_tot_dvoa",
    "opp_dvoa": "opp_tot_dvoa",
    "dvoa_opp": "opp_tot_dvoa",
    # gaps / adjustments
    "power_gap": "rating_gap",
    "power_diff": "rating_gap",
    "pr_gap": "rating_gap",
    "inj_adj": "injury_adjustment",
    "injury_adj": "injury_adjustment",
    "scarcity_bonus": "future_scarcity_bonus",
    "scarcity": "future_scarcity_bonus",
}

OPTIONAL = [
    "date","time","home_or_away","projected_win_prob",
    "team_tot_dvoa","opp_tot_dvoa","dvoa_gap","rating_gap",
    "injury_adjustment","future_scarcity_bonus"
]


def normalize_columns(cols: list[str]) -> list[str]:
    out = []
    for c in cols:
        lc = c.strip().lower()
        if lc in CANON:
            out.append(CANON[lc])
        elif lc in SYNONYMS:
            out.append(SYNONYMS[lc])
        else:
            out.append(lc)  # keep unknowns (will be appended as-is)
    return out


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    # teams upper, strip spaces
    for col in ("team","opponent"):
        if col in d:
            d[col] = d[col].astype(str).str.upper().str.strip()
    # week to Int64
    if "week" in d:
        d["week"] = pd.to_numeric(d["week"], errors="coerce").astype("Int64")
    # date/time to string (format dates to m/d/YYYY if datetime)
    if "date" in d:
        if pd.api.types.is_datetime64_any_dtype(d["date"]):
            d["date"] = d["date"].dt.strftime("%m/%d/%Y")
        else:
            d["date"] = d["date"].astype(str)
    if "time" in d:
        if pd.api.types.is_datetime64_any_dtype(d["time"]):
            d["time"] = d["time"].dt.strftime("%I:%M %p").str.lstrip("0")
        else:
            d["time"] = d["time"].astype(str)
    # home_or_away normalized
    if "home_or_away" in d:
        d["home_or_away"] = d["home_or_away"].astype(str).str.strip().str.capitalize()
        d.loc[~d["home_or_away"].isin(["Home","Away"]), "home_or_away"] = ""
    # numeric floats
    for c in ["projected_win_prob","team_tot_dvoa","opp_tot_dvoa","dvoa_gap","rating_gap","injury_adjustment","future_scarcity_bonus"]:
        if c in d:
            d[c] = pd.to_numeric(d[c], errors="coerce").astype("Float64")
    # compute dvoa_gap if missing
    if "dvoa_gap" in d and d["dvoa_gap"].isna().all() and {"team_tot_dvoa","opp_tot_dvoa"}.issubset(d.columns):
        d["dvoa_gap"] = (d["team_tot_dvoa"] - d["opp_tot_dvoa"]).astype("Float64")
    return d


def build_long_schedule(schedule_path: Path) -> pd.DataFrame:
    if not schedule_path.exists():
        raise FileNotFoundError(f"Schedule not found: {schedule_path}")
    raw = read_csv_safe(schedule_path.as_posix())  # columns lowercased
    # normalize to long format
    if {"team", "opponent"}.issubset(raw.columns):
        sched = raw.copy()
    elif {"vistm", "hometm"}.issubset(raw.columns):
        games = raw[["week", "date", "time", "vistm", "hometm"]].copy()
        home_rows = games.assign(team=games["hometm"], opponent=games["vistm"], home_or_away="Home")
        away_rows = games.assign(team=games["vistm"], opponent=games["hometm"], home_or_away="Away")
        sched = pd.concat([home_rows, away_rows], ignore_index=True)
        sched = sched[["week", "date", "time", "team", "opponent", "home_or_away"]]
    else:
        raise ValueError("Clean schedule must have either (team,opponent) or (vistm,hometm).")
    sched["team"] = sched["team"].astype(str).str.upper().str.strip()
    sched["opponent"] = sched["opponent"].astype(str).str.upper().str.strip()
    sched["week"] = pd.to_numeric(sched["week"], errors="coerce").astype("Int64")
    return sched


def infer_from_schedule(df: pd.DataFrame, sched_long: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    # if opponent missing, fill from schedule by (week, team)
    need_opp = "opponent" not in d.columns or d["opponent"].isna().any()
    if need_opp:
        d = d.merge(
            sched_long[["week","team","opponent","home_or_away","date","time"]],
            on=["week","team"], how="left", suffixes=("", "_sched")
        )
        # take schedule values only when Excel lacks them
        if "opponent" in d.columns:
            d["opponent"] = d["opponent"].where(d["opponent"].notna() & (d["opponent"].astype(str).str.len()>0), d["opponent_sched"])
        else:
            d["opponent"] = d["opponent_sched"]
        if "home_or_away" in d.columns:
            d["home_or_away"] = d["home_or_away"].where(d["home_or_away"].isin(["Home","Away"]), d["home_or_away_sched"])
        else:
            d["home_or_away"] = d["home_or_away_sched"]
        # fill date/time if missing
        if "date" in d.columns:
            d["date"] = d["date"].where(d["date"].astype(str).str.len()>0, d["date_sched"])
        else:
            d["date"] = d["date_sched"]
        if "time" in d.columns:
            d["time"] = d["time"].where(d["time"].astype(str).str.len()>0, d["time_sched"])
        else:
            d["time"] = d["time_sched"]
        # drop helper cols
        dropcols = [c for c in d.columns if c.endswith("_sched")]
        d = d.drop(columns=dropcols)
    return d


def main():
    ap = argparse.ArgumentParser(description="Convert Excel staging to Survivor staging CSV")
    ap.add_argument("--xlsx", required=True, type=Path)
    ap.add_argument("--sheet", type=str, default=None, help="Excel sheet name (default: first sheet)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--schedule", type=Path, default=DEFAULT_SCHEDULE,
                    help="Clean schedule CSV used to infer opponent/home_or_away when missing")
    ap.add_argument("--infer-opponent", dest="infer_opponent", action="store_true", default=True)
    ap.add_argument("--no-infer-opponent", dest="infer_opponent", action="store_false")
    args = ap.parse_args()

    if not args.xlsx.exists():
        raise FileNotFoundError(f"Excel not found: {args.xlsx}")

    # Read Excel (force first sheet if no sheet name provided)
    sheet_to_read = args.sheet if args.sheet is not None else 0
    df = pd.read_excel(args.xlsx, sheet_name=sheet_to_read)

    # If the result is a dict (multiple sheets), pick the first one
    if isinstance(df, dict):
        first_key = next(iter(df))
        print(f"[INFO] Multiple sheets detected, using first: {first_key}")
        df = df[first_key]

    if df.empty:
        raise ValueError("Excel sheet appears empty.")

    # Normalize columns → canonical
    df.columns = normalize_columns(list(df.columns))

    # Coerce basic types
    df = coerce_types(df)

    # If opponent/home_or_away missing and inference enabled, fill from schedule
    inferred = 0
    if args.infer_opponent and ("opponent" not in df.columns or df["opponent"].isna().any()):
        sched_long = build_long_schedule(args.schedule)
        before = df["opponent"].notna().sum() if "opponent" in df.columns else 0
        df = infer_from_schedule(df, sched_long)
        after = df["opponent"].notna().sum()
        inferred = max(0, after - before)

    # Basic checks after inference
    missing_keys = [k for k in ["week","team","opponent"] if k not in df.columns]
    if missing_keys:
        raise ValueError(f"Missing required columns after inference: {missing_keys} | Present: {list(df.columns)}")

    # Drop duplicates, keep last (assume last edit is most recent)
    df = df.drop_duplicates(subset=["week","team","opponent"], keep="last").reset_index(drop=True)

    # Sort for readability
    sort_cols = [c for c in ["week","team","opponent"] if c in df.columns]
    df = df.sort_values(sort_cols)

    # Write atomically
    args.out.parent.mkdir(parents=True, exist_ok=True)
    write_csv_atomic(df, args.out)

    # Summary
    filled = {c: int(df[c].notna().sum()) for c in OPTIONAL if c in df.columns}
    print(f"Wrote staging CSV → {args.out}")
    print("Rows:", len(df))
    print("Filled counts:")
    for k, v in filled.items():
        print(f"  {k:>22}: {v}")
    print(f"Opponents inferred: {inferred}")


if __name__ == "__main__":
    main()
