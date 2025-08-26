#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

# Columns we expect to exist in survivor_roadmap_expanded.csv
EXPECTED = [
    # keys & basics
    "week","date","time","team","opponent","home_or_away",
    # static
    "holiday_flag","rest_days",
    # planner flags / notes
    "reserved","is_locked_out","expected_avail",
    "preferred","must_use","save_for_later",
    "notes_future","notes","spot_quality",
    # inputs/metrics
    "projected_win_prob",
    # DVOA
    "team_tot_dvoa","opp_tot_dvoa","dvoa_gap",
    # outputs
    "spot_value","spot_value_score",
    # placeholders / dynamic inputs (kept even if blank)
    "moneyline","spread","implied_wp",
    "power_rating","opp_power_rating","power_gap",
    "rest_diff","travel_miles",
    "rating_gap","injury_adjustment","future_scarcity_bonus",
]


def main():
    ap = argparse.ArgumentParser(description="Validate Survivor roadmap columns & key uniqueness")
    ap.add_argument("--path", required=True, type=Path, help="Path to survivor_roadmap_expanded.csv")
    args = ap.parse_args()

    if not args.path.exists():
        raise FileNotFoundError(f"CSV not found: {args.path}")

    df = pd.read_csv(args.path)

    # Check columns
    missing = [c for c in EXPECTED if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED]

    # Key uniqueness on (week, team, opponent)
    key_cols = ["week","team","opponent"]
    if not set(key_cols).issubset(df.columns):
        print("Key columns missing; skipping duplicate check.")
        dups = None
    else:
        dups = int(df.duplicated(subset=key_cols).sum())

    print("--- Survivor Roadmap Validation ---")
    print(f"File: {args.path}")
    print(f"Rows: {len(df):,} | Columns: {len(df.columns):,}")
    print("Missing columns:", missing or "None")
    print("Extra columns:", extra or "None")
    if dups is not None:
        print(f"Duplicates on (week, team, opponent): {dups}")

    # Simple failure non-zero exit if problems
    if missing or (dups is not None and dups > 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
