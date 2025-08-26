#!/usr/bin/env python3
"""
Survivor BG Likes — Y/N Matrix Utilities
---------------------------------------
This script does two simple, reliable things for us:

1) **Recompute cross weeks + like counts** from a Y/N matrix
   Input: survivor_bg_likes_YN.csv (teams across rows, weeks across columns)
   Output: survivor_bg_likes_YN_enriched.csv (adds Cross_Weeks + Likes_N)

2) **(Optional) Build a Y/N matrix from a raw long list**
   Input: survivor_bg_likes_long.csv with columns [team, week]
   Output: survivor_bg_likes_YN.csv

Notes for beginners:
- Weeks must be one of: W1..W16, TG, CH
- Team codes are standard: ARI, ATL, BAL, ..., WSH
- You can edit the Y/N CSV by hand during the season; just rerun this script
  to refresh Cross_Weeks and Likes_N.

Usage:
  # 1) Recompute Cross_Weeks + Likes_N for an existing Y/N file
  python survivor_bg_likes_y_n.py --yn data/survivor_bg_likes_YN.csv

  # 2) Build a Y/N matrix from a long list, then enrich it
  python survivor_bg_likes_y_n.py --long data/survivor_bg_likes_long.csv \
                                  --out-yn data/survivor_bg_likes_YN.csv
  python survivor_bg_likes_y_n.py --yn data/survivor_bg_likes_YN.csv
"""

import argparse
import pandas as pd
from pathlib import Path

ALL_TEAMS = [
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB",
    "HOU","IND","JAX","KC","LAC","LAR","LV","MIA","MIN","NE","NO","NYG","NYJ",
    "PHI","PIT","SEA","SF","TB","TEN","WSH"
]
WEEKS = [f"W{i}" for i in range(1,17)] + ["TG","CH"]


def build_from_long(long_csv: Path, out_yn: Path):
    df = pd.read_csv(long_csv, dtype=str).fillna("")
    df["team"] = df["team"].str.upper().str.strip()
    df["week"] = df["week"].str.upper().str.strip()

    # Start a clean Y/N matrix
    base = {"team": ALL_TEAMS}
    for w in WEEKS:
        base[w] = ["N"] * len(ALL_TEAMS)
    yn = pd.DataFrame(base)

    # Mark likes
    for _, r in df.iterrows():
        t = r["team"]; w = r["week"]
        if t in ALL_TEAMS and w in WEEKS:
            yn.loc[yn["team"] == t, w] = "Y"
    yn.to_csv(out_yn, index=False)
    print("Saved Y/N →", out_yn)


def enrich_yn(yn_csv: Path, out_csv: Path|None=None):
    yn = pd.read_csv(yn_csv, dtype=str).fillna("")
    # Ensure week columns exist (if user deletes a column by accident)
    for w in WEEKS:
        if w not in yn.columns:
            yn[w] = "N"
    # Ensure all teams are present
    missing_teams = [t for t in ALL_TEAMS if t not in yn["team"].values]
    if missing_teams:
        for t in missing_teams:
            row = {"team": t} | {w: "N" for w in WEEKS}
            yn = pd.concat([yn, pd.DataFrame([row])], ignore_index=True)

    # Cross_Weeks and Likes_N
    def cross_weeks(row):
        used = [w for w in WEEKS if str(row[w]).strip().upper() == "Y"]
        display = [(w[1:] if w.startswith("W") else w) for w in used]
        return ",".join(display)

    yn["Cross_Weeks"] = yn.apply(cross_weeks, axis=1)
    yn["Likes_N"] = yn[WEEKS].apply(lambda r: (r.astype(str).str.upper()=="Y").sum(), axis=1)

    out_csv = out_csv or yn_csv.with_name(yn_csv.stem + "_enriched.csv")
    yn.to_csv(out_csv, index=False)
    print("Saved enriched →", out_csv)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--yn", type=Path, help="Path to survivor_bg_likes_YN.csv to enrich")
    ap.add_argument("--long", type=Path, help="Optional long CSV with columns [team, week] to build Y/N")
    ap.add_argument("--out-yn", type=Path, help="Where to save the built Y/N from --long")
    args = ap.parse_args()

    if args.long:
        if not args.out_yn:
            raise SystemExit("--out-yn is required when using --long")
        build_from_long(args.long, args.out_yn)

    if args.yn:
        enrich_yn(args.yn)

if __name__ == "__main__":
    main()
