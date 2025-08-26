#!/usr/bin/env python3
"""
Seed + Enrich Y/N Matrix for Sean's Survivor Likes
--------------------------------------------------
This script preloads the current likes from the canvas doc ("Sean Survivor Likes")
so you can generate an editable Y/N matrix, and then enrich it with Cross_Weeks
and Likes_N. You can also merge the seed with an existing file.

Beginner‑friendly usage (from repo root):

  # 1) Create a brand‑new Y/N file pre‑seeded with Sean's likes
  python scripts/survivor_bg_likes_seed.py --seed-out data/survivor_bg_likes_YN.csv

  # 2) Enrich it (adds Cross_Weeks + Likes_N)
  python scripts/survivor_bg_likes_seed.py --yn data/survivor_bg_likes_YN.csv

  # (Optional) Overlay the seed ONTO an existing file (doesn't erase your edits)
  python scripts/survivor_bg_likes_seed.py --seed-out data/survivor_bg_likes_YN.csv --merge data/existing.csv

Notes
- Weeks include W1..W16 plus TG (Thanksgiving) and CH (Christmas).
- Team codes use standard NFL abbreviations (e.g., WSH, LAR, JAX).
- The seed below is taken from the bold/first team in each bullet of your canvas doc.
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

ALL_TEAMS = [
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB",
    "HOU","IND","JAX","KC","LAC","LAR","LV","MIA","MIN","NE","NO","NYG","NYJ",
    "PHI","PIT","SEA","SF","TB","TEN","WSH"
]
WEEKS = [f"W{i}" for i in range(1,17)] + ["TG","CH"]
ORDER = {**{f"W{i}": i for i in range(1,17)}, "TG": 12.5, "CH": 16.5}

# ---- SEED from "Sean Survivor Likes" (bolded/first team per bullet) ----
SEAN_SEED: dict[str, list[str]] = {
    # Week 1
    "PHI": ["W1","W6","TG","W15"],
    "ARI": ["W1","W5"],
    "DEN": ["W1","W6","W7","W10"],
    # Week 2
    "LAR": ["W2","W9"],
    "CIN": ["W2","W8","W12"],
    "BAL": ["W2","W5","W8","W12","W14","W16"],
    "BUF": ["W2","W3","W4","W5","W8"],
    "SF":  ["W2","W4","W7","W12","W13","W15"],
    # Week 3
    "TB":  ["W3","W8","W10","W14","W15"],
    "GB":  ["W3","W9"],
    "SEA": ["W3"],
    "KC":  ["W3","W12","TG","W16"],
    # Week 4
    "HOU": ["W4"],
    "DET": ["W4","TG","W16","CH"],
    # Week 5
    "MIN": ["W5"],
    # Week 6
    "PIT": ["W6"],
    "LV":  ["W6"],
    "NE":  ["W6","W11"],
    "CHI": ["W7"],
    # Chargers multi‑week likes
    "LAC": ["W7","W9","W10","W11","W13"],
    # Remaining singles
    "MIA": ["W13"],
    "WSH": ["CH"],
}

# Ensure all teams are present in the seed with default [] (no likes)
for team in ALL_TEAMS:
    SEAN_SEED.setdefault(team, [])


def _blank_matrix() -> pd.DataFrame:
    base = {"team": ALL_TEAMS}
    for w in WEEKS:
        base[w] = ["N"] * len(ALL_TEAMS)
    return pd.DataFrame(base)


def seed_to_matrix() -> pd.DataFrame:
    df = _blank_matrix()
    for team, weeks in SEAN_SEED.items():
        for w in weeks:
            if w in WEEKS:
                df.loc[df.team == team, w] = "Y"
    return df


def merge_seed_onto(existing_csv: Path) -> pd.DataFrame:
    """Overlay the seed Y's onto an existing Y/N file without clearing other Y's."""
    existing = pd.read_csv(existing_csv, dtype=str).fillna("")
    existing.columns = [c.strip() for c in existing.columns]
    # Ensure core schema
    if "team" not in existing.columns:
        raise SystemExit("Existing file must have a 'team' column")
    for w in WEEKS:
        if w not in existing.columns:
            existing[w] = "N"
    # Ensure all teams present
    missing = [t for t in ALL_TEAMS if t not in existing["team"].str.upper().tolist()]
    if missing:
        extra = pd.DataFrame({"team": missing} | {w: "N" for w in WEEKS})
        existing = pd.concat([existing, extra], ignore_index=True)
    # Normalize team codes
    existing["team"] = existing["team"].str.upper().str.strip()
    # Overlay Y's from seed
    for team, weeks in SEAN_SEED.items():
        for w in weeks:
            existing.loc[existing.team == team, w] = "Y"
    return existing


def enrich_yn(yn_csv: Path, out_csv: Path|None=None) -> Path:
    yn = pd.read_csv(yn_csv, dtype=str).fillna("")
    # Guard columns/teams
    for w in WEEKS:
        if w not in yn.columns:
            yn[w] = "N"
    missing_teams = [t for t in ALL_TEAMS if t not in yn["team"].str.upper().tolist()]
    if missing_teams:
        yn = pd.concat([yn, pd.DataFrame({"team": missing_teams} | {w: "N" for w in WEEKS})], ignore_index=True)
    yn["team"] = yn["team"].str.upper().str.strip()

    # Cross_Weeks (ordered) and Likes_N
    def cross_weeks(row):
        used = [w for w in WEEKS if str(row[w]).strip().upper() == "Y"]
        used_sorted = sorted(used, key=lambda x: ORDER.get(x, 999))
        tokens = [(w[1:] if w.startswith("W") else w) for w in used_sorted]
        return ",".join(tokens)

    yn["Cross_Weeks"] = yn.apply(cross_weeks, axis=1)
    yn["Likes_N"] = yn[WEEKS].apply(lambda r: (r.astype(str).str.upper()=="Y").sum(), axis=1)

    out_csv = out_csv or yn_csv.with_name(yn_csv.stem + "_enriched.csv")
    yn.to_csv(out_csv, index=False)
    return out_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed-out", type=Path, help="Write a new Y/N CSV from the embedded Sean seed")
    ap.add_argument("--merge", type=Path, help="Existing Y/N CSV to overlay with the seed (does not erase other Y's)")
    ap.add_argument("--yn", type=Path, help="Enrich a Y/N CSV (adds Cross_Weeks + Likes_N)")
    args = ap.parse_args()

    if args.seed_out and args.merge:
        # Overlay onto existing
        merged = merge_seed_onto(args.merge)
        merged.to_csv(args.seed_out, index=False)
        print(f"Seed overlaid onto existing → {args.seed_out}")

    elif args.seed_out:
        seeded = seed_to_matrix()
        args.seed_out.parent.mkdir(parents=True, exist_ok=True)
        seeded.to_csv(args.seed_out, index=False)
        print(f"Seeded Y/N written → {args.seed_out}")

    if args.yn:
        out = enrich_yn(args.yn)
        print(f"Enriched file → {out}")

if __name__ == "__main__":
    main()
