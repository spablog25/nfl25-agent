#!/usr/bin/env python3
"""
rank_td_digits.py

Reads data/td_digits/td_digits_season_totals.csv and produces a per‑year ranking
of digits 0–9 (1 = most TDs in that season). Outputs a CSV matrix and prints a
nice console table, plus the top 3 digits each year.

Usage (from project root):
  python scripts/rank_td_digits.py --start 2015 --end 2024
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
IN_PATH = PROJECT_ROOT / "data" / "td_digits" / "td_digits_season_totals.csv"
OUT_DIR = PROJECT_ROOT / "data" / "td_digits"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def competition_rank(series: pd.Series) -> pd.Series:
    """
    Standard competition ranking: highest value gets rank 1;
    ties share rank, and next rank is skipped appropriately.
    """
    # sort descending, rank method='min' gives competition style (1,2,2,4)
    return (-series).rank(method="min").astype(int)

def main():
    ap = argparse.ArgumentParser(description="Rank jersey last-digit TD totals per year.")
    ap.add_argument("--start", type=int, default=2015)
    ap.add_argument("--end", type=int, default=2024)
    args = ap.parse_args()

    if not IN_PATH.exists():
        raise SystemExit(f"Missing {IN_PATH}. Run generate_td_digits.py for those seasons first.")

    df = pd.read_csv(IN_PATH)
    df = df[(df["season"] >= args.start) & (df["season"] <= args.end)].copy()
    if df.empty:
        raise SystemExit("No seasons in requested range.")

    # Ensure columns digit_0..digit_9
    digit_cols = [f"digit_{d}" for d in range(10)]
    for c in digit_cols:
        if c not in df.columns:
            df[c] = 0

    # Build matrix: rows=digit (0..9), cols=years, values=counts
    counts = df.set_index("season")[digit_cols].T
    counts.index = counts.index.str.replace("digit_", "", regex=False).astype(int)
    counts = counts.reindex(index=range(10), fill_value=0).sort_index()
    counts = counts.sort_index(axis=1)  # years ascending

    # Convert to ranks per column (per year)
    ranks = counts.copy()
    for col in ranks.columns:
        ranks[col] = competition_rank(counts[col])

    # Save ranks matrix
    out_csv = OUT_DIR / f"td_digits_ranks_{args.start}_{args.end}.csv"
    ranks.to_csv(out_csv, index_label="digit")
    print(f"Saved ranks → {out_csv.as_posix()}")

    # Console display
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print("\nTD Digit Rankings — (rows=digit 0–9, cols=year; 1 = most TDs)\n")
        print(ranks)

    # Also print top 3 “podium” per year
    print("\nPodium per year (Top 3 digits):")
    for year in ranks.columns:
        # digits with rank 1, 2, 3 (handle ties)
        podium = []
        for r in (1, 2, 3):
            ds = ranks.index[ranks[year] == r].tolist()
            podium.append(f"{r}: {', '.join(map(str, ds)) if ds else '—'}")
        print(f"{year}:  " + " | ".join(podium))

if __name__ == "__main__":
    main()
