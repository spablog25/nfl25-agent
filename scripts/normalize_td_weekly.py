#!/usr/bin/env python3
"""
normalize_td_weekly.py

Reads data/td_digits/td_digits_weekly_{season}.csv (wide format) and produces:
1) Long table with ranks per week (week,digit,count,rank)
2) Winners table (week, first, second, third) with deterministic tie‑breaks:
   - Sort by count DESC, then digit ASC.
Outputs under data/td_digits/.
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_DIR = ROOT / "data" / "td_digits"
OUT_DIR = IN_DIR

def load_weekly(season: int) -> pd.DataFrame:
    path = IN_DIR / f"td_digits_weekly_{season}.csv"
    if not path.exists():
        raise SystemExit(f"Missing {path}. Run generate_td_digits.py first.")
    df = pd.read_csv(path)
    # Drop TOTAL row if present
    df = df[df["week"] != "TOTAL"].copy()
    # coerce week to int
    df["week"] = pd.to_numeric(df["week"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["week"]).copy()
    df["week"] = df["week"].astype(int)
    # keep only digit_0..digit_9
    digit_cols = [c for c in df.columns if c.startswith("digit_")]
    df = df[["week"] + digit_cols].sort_values("week")
    return df

def main():
    ap = argparse.ArgumentParser(description="Normalize weekly TD digits and produce winners.")
    ap.add_argument("--season", type=int, required=True)
    args = ap.parse_args()

    wide = load_weekly(args.season)
    digit_cols = [f"digit_{d}" for d in range(10)]
    # long format
    long = wide.melt(id_vars=["week"], value_vars=digit_cols, var_name="digit_col", value_name="count")
    long["digit"] = long["digit_col"].str.replace("digit_", "", regex=False).astype(int)
    long = long.drop(columns=["digit_col"])

    # rank within each week: higher count -> rank 1; ties broken by lower digit (asc)
    long = long.sort_values(["week", "count", "digit"], ascending=[True, False, True])
    long["rank"] = long.groupby("week")["count"].rank(method="dense", ascending=False).astype(int)

    # winners table (top 3)
    winners = (
        long[long["rank"] <= 3]
        .sort_values(["week", "rank", "digit"])
        .groupby("week")
        .apply(lambda g: pd.Series({
            "first": int(g.loc[g["rank"]==1, "digit"].iloc[0]),
            "second": int(g.loc[g["rank"]==2, "digit"].iloc[0]) if (g["rank"]==2).any() else None,
            "third": int(g.loc[g["rank"]==3, "digit"].iloc[0]) if (g["rank"]==3).any() else None,
        }))
        .reset_index()
    )

    # save
    long_out = OUT_DIR / f"td_digits_weekly_long_{args.season}.csv"
    winners_out = OUT_DIR / f"td_weekly_winners_{args.season}.csv"
    long.to_csv(long_out, index=False)
    winners.to_csv(winners_out, index=False)

    print(f"Saved long table  → {long_out.as_posix()}")
    print(f"Saved winners     → {winners_out.as_posix()}")

if __name__ == "__main__":
    main()
