
# ===============================
# File: scripts/survivor_week_shortlist.py
# ===============================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
survivor_week_shortlist — quick top‑N by win probability
-------------------------------------------------------
Purpose
- Read picks/survivor/survivor_roadmap.csv and output a short‑list of the
  strongest teams by `win_prob_team` for a given week.

Usage (PowerShell)
  python scripts/survivor_week_shortlist.py \
    --roadmap picks/survivor/survivor_roadmap.csv \
    --week 1 \
    --top 12 \
    --out exports/survivor_week01_shortlist.csv
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser(description="Produce a Survivor short‑list for a given week")
    ap.add_argument("--roadmap", required=True)
    ap.add_argument("--week", type=int, default=None)
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--top", type=int, default=12)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.roadmap)
    # Normalize basic columns if present
    for c in ("team", "opponent"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper().str.strip()

    if args.season is not None and "season" in df.columns:
        df = df[df["season"] == args.season]
    if args.week is not None and "week" in df.columns:
        df = df[df["week"] == args.week]

    if "win_prob_team" not in df.columns:
        raise SystemExit("win_prob_team not found. Run fetch_nfl_odds to populate it.")

    cols = [c for c in ("season","week","team","opponent","current_ml_team","win_prob_team") if c in df.columns]
    out = df.sort_values("win_prob_team", ascending=False)[cols].head(args.top).reset_index(drop=True)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Short‑list written → {args.out} (rows={len(out)})")

if __name__ == "__main__":
    main()
