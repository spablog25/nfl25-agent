# ===============================
# File: scripts/compute_line_deltas.py
# ===============================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
compute_line_deltas — derive open_* and line deltas from snapshots
-----------------------------------------------------------------
Purpose
- Read current odds CSV + all files in data/odds/snapshots/.
- For each game (by home/away team key), pick the **earliest** snapshot as "open".
- Produce a merged CSV with `open_spread_home`, `open_spread_away`, `open_total`, and `line_delta_home`.
- Optionally update the Millions planner with these opens/deltas.

Usage (PowerShell)
  python scripts/compute_line_deltas.py \
    --current data/odds/season_all.csv \
    --snapshots_dir data/odds/snapshots \
    --out data/odds/season_with_opens.csv \
    --update_planner picks/millions/millions_planner.csv \
    --backup 1
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import List
import re

import pandas as pd


def make_key(a: str, b: str) -> str:
    return "|".join(sorted([(a or "").upper(), (b or "").upper()]))


def parse_stamp(name: str) -> str:
    # Accept names like 2025-08-20T0200Z.csv → 2025-08-20T0200Z
    m = re.search(r"(\d{4}-\d{2}-\d{2}T\d{4}Z)", name)
    return m.group(1) if m else name


def earliest_snapshot(snap_dir: Path) -> pd.DataFrame:
    files = sorted([p for p in snap_dir.glob("*.csv")])
    if not files:
        raise SystemExit(f"No snapshot files in {snap_dir}")
    frames: List[pd.DataFrame] = []
    for p in files:
        df = pd.read_csv(p)
        df["__stamp"] = parse_stamp(p.name)
        frames.append(df)
    all_df = pd.concat(frames, ignore_index=True)
    all_df["__game_key"] = [make_key(h, a) for h, a in zip(all_df.get("home_team"), all_df.get("away_team"))]
    # sort by stamp ascending then take first per key
    all_df = all_df.sort_values(["__game_key", "__stamp"])  # lexicographic stamp ok
    first = all_df.groupby("__game_key", as_index=False).first()
    # rename opens
    opens = first[[
        "__game_key", "current_spread_home", "current_spread_away", "current_total"
    ]].rename(columns={
        "current_spread_home": "open_spread_home",
        "current_spread_away": "open_spread_away",
        "current_total": "open_total",
    })
    return opens


def update_planner_with_opens(pl_path: Path, opens: pd.DataFrame, backup: bool = True) -> None:
    pl = pd.read_csv(pl_path)
    pl["__game_key"] = [make_key(t, o) for t, o in zip(pl.get("team"), pl.get("opponent"))]
    merged = pl.merge(opens, on="__game_key", how="left")
    # Fill NaNs only
    for c in ("open_spread_home", "open_spread_away", "open_total"):
        if c in merged.columns:
            merged[c] = merged[c] if c in pl.columns else merged[c]
    # Derive open_line (team perspective) and line_delta_home
    if "home_or_away" in merged.columns:
        mask_home = merged["home_or_away"].astype(str).str.upper().eq("HOME")
        if "open_spread_home" in merged.columns and "open_spread_away" in merged.columns:
            merged["open_line"] = merged.get("open_line")
            merged.loc[mask_home, "open_line"] = merged.loc[mask_home, "open_spread_home"]
            merged.loc[~mask_home, "open_line"] = merged.loc[~mask_home, "open_spread_away"]
        if "current_spread_home" in merged.columns and "open_spread_home" in merged.columns:
            merged["line_delta_home"] = merged["current_spread_home"] - merged["open_spread_home"]
    if backup:
        bak = pl_path.with_suffix(pl_path.suffix + ".bak")
        pl.to_csv(bak, index=False)
        print(f"Planner backup → {bak}")
    # drop helper
    merged = merged.drop(columns=[c for c in merged.columns if c.startswith("__")], errors="ignore")
    merged.to_csv(pl_path, index=False)
    print(f"Planner updated with opens → {pl_path} (rows={len(merged)})")


def main():
    ap = argparse.ArgumentParser(description="Compute open_* and line deltas from snapshots + update planner")
    ap.add_argument("--current", required=True, help="Current odds CSV (e.g., data/odds/season_all.csv)")
    ap.add_argument("--snapshots_dir", default="data/odds/snapshots")
    ap.add_argument("--out", required=True, help="Output merged CSV with opens (e.g., data/odds/season_with_opens.csv)")
    ap.add_argument("--update_planner", default=None, help="Optionally update planner CSV in-place")
    ap.add_argument("--backup", type=int, default=1)
    args = ap.parse_args()

    cur = pd.read_csv(args.current)
    cur["__game_key"] = [make_key(h, a) for h, a in zip(cur.get("home_team"), cur.get("away_team"))]

    opens = earliest_snapshot(Path(args.snapshots_dir))
    out = cur.merge(opens, on="__game_key", how="left")

    # Compute line_delta_home where possible
    if "current_spread_home" in out.columns and "open_spread_home" in out.columns:
        out["line_delta_home"] = out["current_spread_home"] - out["open_spread_home"]

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Merged current + opens → {args.out} (rows={len(out)})")

    if args.update_planner:
        update_planner_with_opens(Path(args.update_planner), opens, backup=bool(args.backup))

if __name__ == "__main__":
    main()
