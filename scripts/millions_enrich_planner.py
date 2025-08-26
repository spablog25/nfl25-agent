#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Millions A2 — Enrich Planner from Roadmap (with Totals) and optional Odds CSV
-----------------------------------------------------------------------------
Purpose
  • Merge the canonical roadmap fields into the team‑oriented planner
  • Bring in spreads, injuries/weather, kickoff text if present
  • NEW: carry totals (open/current/closing/circa) into the planner
  • Optional: join a normalized odds CSV to supply current_total when roadmap
    doesn’t have totals yet (your fetch_nfl_odds output)
  • Avoid *_x/*_y or .1 suffixes by dropping overlaps before merge when asked

Inputs
  --planner   picks/millions/millions_planner.csv  (team‑oriented)
  --roadmap   picks/millions/millions_roadmap_game.csv (game‑level)
  --odds      data/odds/millions_week1_odds.csv (optional, game‑level)
  --week      Restrict roadmap/odds to this week to keep keys unique

Usage (PowerShell)
  python scripts/millions_enrich_planner.py `
    --planner "picks/millions/millions_planner.csv" `
    --roadmap "picks/millions/millions_roadmap_game.csv" `
    --odds    "data/odds/millions_week1_odds.csv" `
    --week 1 `
    --drop_overlaps --backup

Notes
  • This script does not compute kickoff sort keys — that’s A3.
  • Safe to re‑run; it overwrites only the merged columns.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd

# Columns we prefer to carry from the roadmap when available
KEEP_FROM_ROADMAP: List[str] = [
    # schedule/context
    "kickoff_local", "venue",
    "rest_days_home", "rest_days_away", "rest_days_diff",
    # spreads (game‑level)
    "open_spread_home", "open_spread_away",
    "current_spread_home", "current_spread_away",
    "closing_spread_home", "closing_spread_away",
    # contest (Circa) spreads
    "circa_spread_home", "circa_spread_away",
    # off/def dvoa (game‑level per side)
    "off_dvoa_home", "off_dvoa_away", "def_dvoa_home", "def_dvoa_away",
    # NEW — totals (game‑level)
    "open_total", "current_total", "closing_total", "circa_total",
    # optional notes
    "injuries_key_home", "injuries_key_away", "weather_notes",
]

ALIASES = {"WAS": "WSH", "ARZ": "ARI", "LA": "LAR"}


def _norm(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()


def _detect_home_away_cols(df: pd.DataFrame) -> tuple[str, str]:
    for h, a in (("home_team", "away_team"), ("hometm", "vistm"), ("home", "away")):
        if h in df.columns and a in df.columns:
            return h, a
    raise SystemExit("Roadmap/Odds is missing home/away team columns (tried home_team/away_team, hometm/vistm, home/away)")


def _key_two(df: pd.DataFrame, a: str, b: str) -> pd.Series:
    return df[[a, b]].apply(lambda r: "::".join(sorted([str(r[a]).strip().upper(), str(r[b]).strip().upper()])), axis=1)


def _derive_circa_line(row: pd.Series) -> float | None:
    hoa = row.get("home_or_away")
    if hoa == "HOME" and pd.notna(row.get("circa_spread_home")):
        return row.get("circa_spread_home")
    if hoa == "AWAY" and pd.notna(row.get("circa_spread_away")):
        return row.get("circa_spread_away")
    return row.get("circa_line")


def _map_off(row: pd.Series, side: str):
    hoa = row.get("home_or_away")
    if hoa == "HOME":
        return row.get("off_dvoa_home" if side == "team" else "off_dvoa_away")
    return row.get("off_dvoa_away" if side == "team" else "off_dvoa_home")


def _map_def(row: pd.Series, side: str):
    hoa = row.get("home_or_away")
    if hoa == "HOME":
        return row.get("def_dvoa_home" if side == "team" else "def_dvoa_away")
    return row.get("def_dvoa_away" if side == "team" else "def_dvoa_home")


def _maybe_divide_100(df: pd.DataFrame, cols: List[str]) -> None:
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().any() and s.abs().max() > 1.5:  # likely percent points, not fractions
                df[c] = s / 100.0


def main() -> None:
    ap = argparse.ArgumentParser(description="Enrich Millions planner from roadmap (+totals) and optional odds CSV")
    ap.add_argument("--planner", required=True)
    ap.add_argument("--roadmap", required=True)
    ap.add_argument("--odds", required=False, help="Optional: normalized odds CSV (from fetch_nfl_odds) to source current_total")
    ap.add_argument("--week", type=int, default=None)
    ap.add_argument("--drop_overlaps", action="store_true", help="Drop overlapping columns from planner before merge (avoids *_x/*_y)")
    ap.add_argument("--backup", action="store_true")
    args = ap.parse_args()

    planner_p = Path(args.planner)
    roadmap_p = Path(args.roadmap)
    odds_p = Path(args.odds) if args.odds else None

    p = pd.read_csv(planner_p)
    _norm(p, ["team", "opponent", "home_or_away"])  # planner orientation

    # --- Roadmap meta ---
    r = pd.read_csv(roadmap_p)
    home_col, away_col = _detect_home_away_cols(r)
    _norm(r, [home_col, away_col])
    r[home_col] = r[home_col].replace(ALIASES)
    r[away_col] = r[away_col].replace(ALIASES)
    if args.week is not None and "week" in r.columns:
        r = r[r["week"] == args.week].copy()
    r["_key"] = _key_two(r, home_col, away_col)

    keep = [c for c in KEEP_FROM_ROADMAP if c in r.columns]
    meta = r[["_key"] + keep].drop_duplicates("_key")

    # --- Optional odds CSV for current_total (fallback source) ---
    if odds_p and odds_p.exists():
        o = pd.read_csv(odds_p)
        oh, oa = _detect_home_away_cols(o)
        _norm(o, [oh, oa])
        o["_key"] = _key_two(o, oh, oa)
        o_keep = [c for c in ("current_total", "open_total", "closing_total", "circa_total") if c in o.columns]
        if o_keep:
            o_meta = o[["_key"] + o_keep].drop_duplicates("_key")
            for c in o_keep:
                if c not in meta.columns:
                    meta[c] = None
                meta[c] = meta[c].combine_first(o_meta.set_index("_key")[c])

    # --- Prepare planner for merge ---
    p["_key"] = p[["team", "opponent"]].apply(lambda x: "::".join(sorted([x.team, x.opponent])), axis=1)

    if args.drop_overlaps:
        overlap = [c for c in keep if c in p.columns]
        if overlap:
            p.drop(columns=overlap, inplace=True, errors="ignore")

    merged = p.merge(meta, on="_key", how="left")

    # Derive team‑oriented circa_line from contest spreads if present
    if "home_or_away" in merged.columns:
        merged["circa_line"] = merged.apply(_derive_circa_line, axis=1)

    # Map Off/Def DVOA from per‑side fields into team/opp projections when available
    if {"off_dvoa_home", "off_dvoa_away", "def_dvoa_home", "def_dvoa_away"}.issubset(merged.columns):
        merged["team_off_dvoa_proj"] = merged.apply(lambda r: _map_off(r, "team"), axis=1)
        merged["opp_off_dvoa_proj"]  = merged.apply(lambda r: _map_off(r, "opp"), axis=1)
        merged["team_def_dvoa_proj"] = merged.apply(lambda r: _map_def(r, "team"), axis=1)
        merged["opp_def_dvoa_proj"]  = merged.apply(lambda r: _map_def(r, "opp"), axis=1)

    # Normalize any DVOA that arrived as percent points
    _maybe_divide_100(merged, [
        "team_off_dvoa_proj", "team_def_dvoa_proj", "opp_off_dvoa_proj", "opp_def_dvoa_proj",
        "off_dvoa_home", "off_dvoa_away", "def_dvoa_home", "def_dvoa_away",
    ])

    merged.drop(columns=["_key"], inplace=True, errors="ignore")

    # Backup and write
    if args.backup:
        bak = planner_p.with_suffix(".pre_enrich.bak.csv")
        pd.read_csv(planner_p).to_csv(bak, index=False)
        print(f"Backup written → {bak}")

    merged.to_csv(planner_p, index=False)

    # Console summary (quick sanity)
    print("Planner enriched →", planner_p)
    if keep:
        have = [c for c in keep if c in merged.columns]
        print("Merged columns:", ", ".join(have))
        for c in ("current_spread_home", "current_spread_away", "current_total", "open_total", "closing_total", "circa_total"):
            if c in merged.columns:
                s = pd.to_numeric(merged[c], errors="coerce")
                print(f"  {c:22s} non‑null={int(s.notna().sum())} min={s.min(skipna=True)} max={s.max(skipna=True)}")
    cols = [c for c in ("week","team","opponent","home_or_away","circa_line","current_spread_home","current_spread_away","current_total","open_total","closing_total","circa_total") if c in merged.columns]
    if cols:
        print("\nPreview:\n", merged[cols].head(12).to_string(index=False))


if __name__ == "__main__":
    main()
