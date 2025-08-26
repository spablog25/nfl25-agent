#!/usr/bin/env python3
"""
Survivor DVOA Align + Clean
---------------------------------
Goal: make Survivor's DVOA cols + keys **exactly** match Millions, and
clean the pesky "first 4 preloaded rows" so your downstream joins don't break.

What this does
1) Loads a Survivor roadmap CSV (staging or final).
2) Drops junk header rows / preloaded rows:
   - rows where `week` is NaN or non-numeric
   - optional: drop first N rows via --drop-top if needed
3) Normalizes team/opponent using team_aliases.csv
4) Standardizes DVOA column names (team/opp total/off/def)
5) Computes `dvoa_gap = team_total_dvoa - opp_total_dvoa`
6) Writes a cleaned file next to the input (or to --out)

Usage
  python -m scripts.survivor_dvoa_align_and_clean \
    --in picks/survivor/survivor_roadmap_expanded.csv \
    --aliases data/seeds/team_aliases.csv \
    --drop-top 4 \
    --out picks/survivor/survivor_roadmap_expanded_clean.csv \
    --show

Notes
- If your file is the *staging* one, point --in there instead.
- This script is idempotent; safe to re-run any time.
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional
import numpy as np
import pandas as pd

# ---------------- helpers ----------------

def _read_csv(p: Path) -> pd.DataFrame:
    return pd.read_csv(p)


def _write_csv(df: pd.DataFrame, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def _load_aliases(path: Optional[Path]) -> dict:
    if path is None or not path.exists():
        return {}
    alias_df = pd.read_csv(path)
    out = {}
    if set(alias_df.columns) >= {"team", "alias"}:
        for _, row in alias_df.iterrows():
            out[str(row["alias"]).strip()] = str(row["team"]).strip()
    return out


def _normalize_team(s: pd.Series, alias_map: dict) -> pd.Series:
    return s.astype(str).str.strip().map(lambda x: alias_map.get(x, x))


def _coalesce(df: pd.DataFrame, target: str, candidates: list[str]) -> None:
    if target not in df.columns:
        df[target] = np.nan
    if df[target].notna().any():
        return
    for c in candidates:
        if c in df.columns:
            df[target] = pd.to_numeric(df[c], errors="coerce")
            if df[target].notna().any():
                break


# ---------------- cleaning core ----------------

def clean_survivor_with_dvoa(path_in: Path, aliases: Optional[Path], drop_top: int = 0, out_path: Optional[Path] = None, show: bool = False) -> Path:
    df = _read_csv(path_in)

    # 1) Optionally drop the first N rows (user-reported preload)
    if drop_top > 0 and len(df) > drop_top:
        df = df.iloc[drop_top:].reset_index(drop=True)

    # 2) Force week to numeric and drop rows with NaN week
    if "week" not in df.columns:
        # try common variants
        for alt in ["Week","wk","WK","Wk"]:
            if alt in df.columns:
                df.rename(columns={alt:"week"}, inplace=True)
                break
    df["week"] = pd.to_numeric(df.get("week"), errors="coerce")
    df = df[df["week"].notna()].reset_index(drop=True)

    # 3) Ensure team/opponent exist and normalize
    if "team" not in df.columns or "opponent" not in df.columns:
        # Attempt from schedule-like columns
        ht = next((c for c in ["hometm","home_team","home_abbr","home"] if c in df.columns), None)
        at = next((c for c in ["vistm","away_team","away_abbr","away"] if c in df.columns), None)
        if ht and at:
            # Create per-team rows
            home = df.copy(); home["team"] = home[ht]; home["opponent"] = home[at]
            away = df.copy(); away["team"] = away[at]; away["opponent"] = away[ht]
            df = pd.concat([home, away], ignore_index=True)
    alias_map = _load_aliases(aliases)
    for col in ["team","opponent"]:
        if col in df.columns:
            df[col] = _normalize_team(df[col], alias_map)

    # 4) Standardize DVOA columns to common names
    _coalesce(df, "team_total_dvoa", [
        "team_total_dvoa","team_tot_dvoa","team_dvoa","team_overall_dvoa"
    ])
    _coalesce(df, "opp_total_dvoa", [
        "opp_total_dvoa","opp_tot_dvoa","opp_dvoa","opp_overall_dvoa"
    ])
    _coalesce(df, "team_off_dvoa", [
        "team_off_dvoa","off_dvoa","off_total_dvoa","off_overall_dvoa"
    ])
    _coalesce(df, "team_def_dvoa", [
        "team_def_dvoa","def_dvoa","def_total_dvoa","def_overall_dvoa"
    ])
    _coalesce(df, "opp_off_dvoa", [
        "opp_off_dvoa","opp_off_dvoa_proj"
    ])
    _coalesce(df, "opp_def_dvoa", [
        "opp_def_dvoa","opp_def_dvoa_proj"
    ])

    # 5) Derive gap
    if {"team_total_dvoa","opp_total_dvoa"} <= set(df.columns):
        df["dvoa_gap"] = pd.to_numeric(df["team_total_dvoa"], errors="coerce") - pd.to_numeric(df["opp_total_dvoa"], errors="coerce")

    # 6) Output
    out = out_path or path_in.with_name(path_in.stem + "_clean.csv")
    _write_csv(df, out)

    if show:
        cols = [c for c in ["week","team","opponent","team_total_dvoa","opp_total_dvoa","dvoa_gap"] if c in df.columns]
        print("Preview →\n", df[cols].head(12).to_string(index=False))
        print(f"\n[write] {out}  rows={len(df)}  cols={len(df.columns)}")
    return out


# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser(description="Clean Survivor roadmap + align DVOA columns with Millions.")
    ap.add_argument("--in", dest="inp", type=str, required=True)
    ap.add_argument("--aliases", type=str, default="data/seeds/team_aliases.csv")
    ap.add_argument("--drop-top", type=int, default=0)
    ap.add_argument("--out", type=str, default=None)
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()

    out = clean_survivor_with_dvoa(
        path_in=Path(args.inp),
        aliases=Path(args.aliases) if args.aliases else None,
        drop_top=args.drop_top,
        out_path=Path(args.out) if args.out else None,
        show=args.show,
    )
    print(f"✅ Cleaned survivor file → {out}")


if __name__ == "__main__":
    main()
