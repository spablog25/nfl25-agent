# scripts/join_dvoa_into_millions.py
# -----------------------------------------------------------------------------
# Purpose
#   Adapter + join helper to wire your weekly DVOA snapshot into the Millions
#   dashboard with zero breakage. This does NOT fetch data; it reads the file
#   your `ingest_ftn_dvoa_snapshot.py` (or other ingest) writes each week.
#
# What this script does
#   1) Reads a weekly DVOA CSV (path configurable) produced by your ingest.
#   2) Normalizes team abbreviations using data/seeds/team_aliases.csv.
#   3) Filters to the given season & week.
#   4) Left‑joins DVOA onto a games/planner dataframe (by team and opponent).
#   5) Computes dvoa_diff (team_total_dvoa - opp_total_dvoa).
#   6) Writes a merged output (or returns a DataFrame if used as a module).
#
# Input expectations (flexible)
#   The DVOA CSV should include at least these columns (case-insensitive):
#     team | total_dvoa (or overall_dvoa / dvoa / team_dvoa)
#   Optional: season, week, off_dvoa, def_dvoa
#   If season/week are missing, you pass them via CLI and we'll tag them.
#
# Games/planner expectation
#   A CSV with at minimum: week, team, opponent, circa_line (others are fine).
#
# Usage
#   # Example: merge DVOA into an existing planner
#   python -m scripts.join_dvoa_into_millions \
#       --season 2025 --week 1 \
#       --planner data/millions_planner.csv \
#       --dvoa data/vendor/ftn/dvoa_weekly.csv \
#       --aliases data/seeds/team_aliases.csv \
#       --out data/millions_planner_with_dvoa.csv \
#       --show
#
# Notes
#   • If your DVOA file uses different column names, we auto-map the best options.
#   • If it contains multiple weeks, we'll filter to the one you pass.
#   • Safe to run repeatedly; it's a pure left-join and overwrite of output.
# -----------------------------------------------------------------------------
from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

# -------------------------- helpers -----------------------------------------

def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def _load_aliases(path: Path | None) -> dict:
    if path is None or not Path(path).exists():
        return {}
    alias_df = pd.read_csv(path)
    out = {}
    if set(alias_df.columns) >= {"team", "alias"}:
        for _, row in alias_df.iterrows():
            out[str(row["alias"]).strip()] = str(row["team"]).strip()
    return out


def _normalize_team(s: pd.Series, alias_map: dict) -> pd.Series:
    return s.astype(str).str.strip().map(lambda x: alias_map.get(x, x))


# ------------------------ core functions ------------------------------------

def read_and_normalize_dvoa(dvoa_path: str, season: int | None, week: int | None, aliases_path: str | None) -> pd.DataFrame:
    dvoa = pd.read_csv(dvoa_path)

    # Column detection
    col_team = _pick_col(dvoa, ["team", "team_abbr", "abbr"]) or "team"
    col_total = _pick_col(dvoa, [
        "total_dvoa", "overall_dvoa", "dvoa", "team_dvoa", "tot_dvoa",
    ])
    col_off = _pick_col(dvoa, ["off_dvoa", "offense_dvoa", "off_total_dvoa", "off" ])
    col_def = _pick_col(dvoa, ["def_dvoa", "defense_dvoa", "def_total_dvoa", "def" ])
    col_season = _pick_col(dvoa, ["season", "year"])
    col_week = _pick_col(dvoa, ["week", "wk"])

    # Basic validity
    if col_team is None:
        raise ValueError("DVOA file is missing a team column (team/team_abbr/abbr)")
    if col_total is None and (col_off is None or col_def is None):
        raise ValueError("DVOA file must include total_dvoa OR both off_dvoa and def_dvoa")

    # Build a compact frame
    keep = {"team": dvoa[col_team]}
    if col_total:
        keep["total_dvoa"] = pd.to_numeric(dvoa[col_total], errors="coerce")
    if col_off:
        keep["off_dvoa"] = pd.to_numeric(dvoa[col_off], errors="coerce")
    if col_def:
        keep["def_dvoa"] = pd.to_numeric(dvoa[col_def], errors="coerce")
    if col_season:
        keep["season"] = pd.to_numeric(dvoa[col_season], errors="coerce")
    if col_week:
        keep["week"] = pd.to_numeric(dvoa[col_week], errors="coerce")

    df = pd.DataFrame(keep)

    # If total is missing but off+def present, approximate total as off - def
    if "total_dvoa" not in df.columns and {"off_dvoa", "def_dvoa"} <= set(df.columns):
        df["total_dvoa"] = df["off_dvoa"] - df["def_dvoa"]

    # Attach season/week if absent
    if season is not None and "season" not in df.columns:
        df["season"] = season
    if week is not None and "week" not in df.columns:
        df["week"] = week

    # Filter to the requested frame
    if season is not None and "season" in df.columns:
        df = df[df["season"] == season]
    if week is not None and "week" in df.columns:
        df = df[df["week"] == week]

    # Normalize teams
    alias_map = _load_aliases(Path(aliases_path) if aliases_path else None)
    df["team"] = _normalize_team(df["team"], alias_map)

    # Deduplicate to one row per team (keep last)
    df = df.drop_duplicates(subset=["team"], keep="last").reset_index(drop=True)

    # Timestamp for freshness
    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Sanity checks
    if df["team"].nunique() < 28:
        print("[warn] Fewer than 28 unique teams after normalization — check source file/week filter.")

    return df[[c for c in ["team","season","week","off_dvoa","def_dvoa","total_dvoa","updated_at"] if c in df.columns]]


def attach_dvoa_to_planner(planner_path: str, dvoa_path: str, season: int, week: int,
                           aliases_path: str | None = None, out_path: str | None = None,
                           show: bool = False) -> pd.DataFrame:
    planner = pd.read_csv(planner_path)

    # Normalize planner keys
    alias_map = _load_aliases(Path(aliases_path) if aliases_path else None)
    for col in ["team", "opponent"]:
        if col in planner.columns:
            planner[col] = _normalize_team(planner[col], alias_map)

    # Load normalized DVOA
    dvoa = read_and_normalize_dvoa(dvoa_path, season, week, aliases_path)

    # Join for team and opponent
    p = planner.copy()
    p = p.merge(dvoa.add_prefix("team_"), left_on=["team"], right_on=["team_team"], how="left")
    p.drop(columns=["team_team"], inplace=True)
    p = p.merge(dvoa.add_prefix("opp_"), left_on=["opponent"], right_on=["opp_team"], how="left")
    p.drop(columns=["opp_team"], inplace=True)

    # Derive diff (team_total - opp_total)
    if {"team_total_dvoa", "opp_total_dvoa"} <= set(p.columns):
        p["dvoa_diff"] = p["team_total_dvoa"] - p["opp_total_dvoa"]

    # Output
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        p.to_csv(out_path, index=False)
        print(f"[write] {out_path}  (rows={len(p)})")

    if show:
        preview_cols = [c for c in [
            "week","team","opponent","team_total_dvoa","opp_total_dvoa","dvoa_diff"
        ] if c in p.columns]
        print("\nPreview (selected cols):")
        print(p[preview_cols].head(12).to_string(index=False))

    return p


# --------------------------- CLI --------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Join weekly DVOA snapshot into Millions planner.")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--planner", type=str, required=True, help="Path to planner/games CSV")
    ap.add_argument("--dvoa", type=str, required=True, help="Path to DVOA weekly CSV (output of your ingest)")
    ap.add_argument("--aliases", type=str, default="data/seeds/team_aliases.csv")
    ap.add_argument("--out", type=str, default="data/millions_planner_with_dvoa.csv")
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()

    attach_dvoa_to_planner(
        planner_path=args.planner,
        dvoa_path=args.dvoa,
        season=args.season,
        week=args.week,
        aliases_path=args.aliases,
        out_path=args.out,
        show=args.show,
    )


if __name__ == "__main__":
    main()
