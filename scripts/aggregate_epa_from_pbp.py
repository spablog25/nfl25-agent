# scripts/aggregate_epa_from_pbp.py
# ------------------------------------------------------------
# Purpose:
#   Weekly play-by-play (PBP) aggregator for team EPA metrics.
#   You run this ONCE PER WEEK. It:
#     1) Loads a single week's PBP (from a URL or local parquet/CSV)
#        • Also supports SEASON-LONG PBP files (e.g., pbp_2024.parquet) and will
#          filter to --season/--week automatically if those columns exist.
#     2) Filters out non-plays (kneels, spikes, penalties) and null EPA
#     3) Aggregates team OFF/DEF EPA per play, plus OFF/DEF pass & run EPA per play
#     4) Updates season-to-date (S2D) totals and writes two files:
#         - data/epa_team_s2d_totals.csv (running sums + play counts)
#         - data/epa_team_s2d.csv (clean per-play metrics the builder will join)
#
# Usage (PowerShell):
#   # From project root and after activating venv
#   # OPTION A: load from a URL (parquet or csv). Paste the exact weekly OR season-long PBP link.
#   python -m scripts.aggregate_epa_from_pbp --season 2024 --week 14 --url "https://<pbp_2024.parquet>" --show
#
#   # OPTION B: load from a local parquet/csv you downloaded once
#   python -m scripts.aggregate_epa_from_pbp --season 2024 --week 14 --file "data/pbp/2024/pbp_2024.parquet" --show
#
# Notes:
#   • Parquet is preferred (fast + smaller). Install pyarrow if needed: pip install pyarrow
#   • This script DOES NOT download all weeks. One run processes the provided file and updates S2D.
#   • Week & Season filtering are applied only if those columns exist in the input.
#   • Safe for 2025 Week 1 and beyond — just pass --season 2025 --week 1 with the correct PBP file.
# ------------------------------------------------------------
from __future__ import annotations
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd
import re

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PBP_DIR = DATA_DIR / "pbp"
S2D_TOTALS_PATH = DATA_DIR / "epa_team_s2d_totals.csv"
S2D_EXPORT_PATH = DATA_DIR / "epa_team_s2d.csv"

# Simple alias registry to keep team keys consistent
ALIAS = {
    "WAS": "WSH", "JAC": "JAX", "NOR": "NO", "TAM": "TB", "SFO": "SF", "ARZ": "ARI",
    "GNB": "GB", "KAN": "KC", "NWE": "NE", "SDG": "LAC", "STL": "LAR", "OAK": "LV",
    # Names → Abbr
    "Washington": "WSH", "Jacksonville": "JAX", "New Orleans": "NO", "Tampa Bay": "TB",
    "San Francisco": "SF", "Arizona": "ARI", "Green Bay": "GB", "Kansas City": "KC",
    "New England": "NE", "San Diego": "LAC", "St. Louis": "LAR", "Oakland": "LV",
}


# --- replace your _read_frame with this ---
def _read_frame(path: Path) -> pd.DataFrame:
    """
    Robust Parquet/CSV loader. For Parquet, use pyarrow -> pandas to avoid
    nested-column block errors.
    """
    p = str(path).lower()
    if p.endswith(".parquet"):
        try:
            import pyarrow.parquet as pq  # type: ignore
        except ImportError:
            raise SystemExit("Parquet requested but pyarrow is not installed. Run: pip install pyarrow")
        table = pq.read_table(path)             # read full table safely
        df = table.to_pandas(strings_to_categorical=False)  # flatten to pandas
        return df
    # CSV fallback
    return pd.read_csv(path)


def _load_weekly_pbp(url: str | None, file_path: str | None, season: int, week: int) -> pd.DataFrame:
    """
    Load the PBP from either a URL or a local file.
    We also save a local copy under data/pbp/{season}/week_{week}.parquet for reproducibility.
    """
    PBP_DIR.mkdir(parents=True, exist_ok=True)
    week_dir = PBP_DIR / str(season)
    week_dir.mkdir(exist_ok=True)
    local_parquet = week_dir / f"week_{week}.parquet"

    if url:
        needed_cols = [
            "season", "week", "epa", "posteam", "defteam", "play_type",
            "qb_kneel", "qb_spike", "no_play"
        ]
        if url.lower().endswith(".parquet"):
            df = pd.read_parquet(url, columns=needed_cols)
        else:
            df = pd.read_csv(url)  # CSV is fine to read fully
        # Save a local parquet cache
        try:
            import pyarrow  # noqa
            df.to_parquet(local_parquet, index=False)
        except Exception:
            # Fall back to CSV cache if pyarrow not installed
            df.to_csv(week_dir / f"week_{week}.csv", index=False)
        return df

    # else: load from local file
    if not file_path:
        raise ValueError("Either --url or --file must be provided")
    f = Path(file_path)
    if not f.exists():
        raise FileNotFoundError(f"No such file: {f}")
    df = _read_frame(f)
    # Also cache as parquet for consistency
    try:
        import pyarrow  # noqa
        df.to_parquet(local_parquet, index=False)
    except Exception:
        pass
    return df


def _safe_bool(s: pd.Series, default: int = 0) -> pd.Series:
    """Convert a column to 0/1 safely if it exists; otherwise returns default 0s."""
    if s is None:
        return pd.Series(default, index=pd.RangeIndex(0))  # dummy, not used
    try:
        return s.fillna(0).astype(int)
    except Exception:
        try:
            return s.fillna(False).astype(int)
        except Exception:
            return pd.Series(default, index=s.index)


def aggregate_week(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Return 6 small DataFrames with weekly sums & play counts:
      off_sum_plays(team), def_sum_plays(team),
      off_pass_sum_plays(team), off_run_sum_plays(team),
      def_pass_sum_plays(team), def_run_sum_plays(team)
    """
    # Ensure required columns exist
    required = ["epa", "posteam", "defteam", "play_type"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"PBP missing required column: {c}")

    # Optional filters
    qb_kneel = _safe_bool(df.get("qb_kneel"))
    qb_spike = _safe_bool(df.get("qb_spike"))
    no_play  = _safe_bool(df.get("no_play"))

    # Base filter: valid EPA rows and exclude kneels/spikes/no_play
    mask = (
        df["epa"].notna() &
        (qb_kneel != 1) & (qb_spike != 1) & (no_play != 1)
    )
    base = df.loc[mask].copy()

    # Normalize teams via alias (offense/defense teams)
    base["posteam"] = base["posteam"].astype(str).str.strip().map(lambda x: ALIAS.get(x, x))
    base["defteam"] = base["defteam"].astype(str).str.strip().map(lambda x: ALIAS.get(x, x))

    # Helper to select play types robustly
    pt = base["play_type"].astype(str).str.lower()
    is_pass = pt.eq("pass")
    is_run  = pt.eq("run")

    # OFFENSE: all plays
    off = base.groupby("posteam").agg(
        off_epa_sum=("epa", "sum"),
        off_plays=("epa", "size"),
    ).reset_index().rename(columns={"posteam": "team"})

    # DEFENSE: all plays (EPA allowed)
    dfn = base.groupby("defteam").agg(
        def_epa_sum=("epa", "sum"),
        def_plays=("epa", "size"),
    ).reset_index().rename(columns={"defteam": "team"})

    # OFFENSE: pass-only
    bp = base.loc[is_pass]
    off_pass = bp.groupby("posteam").agg(
        off_pass_epa_sum=("epa", "sum"),
        off_pass_plays=("epa", "size"),
    ).reset_index().rename(columns={"posteam": "team"})

    # OFFENSE: run-only
    br = base.loc[is_run]
    off_run = br.groupby("posteam").agg(
        off_rush_epa_sum=("epa", "sum"),
        off_rush_plays=("epa", "size"),
    ).reset_index().rename(columns={"posteam": "team"})

    # DEFENSE: pass-only (EPA allowed vs pass)
    dp = base.loc[is_pass]
    def_pass = dp.groupby("defteam").agg(
        def_pass_epa_sum=("epa", "sum"),
        def_pass_plays=("epa", "size"),
    ).reset_index().rename(columns={"defteam": "team"})

    # DEFENSE: run-only (EPA allowed vs run)
    dr = base.loc[is_run]
    def_run = dr.groupby("defteam").agg(
        def_rush_epa_sum=("epa", "sum"),
        def_rush_plays=("epa", "size"),
    ).reset_index().rename(columns={"defteam": "team"})

    return off, dfn, off_pass, off_run, def_pass, def_run


def _update_s2d_totals(season: int, week: int, off, dfn, off_pass, off_run, def_pass, def_run) -> pd.DataFrame:
    """Merge this week's sums into the season totals file (create if missing)."""
    cols = [
        "team",
        "off_epa_sum", "off_plays",
        "def_epa_sum", "def_plays",
        "off_pass_epa_sum", "off_pass_plays",
        "off_rush_epa_sum", "off_rush_plays",
        "def_pass_epa_sum", "def_pass_plays",
        "def_rush_epa_sum", "def_rush_plays",
    ]
    # Build a single weekly frame
    weekly = (
        off.merge(dfn, on="team", how="outer")
           .merge(off_pass, on="team", how="outer")
           .merge(off_run, on="team", how="outer")
           .merge(def_pass, on="team", how="outer")
           .merge(def_run, on="team", how="outer")
    )
    for c in cols[1:]:
        if c in weekly.columns:
            weekly[c] = weekly[c].fillna(0)

    # Load existing totals if any
    if S2D_TOTALS_PATH.exists():
        totals = pd.read_csv(S2D_TOTALS_PATH)
    else:
        totals = pd.DataFrame(columns=cols)

    # Outer-merge to add new teams if any
    merged = totals.merge(weekly, on="team", how="outer", suffixes=("", "_wk"))

    # Accumulate sums and plays; fill NaNs with 0 before adding
    for base_col in [
        "off_epa_sum", "off_plays",
        "def_epa_sum", "def_plays",
        "off_pass_epa_sum", "off_pass_plays",
        "off_rush_epa_sum", "off_rush_plays",
        "def_pass_epa_sum", "def_pass_plays",
        "def_rush_epa_sum", "def_rush_plays",
    ]:
        merged[base_col] = merged.get(base_col, 0).fillna(0) + merged.get(base_col+"_wk", 0).fillna(0)
        if base_col+"_wk" in merged.columns:
            merged.drop(columns=[base_col+"_wk"], inplace=True)

    merged["season"] = season
    merged["last_updated_week"] = week
    merged["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return merged[cols + ["season", "last_updated_week", "updated_at"]]


def _export_s2d_per_play(totals: pd.DataFrame) -> pd.DataFrame:
    """Compute per-play metrics from running totals and write tiny join file."""
    df = totals.copy()
    # Avoid division by zero
    for c in [
        "off_plays", "def_plays",
        "off_pass_plays", "off_rush_plays",
        "def_pass_plays", "def_rush_plays",
    ]:
        if c not in df.columns:
            df[c] = 0
        df[c] = df[c].fillna(0).clip(lower=1)

    out = pd.DataFrame({
        "team": df["team"],
        "off_epa_per_play": df.get("off_epa_sum", 0) / df["off_plays"],
        "def_epa_per_play": df.get("def_epa_sum", 0) / df["def_plays"],
        "off_pass_epa_per_play": df.get("off_pass_epa_sum", 0) / df["off_pass_plays"],
        "off_rush_epa_per_play": df.get("off_rush_epa_sum", 0) / df["off_rush_plays"],
        "def_pass_epa_per_play": df.get("def_pass_epa_sum", 0) / df["def_pass_plays"],
        "def_rush_epa_per_play": df.get("def_rush_epa_sum", 0) / df["def_rush_plays"],
        "updated_at": df.get("updated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    })
    out.sort_values("team", inplace=True)
    return out


def main():
    parser = argparse.ArgumentParser(description="Aggregate weekly PBP to team EPA metrics and update season-to-date files.")
    parser.add_argument("--season", type=int, required=True, help="Season year (e.g., 2025)")
    parser.add_argument("--week", type=int, required=True, help="Week number (1-18)")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--url", type=str, help="HTTP(S) weekly OR season-long PBP file (parquet or csv)")
    src.add_argument("--file", type=str, help="Local weekly OR season-long PBP file (parquet or csv)")
    parser.add_argument("--show", action="store_true", help="Print preview tables")
    args = parser.parse_args()

    df = _load_weekly_pbp(args.url, args.file, args.season, args.week)

    # NEW: derive/ensure season & week, then filter
    df = _ensure_season_week(df, args.season, args.week)
    # --- NEW: Filter to requested season/week if columns exist (supports season-long PBP files) ---
    if "season" in df.columns:
        df = df[df["season"] == args.season]
    if "week" in df.columns:
        df = df[df["week"] == args.week]
    # ----------------------------------------------------------------------------------------------

    off, dfn, off_pass, off_run, def_pass, def_run = aggregate_week(df)

    totals = _update_s2d_totals(args.season, args.week, off, dfn, off_pass, off_run, def_pass, def_run)

    # Ensure dirs
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Write season totals (running sums)
    totals.to_csv(S2D_TOTALS_PATH, index=False)

    # Write per-play join file for the builder
    s2d = _export_s2d_per_play(totals)
    s2d.to_csv(S2D_EXPORT_PATH, index=False)

    print(f"Updated totals → {S2D_TOTALS_PATH}")
    print(f"Exported per-play → {S2D_EXPORT_PATH} (rows={len(s2d)})")

    if args.show:
        print("\n=== Weekly Offense (sums) ===")
        print(off.head(10).to_string(index=False))
        print("\n=== Weekly Defense (sums) ===")
        print(dfn.head(10).to_string(index=False))
        print("\n=== Weekly DEF Pass/Run (sums) ===")
        print(def_pass.head(10).to_string(index=False))
        print(def_run.head(10).to_string(index=False))
        print("\n=== S2D Per-Play Preview ===")
        print(s2d.head(10).to_string(index=False))


def _ensure_season_week(df: pd.DataFrame, season: int | None, week: int | None) -> pd.DataFrame:
    """
    Ensure df has 'season' and 'week' columns. If missing, try to derive from:
    - 'old_game_id' like '2023_14_TB_ATL'
    - 'nflverse_game_id' if it follows 'YYYY_WW_...' pattern

    Then filter to the provided season/week if available.
    """
    dfx = df.copy()

    have_season = "season" in dfx.columns
    have_week   = "week" in dfx.columns

    # Try deriving from old_game_id first (most reliable)
    if not (have_season and have_week) and "old_game_id" in dfx.columns:
        m = dfx["old_game_id"].astype(str).str.extract(r"(?P<season>\d{4})_(?P<week>\d{1,2})_")
        if "season" not in dfx.columns and "season" in m.columns:
            dfx["season"] = pd.to_numeric(m["season"], errors="coerce")
            have_season = True
        if "week" not in dfx.columns and "week" in m.columns:
            dfx["week"] = pd.to_numeric(m["week"], errors="coerce")
            have_week = True

    # Fallback: derive from nflverse_game_id if present
    if not (have_season and have_week) and "nflverse_game_id" in dfx.columns:
        m2 = dfx["nflverse_game_id"].astype(str).str.extract(r"(?P<season>\d{4})_(?P<week>\d{1,2})_")
        if not have_season and "season" in m2.columns:
            dfx["season"] = pd.to_numeric(m2["season"], errors="coerce")
            have_season = True
        if not have_week and "week" in m2.columns:
            dfx["week"] = pd.to_numeric(m2["week"], errors="coerce")
            have_week = True

    # If still missing week but we have a game date, you *could* map dates→weeks later.
    # For now, we require the above derivations.

    # Apply filter if columns now exist
    before = len(dfx)
    if season is not None and "season" in dfx.columns:
        dfx = dfx[dfx["season"] == season]
    if week is not None and "week" in dfx.columns:
        dfx = dfx[dfx["week"] == week]
    after = len(dfx)
    print(f"[derive] filter season/week → {before} → {after} rows "
          f"(have season={ 'season' in dfx.columns }, week={ 'week' in dfx.columns })")

    return dfx

if __name__ == "__main__":
    main()
