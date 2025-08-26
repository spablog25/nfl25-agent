#!/usr/bin/env python3
"""
generate_td_digits.py

Info view (now) + ready for weekly automation (later).

Features:
- Pull TD plays and map scorer -> jersey number using nfl_data_py (nflverse).
- Output WEEKLY counts by last jersey digit (0–9) for each season requested.
- Maintain SEASON TOTALS file across runs.
- Optional: write a digits x years matrix for a range (no all-time totals).
- --week support: process a single week quickly and merge it into the season file.
- Robust nfl_data_py loaders with clear error messages.

Outputs (under data/td_digits/):
- td_digits_weekly_{season}.csv      # rows: week (1..N + TOTAL), cols: digit_0..digit_9
- td_digits_season_totals.csv        # one row per season, cols: digit_0..digit_9
- td_digits_year_columns_{start}_{end}.csv  # (if --write-matrix) rows: 0..9, cols: years

Run examples (from project root):
  python scripts/generate_td_digits.py --season 2024
  python scripts/generate_td_digits.py --season 2024 --week 12
  python scripts/generate_td_digits.py --start 2015 --end 2024 --write-matrix
"""

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Iterable, Tuple, Optional
import pandas as pd

# Dependency: pip install nfl-data-py
try:
    import nfl_data_py as nfl
except ImportError as e:
    raise SystemExit(
        "Missing dependency: nfl_data_py\nInstall with: pip install nfl-data-py"
    ) from e

# ---------- Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # scripts/ is under project root
OUT_DIR = PROJECT_ROOT / "data" / "td_digits"
OUT_DIR.mkdir(parents=True, exist_ok=True)
SEASON_TOTALS_PATH = OUT_DIR / "td_digits_season_totals.csv"

# ---------- Robust loaders (avoid nfl_data_py quirks) ----------

def _safe_import_pbp(season: int) -> pd.DataFrame:
    try:
        if hasattr(nfl, "import_pbp_data"):
            df = nfl.import_pbp_data(years=[season], cache=False, downcast=False)
        else:
            df = nfl.load_pbp_data([season])
    except Exception as e:
        raise SystemExit(
            f"nfl_data_py failed to load PBP for {season}: {e}\n"
            f"Try: pip install --upgrade nfl-data-py pyarrow"
        ) from e
    if not isinstance(df, pd.DataFrame) or df.empty or "season" not in df.columns:
        raise SystemExit(
            f"No PBP data returned for season {season}. "
            f"Try upgrading nfl-data-py/pyarrow or re-running in a fresh venv."
        )
    return df


def _safe_import_weekly_rosters(season: int) -> pd.DataFrame:
    try:
        if hasattr(nfl, "import_weekly_rosters"):
            df = nfl.import_weekly_rosters(years=[season], cache=False)
        else:
            df = nfl.load_roster_weekly([season])
    except Exception as e:
        raise SystemExit(
            f"nfl_data_py failed to load weekly rosters for {season}: {e}"
        ) from e
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise SystemExit(f"No weekly roster data returned for season {season}.")
    return df

# ---------- Helpers ----------

def _clean_roster_weekly(roster: pd.DataFrame) -> pd.DataFrame:
    need = {"season", "week", "player_id", "jersey_number"}
    miss = need - set(roster.columns)
    if miss:
        raise ValueError(f"Weekly roster missing columns: {miss}")
    r = roster.copy()
    r["season"] = r["season"].astype(int)
    r["week"] = r["week"].astype(int)
    r["player_id"] = r["player_id"].astype(str)
    r["jersey_number"] = pd.to_numeric(r["jersey_number"], errors="coerce").astype("Int64")
    return r[["season", "week", "player_id", "jersey_number"]]


def _fallback_season_jersey(roster: pd.DataFrame) -> pd.DataFrame:
    # Mode jersey per player over the season (ignore NA)
    def mode_first(s: pd.Series):
        vals = s.dropna().astype("Int64")
        if vals.empty:
            return pd.NA
        m = vals.mode(dropna=True)
        return m.iloc[0] if not m.empty else pd.NA
    out = roster.groupby("player_id", as_index=False)["jersey_number"].agg(mode_first)
    return out.rename(columns={"jersey_number": "fallback_jersey_number"})


def _ascii_table(title: str, totals: pd.Series) -> str:
    header = f"\n{title}\n" + "-" * len(title)
    lines = []
    max_count = int(totals.max()) if len(totals) else 0
    scale = 40 / max(1, max_count)
    for d in range(10):
        cnt = int(totals.get(d, 0))
        bar = "█" * max(1, int(cnt * scale)) if cnt > 0 else ""
        lines.append(f" {d} | {cnt:4d} {bar}")
    return header + "\n" + "\n".join(lines) + "\n"


def compute_td_digits_for_season(season: int, week: Optional[int] = None) -> Tuple[pd.DataFrame, pd.Series, Optional[pd.Series]]:
    """
    If `week` is None: compute full-season weekly table + season totals.
    If `week` is provided: compute just that week and return a weekly table containing
    ONLY that week (plus a TOTAL row equal to that week). The caller merges safely.

    Returns:
      weekly_df: DataFrame (rows: week(s) + TOTAL, cols: digit_0..digit_9)
      season_totals: Series index 0..9 for this computed slice
      week_counts: Series index 0..9 for the single week (or None if full season)
    """
    # Load data using robust helpers
    pbp = _safe_import_pbp(season)
    roster_weekly = _safe_import_weekly_rosters(season)

    for col in ["season", "week", "touchdown", "td_player_id", "td_player_name"]:
        if col not in pbp.columns:
            raise ValueError(f"PBP missing expected column: {col}")

    td = pbp.loc[pbp["touchdown"] == 1, ["season", "week", "td_player_id", "td_player_name"]].copy()
    td["td_player_id"] = td["td_player_id"].astype(str)
    td = td.dropna(subset=["td_player_id"])

    roster_weekly = _clean_roster_weekly(roster_weekly)

    # Week filter (apply to both PBP subset and roster subset)
    if week is not None:
        td = td[td["week"] == int(week)].copy()
        roster_weekly = roster_weekly[roster_weekly["week"] == int(week)].copy()

    fallback = _fallback_season_jersey(roster_weekly)

    tdj = td.merge(
        roster_weekly,
        left_on=["season", "week", "td_player_id"],
        right_on=["season", "week", "player_id"],
        how="left",
    )

    missing = tdj["jersey_number"].isna()
    if missing.any():
        tdj = tdj.merge(fallback, left_on="td_player_id", right_on="player_id", how="left")
        tdj.loc[missing, "jersey_number"] = tdj.loc[missing, "fallback_jersey_number"]

    tdj["jersey_number"] = pd.to_numeric(tdj["jersey_number"], errors="coerce").astype("Int64")
    dropped = int(tdj["jersey_number"].isna().sum())

    td_clean = tdj.dropna(subset=["jersey_number"]).copy()
    td_clean["digit"] = (td_clean["jersey_number"].astype(int) % 10).astype(int)

    # Aggregate per week
    if td_clean.empty:
        # empty slice: return zeros for requested week
        if week is not None:
            zero = pd.DataFrame([[0]*10], columns=[f"digit_{d}" for d in range(10)])
            zero.index = [int(week)]
            weekly_out = pd.concat([zero, pd.DataFrame([zero.sum()], index=["TOTAL"])])
            season_totals = zero.sum(axis=0)
            season_totals.index = [int(c.split("_")[1]) for c in season_totals.index]
            print(f"Season {season} Week {week}: no TD data found; dropped (no jersey): {dropped}")
            return weekly_out, season_totals, season_totals.copy()
        # full-season but empty (unlikely)
        weekly_out = pd.DataFrame(columns=[f"digit_{d}" for d in range(10)])
        season_totals = pd.Series({d: 0 for d in range(10)})
        return weekly_out, season_totals, None

    week_digit = (
        td_clean.groupby(["week", "digit"]).size().unstack(fill_value=0)
        .reindex(columns=range(10), fill_value=0).sort_index()
    )
    week_digit.columns = [f"digit_{d}" for d in week_digit.columns]

    # Totals for this slice
    season_totals = week_digit.sum(axis=0)
    season_totals.index = [int(c.split("_")[1]) for c in season_totals.index]
    season_totals = season_totals.sort_index()

    # TOTAL row
    total_row = pd.DataFrame([week_digit.sum(axis=0)], index=["TOTAL"])
    weekly_out = pd.concat([week_digit, total_row], axis=0)

    # If a single week was requested, produce that week series for pretty printing
    week_counts = None
    if week is not None and int(week) in week_digit.index:
        wk_row = week_digit.loc[int(week)]
        wk_series = wk_row.rename(lambda c: int(c.split("_")[1]))
        week_counts = wk_series.sort_index()

    if week is None:
        print(f"Season {season}: TD plays: {len(td):,} | with jersey_number: {len(td_clean):,} | dropped (no jersey): {dropped:,}")
    else:
        print(f"Season {season} Week {week}: TD plays: {len(td):,} | with jersey_number: {len(td_clean):,} | dropped (no jersey): {dropped:,}")

    return weekly_out, season_totals, week_counts


def _merge_weekly_csv(season: int, weekly_df: pd.DataFrame) -> Path:
    """Safely update/merge the season's weekly CSV. Handles legacy files missing 'week' header,
    replaces existing week rows, and recomputes TOTAL. Also prints digit rankings for TOTAL."""
    path = OUT_DIR / f"td_digits_weekly_{season}.csv"
    new_df = weekly_df.copy()

    if path.exists():
        cur = pd.read_csv(path)
        # Legacy compatibility: rename first column to 'week' if missing
        if "week" not in cur.columns:
            first_col = cur.columns[0]
            cur = cur.rename(columns={first_col: "week"})

        # Keep only numeric week rows (drop TOTAL)
        cur_numeric = cur[cur["week"].ne("TOTAL")].copy()
        cur_numeric["week"] = pd.to_numeric(cur_numeric["week"], errors="coerce").astype("Int64")
        cur_numeric = cur_numeric.dropna(subset=["week"]).copy()
        cur_numeric["week"] = cur_numeric["week"].astype(int)

        # Incoming rows (single week or many)
        new_numeric = new_df[new_df.index.to_series().astype(str).ne("TOTAL")].copy()
        new_numeric = new_numeric.reset_index().rename(columns={"index": "week"})

        # Replace any existing rows for those weeks
        cur_numeric = cur_numeric[~cur_numeric["week"].isin(new_numeric["week"])].copy()
        merged = pd.concat([cur_numeric, new_numeric], ignore_index=True).sort_values("week")

        # Recompute TOTAL
        digit_cols = [c for c in merged.columns if c.startswith("digit_")]
        total_row = pd.DataFrame([merged[digit_cols].sum(axis=0)])
        total_row.insert(0, "week", "TOTAL")
        out = pd.concat([merged, total_row], ignore_index=True)

        # Print ranking for TOTAL row
        total_counts = total_row[digit_cols].iloc[0]
        ranking = total_counts.sort_values(ascending=False)
        print("\nDigit Rankings (TOTAL, highest to lowest):")
        for rank, (digit, count) in enumerate(ranking.items(), start=1):
            print(f"{rank}. {digit.replace('digit_', '')}: {int(count)}")

        out.to_csv(path, index=False)
    else:
        # First write; ensure TOTAL exists
        if "TOTAL" not in new_df.index:
            digit_cols = [c for c in new_df.columns if c.startswith("digit_")]
            total_row = pd.DataFrame([new_df[digit_cols].sum(axis=0)], index=["TOTAL"])
            new_df = pd.concat([new_df, total_row])
        out = new_df.reset_index().rename(columns={"index": "week"})
        out.to_csv(path, index=False)

    return path


def _update_season_totals(season: int, season_totals: pd.Series) -> Path:
    row = pd.DataFrame([season_totals.values], columns=[f"digit_{d}" for d in range(10)])
    row.insert(0, "season", season)

    if SEASON_TOTALS_PATH.exists():
        cur = pd.read_csv(SEASON_TOTALS_PATH)
        cur = cur[cur["season"] != season]
        out = pd.concat([cur, row], ignore_index=True).sort_values("season")
    else:
        out = row

    out.to_csv(SEASON_TOTALS_PATH, index=False)
    return SEASON_TOTALS_PATH


def _write_matrix(start: int, end: int) -> Path:
    df = pd.read_csv(SEASON_TOTALS_PATH)
    df = df[(df["season"] >= start) & (df["season"] <= end)].copy()
    if df.empty:
        raise SystemExit("No seasons in the requested range to build matrix.")
    # Ensure digit columns
    digit_cols = [f"digit_{d}" for d in range(10)]
    for c in digit_cols:
        if c not in df.columns:
            df[c] = 0
    # Pivot to rows=digit (0..9), cols=season (years)
    pivot = df.set_index("season")[digit_cols].T
    pivot.index = pivot.index.str.replace("digit_", "", regex=False).astype(int)
    pivot = pivot.reindex(index=range(10), fill_value=0).sort_index()
    pivot = pivot.sort_index(axis=1)

    out_path = OUT_DIR / f"td_digits_year_columns_{start}_{end}.csv"
    pivot.to_csv(out_path, index_label="digit")
    return out_path


def run_for_seasons(seasons: Iterable[int], write_matrix: bool, start: Optional[int], end: Optional[int], week: Optional[int]) -> None:
    for yr in seasons:
        weekly_df, season_totals, week_counts = compute_td_digits_for_season(yr, week=week)
        wp = _merge_weekly_csv(yr, weekly_df)
        tp = _update_season_totals(yr, season_totals)

        if week_counts is not None:
            # Print Week table and YTD (from merged file)
            print(_ascii_table(f"TDs by Jersey Digit — Season {yr} Week {week}", week_counts))
            ydf = pd.read_csv(wp)
            ydf_num = ydf[ydf["week"].ne("TOTAL")].copy()
            digit_cols = [c for c in ydf.columns if c.startswith("digit_")]
            ytd = ydf_num[digit_cols].sum(axis=0)
            ytd.index = [int(c.split("_")[1]) for c in ytd.index]
            print(_ascii_table(f"Season {yr} — YTD through Week {week}", ytd))
        else:
            print(_ascii_table(f"TDs by Jersey Digit — Season {yr}", season_totals))

        print(f"Saved/merged weekly → {wp.as_posix()}")
        print(f"Updated season totals → {tp.as_posix()}")

    if write_matrix:
        if start is None or end is None:
            raise SystemExit("--write-matrix requires --start and --end")
        mp = _write_matrix(start, end)
        print(f"Wrote matrix (rows 0–9, cols years) → {mp.as_posix()}")


def parse_args():
    ap = argparse.ArgumentParser(description="Compute TD counts by last jersey digit (0–9).")
    ap.add_argument("--season", type=int, help="Single season (e.g., 2024).")
    ap.add_argument("--start", type=int, help="Start season for a range (inclusive).")
    ap.add_argument("--end", type=int, help="End season for a range (inclusive).")
    ap.add_argument("--write-matrix", action="store_true",
                    help="Also write digits x years matrix for the provided --start/--end range.")
    ap.add_argument("--week", type=int, help="Optional single week (e.g., 12) to process only that week")
    args = ap.parse_args()

    # Determine seasons to run
    seasons: list[int] = []
    if args.season is not None:
        seasons.append(args.season)
    if args.start is not None or args.end is not None:
        if args.start is None or args.end is None:
            raise SystemExit("Provide both --start and --end for a range.")
        seasons.extend(range(int(args.start), int(args.end) + 1))

    if not seasons:
        # Default to 2024 if nothing provided, keeps it simple for first run
        seasons = [2024]

    # Deduplicate & sort
    seasons = sorted(set(seasons))
    return seasons, bool(args.write_matrix), args.start, args.end, args.week


def main():
    seasons, write_matrix, start, end, week = parse_args()
    run_for_seasons(seasons, write_matrix, start, end, week)


if __name__ == "__main__":
    main()
