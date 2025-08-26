from __future__ import annotations
import argparse
from pathlib import Path
import sys
import pandas as pd

"""
Build (or rebuild) the Millions planner for a given season/week from your
canonical schedule file, enrich with DVOA, derive circa_line (temp), and
optionally preserve or strip planner fields.

v1.1: **DVOA numeric fix** — robustly coerce DVOA columns to floats and auto‑detect
percent scale (e.g., "11.5%" → 0.115). Prevents "TypeError: unsupported operand type(s) for -: 'str' and 'str'".

USAGE (from repo root):

  python scripts/millions_build_planner.py \
    --season 2025 \
    --week 1 \
    --planner picks/millions/millions_planner.csv \
    --schedule picks/millions/millions_roadmap_game.csv \
    --dvoa data/dvoa/2025_dvoa_projections.csv \
    --derive_lines \
    --preserve_fields pick_side pick_confidence notes \
    --strip_placeholders circa_line result closing_line line_value

Notes:
- Schedule can be the roadmap (home_team/away_team) or a weekly games file that
  already has team/opponent/home_or_away.
- We generate ONE row per matchup (home perspective: HOME team as `team`,
  AWAY team as `opponent`, `home_or_away = HOME`). This keeps the planner at
  16 rows for Week 1 (NFL) and avoids duplicates.
- If you want to derive a temporary `circa_line` from schedule spreads
  (circa_spread_home/away), pass `--derive_lines`.
- We only overwrite the specified week in the planner. Other weeks remain.
"""

PREFERRED_ORDER = [
    "season", "week", "game_num", "team", "opponent", "home_or_away",
    "circa_line", "result", "closing_line", "line_value",
    "pick_side", "pick_confidence", "notes",
    "team_total_dvoa_proj", "opp_total_dvoa_proj", "dvoa_diff_proj",
]


def _norm_case(df: pd.DataFrame, cols=("team", "opponent")) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()
    return df


def _to_numeric_series(s: pd.Series) -> pd.Series:
    """Best-effort: parse numbers that may be strings like '11.5%' or '0.115'.
    Returns float series; auto-divides by 100 if values look like percents.
    """
    if s is None:
        return s
    # Strip percent signs, commas, and spaces then to_numeric
    cleaned = (
        s.astype(str)
         .str.replace('%', '', regex=False)
         .str.replace(',', '', regex=False)
         .str.strip()
    )
    num = pd.to_numeric(cleaned, errors='coerce')
    # Decide if this is in percent scale (e.g., 11.5 meaning 11.5%)
    sample = num.dropna()
    if not sample.empty:
        # Heuristic: if >=70% of non-nulls are >1 and <=100, treat as percent
        frac_gt1 = (sample.abs() > 1.0).mean()
        max_abs = sample.abs().max()
        if frac_gt1 >= 0.70 and max_abs <= 100:
            num = num / 100.0
    return num


def _load_schedule(path: Path, season: int | None, week: int | None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Schedule file not found: {path}")
    df = pd.read_csv(path)

    # Try to detect shape
    has_home_away_columns = {"home_team", "away_team"}.issubset(df.columns)
    has_team_opponent = {"team", "opponent"}.issubset(df.columns)

    # Filter season/week if present
    if season is not None and "season" in df.columns:
        df = df[df["season"] == season]
    if week is not None and "week" in df.columns:
        df = df[df["week"] == week]

    if df.empty:
        raise SystemExit("No schedule rows after filtering — check season/week or file contents.")

    if has_home_away_columns:
        # Build home‑perspective rows only (one row per matchup)
        base = pd.DataFrame({
            "season": season if "season" not in df.columns else df.get("season"),
            "week": week if "week" not in df.columns else df.get("week"),
            "team": df["home_team"].astype(str).str.strip().str.upper(),
            "opponent": df["away_team"].astype(str).str.strip().str.upper(),
            "home_or_away": "HOME",
        })
        # Keep game_num if present on schedule
        if "game_num" in df.columns:
            base["game_num"] = df["game_num"].values
        else:
            base["game_num"] = range(1, len(base) + 1)

        # Carry through useful spread columns if present (for optional derivation)
        for c in ["circa_spread_home", "circa_spread_away", "current_spread_home", "current_spread_away"]:
            if c in df.columns:
                base[c] = pd.to_numeric(df[c], errors='coerce')

        # Carry some extra metadata if available
        for c in ["kickoff_local", "venue", "rest_days_home", "rest_days_away", "rest_days_diff"]:
            if c in df.columns:
                base[c] = df[c].values

        # Deduplicate by matchup key
        base = base.drop_duplicates(subset=["team", "opponent"]).reset_index(drop=True)
        return base

    if has_team_opponent:
        # Assume file already in team/opponent shape; try to determine HOME/AWAY
        out = df[[c for c in df.columns if c in {"season", "week", "team", "opponent", "home_or_away", "game_num"}]].copy()
        out = _norm_case(out, ("team", "opponent"))
        if "home_or_away" not in out.columns:
            out["home_or_away"] = pd.NA
        if "game_num" not in out.columns:
            out["game_num"] = range(1, len(out) + 1)
        return out.drop_duplicates(subset=["team", "opponent"]).reset_index(drop=True)

    raise SystemExit("Schedule file must have either (home_team, away_team) or (team, opponent) columns.")


def _merge_preserve_fields(base: pd.DataFrame, planner_path: Path, season: int, week: int,
                           preserve_fields: list[str]) -> pd.DataFrame:
    if not planner_path.exists():
        return base

    p = pd.read_csv(planner_path)
    p = _norm_case(p)

    # Keep other weeks, we will concat later in caller; here we only need fields to merge
    if "season" in p.columns:
        p = p[p["season"] == season]
    if "week" in p.columns:
        p = p[p["week"] == week]

    if p.empty or not preserve_fields:
        return base

    keep = [c for c in preserve_fields if c in p.columns]
    if not keep:
        return base

    merged = base.merge(p[["team", "opponent"] + keep], on=["team", "opponent"], how="left")
    return merged


def _strip_placeholders(df: pd.DataFrame, cols_to_strip: list[str]) -> pd.DataFrame:
    for c in cols_to_strip:
        if c in df.columns:
            df[c] = pd.NA
        else:
            df[c] = pd.NA
    return df


def _merge_dvoa(df: pd.DataFrame, dvoa_path: Path) -> pd.DataFrame:
    if not dvoa_path or not dvoa_path.exists():
        print("[info] DVOA file not found or not provided — skipping DVOA merge.")
        return df
    d = pd.read_csv(dvoa_path)

    # Detect team column
    if "team" in d.columns:
        team_col = "team"
    elif "abbr" in d.columns:
        team_col = "abbr"
    else:
        team_col = d.columns[0]

    # Prefer a column containing both 'total' and 'dvoa'
    cand = [c for c in d.columns if ("total" in c.lower() and "dvoa" in c.lower())]
    if not cand:
        print("[warn] Could not detect a 'total dvoa' column — skipping DVOA merge.")
        return df
    total_col = cand[0]

    T = d[[team_col, total_col]].copy(); T.columns = ["team", "team_total_dvoa_proj"]
    O = d[[team_col, total_col]].copy(); O.columns = ["opponent", "opp_total_dvoa_proj"]

    df = df.merge(T, on="team", how="left").merge(O, on="opponent", how="left")

    # Coerce DVOA columns to numeric and auto-normalize percent to fractional
    for c in ("team_total_dvoa_proj", "opp_total_dvoa_proj"):
        if c in df.columns:
            df[c] = _to_numeric_series(df[c])

    # Compute diff safely
    df["dvoa_diff_proj"] = df["team_total_dvoa_proj"].astype(float) - df["opp_total_dvoa_proj"].astype(float)
    return df


def _derive_circa_line(df: pd.DataFrame) -> pd.DataFrame:
    # Use circa_spread_home/away if present; respect home_or_away
    if "home_or_away" not in df.columns:
        return df

    def pick_line(row):
        hoa = str(row.get("home_or_away") or "").upper()
        if hoa == "HOME" and "circa_spread_home" in df.columns:
            return row.get("circa_spread_home")
        if hoa == "AWAY" and "circa_spread_away" in df.columns:
            return row.get("circa_spread_away")
        return row.get("circa_line")

    # Ensure numeric even if inputs are strings
    df["circa_line"] = pd.to_numeric(df.apply(pick_line, axis=1), errors='coerce')
    return df


def build_planner(season: int, week: int, planner_path: Path, schedule_path: Path,
                  dvoa_path: Path | None, derive_lines: bool,
                  preserve_fields: list[str], strip_fields: list[str]) -> pd.DataFrame:

    # 1) Schedule → home‑perspective planner rows for the week
    base = _load_schedule(schedule_path, season, week)

    # 2) Normalize keys
    base = _norm_case(base)

    # 3) Merge preserved fields from existing planner (for this week)
    base = _merge_preserve_fields(base, planner_path, season, week, preserve_fields)

    # 4) Optionally strip placeholders
    if strip_fields:
        base = _strip_placeholders(base, strip_fields)

    # 5) DVOA enrichment (optional)
    if dvoa_path:
        base = _merge_dvoa(base, dvoa_path)

    # 6) Derive temporary circa_line from schedule spreads (optional)
    if derive_lines:
        base = _derive_circa_line(base)

    # 7) Sort and assign game_num if missing
    if "game_num" not in base.columns or base["game_num"].isna().any():
        base = base.sort_values(["team", "opponent"]).reset_index(drop=True)
        base["game_num"] = range(1, len(base) + 1)

    # 8) Order columns nicely
    cols = [c for c in PREFERRED_ORDER if c in base.columns] + [c for c in base.columns if c not in PREFERRED_ORDER]
    base = base[cols]

    return base


def main() -> None:
    ap = argparse.ArgumentParser(description="Build or rebuild Millions planner for a specific week.")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--planner", required=True, help="Path to millions_planner.csv (will be overwritten for the target week)")
    ap.add_argument("--schedule", required=True, help="Path to canonical schedule CSV (roadmap or weekly games)")
    ap.add_argument("--dvoa", required=False, default=None, help="Optional DVOA projections CSV to merge")
    ap.add_argument("--derive_lines", action="store_true", help="Derive circa_line from schedule (circa_spread_home/away)")
    ap.add_argument("--preserve_fields", nargs="*", default=[], help="Planner fields to carry over (e.g., pick_side pick_confidence notes)")
    ap.add_argument("--strip_placeholders", nargs="*", default=[], help="Planner fields to blank out (e.g., circa_line result closing_line line_value pick_side notes)")
    ap.add_argument("--out", required=False, default=None, help="Optional output path. If omitted, writes back to --planner (only replacing the target week)")
    args = ap.parse_args()

    season = args.season
    week = args.week
    planner_path = Path(args.planner)
    schedule_path = Path(args.schedule)
    dvoa_path = Path(args.dvoa) if args.dvoa else None
    out_path = Path(args.out) if args.out else planner_path

    # Build the new week slice
    week_df = build_planner(
        season=season,
        week=week,
        planner_path=planner_path,
        schedule_path=schedule_path,
        dvoa_path=dvoa_path,
        derive_lines=args.derive_lines,
        preserve_fields=args.preserve_fields,
        strip_fields=args.strip_placeholders,
    )

    # Merge into existing planner (replace only target week)
    if planner_path.exists():
        existing = pd.read_csv(planner_path)
        # Align columns
        for c in week_df.columns:
            if c not in existing.columns:
                existing[c] = pd.NA
        for c in existing.columns:
            if c not in week_df.columns:
                week_df[c] = pd.NA
        # Replace week
        if "week" in existing.columns:
            rest = existing[existing["week"] != week].copy()
        else:
            rest = existing.iloc[0:0].copy()
        final = pd.concat([rest, week_df], ignore_index=True)
    else:
        final = week_df.copy()

    # Nice column order
    cols = [c for c in PREFERRED_ORDER if c in final.columns] + [c for c in final.columns if c not in PREFERRED_ORDER]
    final = final[cols]

    final.to_csv(out_path, index=False)

    # Prints for humans
    wcnt = len(final[final["week"] == week]) if "week" in final.columns else len(week_df)
    uniq_pairs = len(set(zip(week_df["team"], week_df["opponent"])))

    print("=== Build complete ===")
    print(f"Output file: {out_path}")
    print(f"Season {season} Week {week} rows: {wcnt}")
    print(f"Unique matchup pairs this week: {uniq_pairs}")
    sample_cols = [c for c in ["season","week","game_num","team","opponent","home_or_away","circa_line","pick_side","notes","team_total_dvoa_proj","opp_total_dvoa_proj","dvoa_diff_proj"] if c in final.columns]
    print("Sample:")
    print(final[final["week"] == week][sample_cols].head(12).to_string(index=False))


if __name__ == "__main__":
    main()
