#!/usr/bin/env python3
"""
Validate and Join DVOA → Millions Planner (Projections-friendly)
- Supports weekly actuals (season+week) and preseason projections (no week).
- Robust header mapping (handles 'Total DVOA', 'Off DVOA', 'Def DVOA', etc.).
- Robust numeric parsing (%, commas, unicode minus, NBSPs) → decimal floats.
- Drops stale DVOA columns in planner to avoid _x/_y duplicates.
- Projections mode writes *_proj columns; weekly mode writes actuals + dvoa_diff.
"""
from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime
import re
import unicodedata
import pandas as pd

# -------------------------- helpers -----------------------------------------

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm_to_orig = {_norm(c): c for c in df.columns}
    for cand in candidates:
        nc = _norm(cand)
        if nc in norm_to_orig:
            return norm_to_orig[nc]
    return None

PERCENT_CHARS = {"%", "％"}
MINUS_CHARS   = {"-", "−", "–", "—"}

def _to_num(x):
    """Robust string -> float (handles '19.3%', '−2.9 %', '1,234', NBSPs, etc.)."""
    if pd.isna(x):
        return pd.NA
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x)
    s = unicodedata.normalize("NFKC", s)
    had_percent = any(ch in s for ch in PERCENT_CHARS)
    for ch in MINUS_CHARS:
        s = s.replace(ch, "-")
    s = s.replace(",", "").replace("\u00A0", " ").strip()
    m = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
    if not m:
        return pd.NA
    val = float(m.group(0))
    if had_percent:
        val /= 100.0
    return val


def _load_aliases(path: Path | None) -> dict[str, str]:
    if path is None or not Path(path).exists():
        return {}
    alias_df = pd.read_csv(path)
    out: dict[str, str] = {}
    if {"team", "alias"} <= set(alias_df.columns):
        for _, row in alias_df.iterrows():
            out[str(row["alias"]).strip()] = str(row["team"]).strip()
    return out


def _normalize_team(s: pd.Series, alias_map: dict[str, str]) -> pd.Series:
    return s.astype(str).str.strip().map(lambda x: alias_map.get(x, x))


def _drop_stale_dvoa_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove old DVOA columns so re-merges don't create _x/_y duplicates."""
    keep_exact = {"team", "opponent", "week"}
    def _is_stale(col: str) -> bool:
        if col in keep_exact:
            return False
        return (
            col.startswith("team_") or col.startswith("opp_") or
            col.endswith("_dvoa") or col.endswith("_dvoa_proj") or
            col.endswith("_updated_at") or col.endswith("_season") or
            col == "dvoa_diff"
        )
    drop_cols = [c for c in df.columns if _is_stale(c)]
    return df.drop(columns=drop_cols, errors="ignore")

# ------------------------ core functions ------------------------------------

def read_and_normalize_dvoa(
    dvoa_path: str,
    season: int | None,
    week: int | None,
    aliases_path: str | None,
    report: bool = True,
) -> pd.DataFrame:
    dvoa_raw = pd.read_csv(dvoa_path)

    # Column detection (robust to FTN variants and case/spaces)
    col_team   = _pick_col(dvoa_raw, ["team", "team_abbr", "abbr", "TEAM", "Team"])
    col_total  = _pick_col(dvoa_raw, ["total_dvoa", "overall_dvoa", "dvoa",
                                      "team_dvoa", "tot dvoa", "tot_dvoa", "totdvoa",
                                      "tot_dvoa_pct", "Total DVOA"])
    col_off    = _pick_col(dvoa_raw, ["off_dvoa", "offense_dvoa", "off total dvoa",
                                      "off dvoa", "OFF DVOA", "Off DVOA"])
    col_def    = _pick_col(dvoa_raw, ["def_dvoa", "defense_dvoa", "def total dvoa",
                                      "def dvoa", "DEF DVOA", "Def DVOA"])
    col_season = _pick_col(dvoa_raw, ["season", "year", "SEASON", "Year"])
    col_week   = _pick_col(dvoa_raw, ["week", "wk", "WEEK"])

    if report:
        print("DVOA schema →", list(dvoa_raw.columns))
        print("mapped team:", col_team, " total:", col_total, " off:", col_off,
              " def:", col_def, " season:", col_season, " week:", col_week)

    if col_team is None:
        raise ValueError("DVOA file is missing a team column (team/team_abbr/abbr)")
    if col_total is None and (col_off is None or col_def is None):
        raise ValueError("DVOA file must include total_dvoa OR both off_dvoa and def_dvoa")

    # Build compact normalized frame with percent-aware parsing
    keep = {"team": dvoa_raw[col_team]}
    if col_total:
        keep["total_dvoa"] = dvoa_raw[col_total].map(_to_num)
    if col_off:
        keep["off_dvoa"]   = dvoa_raw[col_off].map(_to_num)
    if col_def:
        keep["def_dvoa"]   = dvoa_raw[col_def].map(_to_num)
    if col_season:
        keep["season"]     = pd.to_numeric(dvoa_raw[col_season], errors="coerce")
    if col_week:
        keep["week"]       = pd.to_numeric(dvoa_raw[col_week], errors="coerce")

    df = pd.DataFrame(keep)

    # If total missing but off+def present, approximate total = off - def
    if "total_dvoa" not in df.columns and {"off_dvoa", "def_dvoa"} <= set(df.columns):
        df["total_dvoa"] = df["off_dvoa"] - df["def_dvoa"]

    # Stamp season/week if absent
    if season is not None and "season" not in df.columns:
        df["season"] = season
    if week is not None and "week" not in df.columns:
        df["week"] = week

    # Filter (when fields exist)
    if season is not None and "season" in df.columns:
        df = df[df["season"] == season]
    if week is not None and "week" in df.columns:
        df = df[df["week"] == week]

    # Normalize teams with aliases
    alias_map = _load_aliases(Path(aliases_path) if aliases_path else None)
    df["team"] = _normalize_team(df["team"], alias_map)

    df = df.drop_duplicates(subset=["team"], keep="last").reset_index(drop=True)
    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if report:
        tmin = df["total_dvoa"].min() if "total_dvoa" in df.columns else float("nan")
        tmax = df["total_dvoa"].max() if "total_dvoa" in df.columns else float("nan")
        print(f"[dvoa] teams={df['team'].nunique()}  min={tmin if pd.notna(tmin) else 'nan'}  max={tmax if pd.notna(tmax) else 'nan'}")

    cols = [c for c in ["team", "season", "week", "off_dvoa", "def_dvoa", "total_dvoa", "updated_at"] if c in df.columns]
    return df[cols]


# --------------------------- join -------------------------------------------

def attach_dvoa_to_planner(
    planner_path: str,
    dvoa_path: str,
    season: int,
    week: int,
    aliases_path: str | None = None,
    out_path: str | None = None,
    show: bool = False,
    projections: bool = False,
) -> pd.DataFrame:
    planner = pd.read_csv(planner_path)
    planner = _drop_stale_dvoa_columns(planner)

    # Normalize planner keys
    alias_map = _load_aliases(Path(aliases_path) if aliases_path else None)
    for col in ["team", "opponent"]:
        if col in planner.columns:
            planner[col] = _normalize_team(planner[col], alias_map)

    # Load normalized DVOA
    if projections:
        dvoa = read_and_normalize_dvoa(dvoa_path, season, None, aliases_path)
        rename_map = {}
        if "total_dvoa" in dvoa.columns:
            rename_map["total_dvoa"] = "total_dvoa_proj"
        if "off_dvoa" in dvoa.columns:
            rename_map["off_dvoa"] = "off_dvoa_proj"
        if "def_dvoa" in dvoa.columns:
            rename_map["def_dvoa"] = "def_dvoa_proj"
        dvoa = dvoa.rename(columns=rename_map)
        if show:
            print("\n[debug] parsed projections (first 8 rows):")
            print(dvoa.head(8).to_string(index=False))
    else:
        dvoa = read_and_normalize_dvoa(dvoa_path, season, week, aliases_path)

    # TEAM join
    p = planner.copy()
    p = p.merge(
        dvoa.add_prefix("team_"),
        left_on=["team"],
        right_on=["team_team"],
        how="left",
        suffixes=("", "_dup"),
    )
    p.drop(columns=["team_team"], inplace=True, errors="ignore")

    # OPP join
    p = p.merge(
        dvoa.add_prefix("opp_"),
        left_on=["opponent"],
        right_on=["opp_team"],
        how="left",
        suffixes=("", "_dup"),
    )
    p.drop(columns=["opp_team"], inplace=True, errors="ignore")

    # Derive diff only for real weekly totals
    if not projections and {"team_total_dvoa", "opp_total_dvoa"} <= set(p.columns):
        p["dvoa_diff"] = p["team_total_dvoa"] - p["opp_total_dvoa"]

    # Output
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        p.to_csv(out_path, index=False)
        print(f"[write] {out_path}  (rows={len(p)})")

    if show:
        preview_cols = [
            c for c in [
                "week", "team", "opponent",
                # projections
                "team_total_dvoa_proj", "opp_total_dvoa_proj",
                # actuals
                "team_total_dvoa", "opp_total_dvoa", "dvoa_diff",
            ] if c in p.columns
        ]
        print("\nPreview (selected cols):")
        print(p[preview_cols].head(16).to_string(index=False))

    return p


# --------------------------- CLI --------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Validate weekly DVOA (or projections) and join into Millions planner.")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--planner", type=str, default="picks/millions/millions_planner.csv")
    ap.add_argument("--dvoa", type=str, default="data/dvoa/dvoa_weekly_latest.csv")
    ap.add_argument("--aliases", type=str, default="data/seeds/team_aliases.csv")
    ap.add_argument("--out", type=str, default="picks/millions/millions_planner.csv")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--projections", action="store_true",
                    help="Input is season-level projections (no per-week rows). Label as *_proj.")
    args = ap.parse_args()

    attach_dvoa_to_planner(
        planner_path=args.planner,
        dvoa_path=args.dvoa,
        season=args.season,
        week=args.week,
        aliases_path=args.aliases,
        out_path=args.out,
        show=args.show,
        projections=args.projections,
    )


if __name__ == "__main__":
    main()
