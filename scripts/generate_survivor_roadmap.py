#!/usr/bin/env python3
"""
Generate Survivor Roadmap (consolidated)
- Handles vistm/hometm → per-team rows + home_or_away
- Derives holiday flags from real dates (date_sch/date/gamedate)
- Adds plays_both_tg_xmas
- Ensures DVOA columns (team/opp total/off/def)
- **Backfills home_or_away** from schedule after merge
- **Coalesces projected_win_prob** from implied win prob or American moneyline
- Dedupes columns; preserves planner fields when present

Run:
  python -m scripts.generate_survivor_roadmap \
    --schedule data/2025_nfl_schedule_cleaned.csv \
    --staging picks/survivor/survivor_schedule_roadmap_expanded.csv \
    --out picks/survivor/survivor_roadmap_expanded.csv
"""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEDULE = ROOT / "data" / "2025_nfl_schedule_cleaned.csv"
DEFAULT_STAGING  = ROOT / "picks" / "survivor" / "survivor_schedule_roadmap_expanded.csv"
DEFAULT_ROADMAP  = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

PLANNER_COLS = [
    "reserved","expected_avail","preferred","must_use","save_for_later","notes","spot_quality"
]

# ---------------- helpers ----------------

def _read_csv(p: Path) -> pd.DataFrame:
    return pd.read_csv(p)

def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated(keep="first")].copy()

def _first(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None

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

# ---------------- holiday utils ----------------

def _thanksgiving_date(y: int) -> date:
    d = date(int(y), 11, 1)
    offset = (3 - d.weekday()) % 7  # Thu=3
    return d + timedelta(days=offset, weeks=3)


def add_holiday_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("is_thanksgiving","is_black_friday","is_christmas"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
        else:
            out[col] = 0

    if "holiday" in out.columns:
        hol = out["holiday"].astype(str).str.lower()
        out.loc[hol.str.contains("thanksgiving", na=False), "is_thanksgiving"] = 1
        out.loc[hol.str.contains("black friday|bf", na=False), "is_black_friday"] = 1
        out.loc[hol.str.contains("christmas|xmas", na=False), "is_christmas"] = 1

    if out[["is_thanksgiving","is_black_friday","is_christmas"]].to_numpy().sum() == 0:
        date_col = _first(out, ["date_sch","date","game_date","gamedate","Date","DATE"])  # prefers your date_sch
        if date_col:
            dt = pd.to_datetime(out[date_col], errors="coerce")
            d_only = dt.dt.normalize()
            yrs = d_only.dt.year.astype("Int64")
            tg_ts = yrs.map(lambda y: pd.NaT if pd.isna(y) else pd.Timestamp(_thanksgiving_date(int(y))))
            bf_ts = yrs.map(lambda y: pd.NaT if pd.isna(y) else pd.Timestamp(_thanksgiving_date(int(y)) + timedelta(days=1)))
            out.loc[d_only.eq(tg_ts), "is_thanksgiving"] = 1
            out.loc[d_only.eq(bf_ts), "is_black_friday"] = 1
            out.loc[(d_only.dt.month == 12) & (d_only.dt.day == 25), "is_christmas"] = 1
    return out

# ---------------- dvoa utils ----------------

def add_dvoa_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    _coalesce(out, "team_dvoa", ["team_dvoa","team_tot_dvoa","team_total_dvoa"])
    _coalesce(out, "opp_dvoa",  ["opp_dvoa","opp_tot_dvoa","opp_total_dvoa"])
    _coalesce(out, "team_off_dvoa", ["team_off_dvoa","off_dvoa","team_off_dvoa_proj"])
    _coalesce(out, "opp_off_dvoa",  ["opp_off_dvoa","opp_off_dvoa_proj"])
    _coalesce(out, "team_def_dvoa", ["team_def_dvoa","def_dvoa","team_def_dvoa_proj"])
    _coalesce(out, "opp_def_dvoa",  ["opp_def_dvoa","opp_def_dvoa_proj"])
    return out

# ---------------- schedule normalization ----------------

def normalize_schedule_keys(sched: pd.DataFrame) -> pd.DataFrame:
    s = sched.copy()
    wk = _first(s, ["week","Week","wk","WK","Wk"]) or "week"
    if wk not in s.columns:
        raise SystemExit("Schedule missing a week column (week/Week/wk)")
    s.rename(columns={wk:"week"}, inplace=True)
    s["week"] = pd.to_numeric(s["week"], errors="coerce").astype("Int64")

    if "team" in s.columns and "opponent" in s.columns:
        if "home_or_away" not in s.columns:
            ht = _first(s, ["hometm","home_team","home","hteam","home_abbr","home_team_abbr","h_abbr"]) or _first(s,["h"])
            at = _first(s, ["vistm","away_team","away","ateam","away_abbr","away_team_abbr","a_abbr"]) or _first(s,["v"])
            if ht and at:
                s["home_or_away"] = np.where(s["team"].astype(str).str.strip()==s[ht].astype(str).str.strip(), "Home", "Away")
            else:
                s["home_or_away"] = "Home"
        return s

    ht = _first(s, ["hometm","home_team","home","hteam","home_abbr","home_team_abbr","h_abbr"]) or _first(s,["h"])
    at = _first(s, ["vistm","away_team","away","ateam","away_abbr","away_team_abbr","a_abbr"]) or _first(s,["v"])
    if not ht or not at:
        raise SystemExit("Schedule missing team columns (need hometm/home_team & vistm/away_team)")

    base_cols = list(s.columns)
    home_rows = s.copy()
    home_rows["team"] = home_rows[ht].astype(str).str.strip()
    home_rows["opponent"] = home_rows[at].astype(str).str.strip()
    home_rows["home_or_away"] = "Home"

    away_rows = s.copy()
    away_rows["team"] = away_rows[at].astype(str).str.strip()
    away_rows["opponent"] = away_rows[ht].astype(str).str.strip()
    away_rows["home_or_away"] = "Away"

    out = pd.concat([home_rows, away_rows], ignore_index=True, sort=False)
    return out[["week","team","opponent","home_or_away"] + [c for c in base_cols if c not in {"week","team","opponent","home_or_away"}]]

# ---------------- main ----------------

def american_to_prob(o: float) -> float:
    try:
        o = float(o)
    except Exception:
        return np.nan
    if np.isnan(o):
        return np.nan
    return (-o)/((-o)+100.0) if o < 0 else 100.0/(o+100.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schedule", type=Path, default=DEFAULT_SCHEDULE)
    ap.add_argument("--staging",  type=Path, default=DEFAULT_STAGING)
    ap.add_argument("--out",      type=Path, default=DEFAULT_ROADMAP)
    args = ap.parse_args()

    sched_raw = _dedupe(_read_csv(args.schedule))
    stage = _dedupe(_read_csv(args.staging))

    sched = normalize_schedule_keys(sched_raw)

    key_cols = ["week","team","opponent"]
    for c in key_cols:
        if c not in stage.columns:
            stage[c] = sched[c]
    stage["week"] = pd.to_numeric(stage["week"], errors="coerce").astype("Int64")
    for c in ["team","opponent"]:
        stage[c] = stage[c].astype(str).str.strip()

    # --- MERGE --- (this is the line you were looking for)
    df = _dedupe(sched.merge(stage, on=key_cols, how="left", suffixes=("_sch","")))

    # --- Backfill home_or_away from schedule if staging is NaN ---
    if "home_or_away" not in df.columns:
        df["home_or_away"] = np.nan
    if "home_or_away_sch" in df.columns:
        df["home_or_away"] = df["home_or_away"].where(df["home_or_away"].notna(), df["home_or_away_sch"])
    df["home_or_away"] = df["home_or_away"].astype(str).str.strip().str.title().replace({"Nan": np.nan})

    # --- Holiday flags & combo ---
    df = add_holiday_flags(df)
    g = df.groupby("team")[ ["is_thanksgiving","is_black_friday","is_christmas"] ].max().reset_index()
    g["plays_both_tg_xmas"] = ((g["is_thanksgiving"].eq(1) | g["is_black_friday"].eq(1)) & g["is_christmas"].eq(1)).astype(int)
    df = df.merge(g[["team","plays_both_tg_xmas"]], on="team", how="left")
    df["plays_both_tg_xmas"] = df["plays_both_tg_xmas"].fillna(0).astype(int)

    # --- DVOA fields ---
    df = add_dvoa_columns(df)

    # --- Coalesce projected win prob ---
    if "projected_win_prob" not in df.columns:
        df["projected_win_prob"] = np.nan
    for cand in ["implied_win_prob","implied_wp","proj_wp","win_prob"]:
        if cand in df.columns:
            df["projected_win_prob"] = df["projected_win_prob"].fillna(pd.to_numeric(df[cand], errors="coerce"))
    if "moneyline" in df.columns and df["projected_win_prob"].isna().any():
        df["projected_win_prob"] = df["projected_win_prob"].fillna(pd.to_numeric(df["moneyline"], errors="coerce").map(american_to_prob))
    df["projected_win_prob"] = pd.to_numeric(df["projected_win_prob"], errors="coerce").clip(0,1).fillna(0.50)

    # Rest days default
    if "rest_days" not in df.columns:
        df["rest_days"] = 6

    # Preserve planner columns from an existing roadmap if present
    if DEFAULT_ROADMAP.exists():
        try:
            old = _dedupe(pd.read_csv(DEFAULT_ROADMAP))
            keep = [c for c in PLANNER_COLS if c in old.columns]
            if keep:
                df = _dedupe(df.merge(old[key_cols + keep], on=key_cols, how="left"))
        except Exception:
            pass

    df.to_csv(args.out, index=False)
    try:
        rel = args.out.relative_to(ROOT)
    except Exception:
        rel = args.out
    print(f"✅ Roadmap written: {rel}")


if __name__ == "__main__":
    main()
