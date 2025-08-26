# scripts/millions_merge_odds.py
# 2) Merge into the planner for the target week:
#python scripts/millions_merge_odds.py `
 # --season 2025 `
  #--week 1 `
  #--planner "picks/millions/millions_planner.csv" `
  #--odds "data/odds/nfl_week1_odds.csv"
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

"""
Merge weekly odds (spreads/totals) into millions_planner.csv in-place.

Assumptions (adjust column names in MAP below to fit your fetch-odds output):
- Odds CSV has one row per game (home_team, away_team).
- Columns can include open/current spreads and totals:
    open_spread_home, open_spread_away, current_spread_home, current_spread_away,
    open_total, current_total, closing_total, circa_total (any subset ok).
- Planner is home-perspective (team=HOME, opponent=AWAY, home_or_away=HOME).

Usage:
  python scripts/millions_merge_odds.py ^
    --season 2025 ^
    --week 1 ^
    --planner picks/millions/millions_planner.csv ^
    --odds data/odds/nfl_week1_odds.csv
"""

MAP = {
    # identity or rename here if fetch-odds uses different headers
    "open_spread_home": "open_spread_home",
    "open_spread_away": "open_spread_away",
    "current_spread_home": "current_spread_home",
    "current_spread_away": "current_spread_away",
    "open_total": "open_total",
    "current_total": "current_total",
    "closing_total": "closing_total",
    "circa_total": "circa_total",
    "circa_line": "circa_line",  # optional, often comes from PDF later
}

TEAM_HOME = "home_team"
TEAM_AWAY = "away_team"

def _norm(x: pd.Series) -> pd.Series:
    return x.astype(str).str.strip().str.upper()

def _to_num(s: pd.Series) -> pd.Series:
    if s is None: return s
    return pd.to_numeric(s.astype(str).str.replace("%","",regex=False).str.replace(",","",regex=False), errors="coerce")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--planner", required=True)
    ap.add_argument("--odds", required=True)
    args = ap.parse_args()

    planner_path = Path(args.planner)
    odds_path = Path(args.odds)

    if not planner_path.exists():
        raise SystemExit(f"Planner not found: {planner_path}")
    if not odds_path.exists():
        raise SystemExit(f"Odds file not found: {odds_path}")

    p = pd.read_csv(planner_path)
    if "season" in p.columns:
        p = p[p["season"] == args.season].copy()
    if "week" in p.columns:
        p = p[p["week"] == args.week].copy()

    full = pd.read_csv(planner_path)  # we’ll merge back into the full planner later

    # Normalize keys
    req = {"team","opponent"}
    if not req.issubset(p.columns):
        raise SystemExit("Planner must have columns: team, opponent.")
    p["team"] = _norm(p["team"]); p["opponent"] = _norm(p["opponent"])

    # Load odds
    o = pd.read_csv(odds_path)
    # Try to detect team columns if not standard
    home_col = TEAM_HOME if TEAM_HOME in o.columns else None
    away_col = TEAM_AWAY if TEAM_AWAY in o.columns else None
    if home_col is None or away_col is None:
        # heuristic fallback
        candidates = [c for c in o.columns if "home" in c.lower()]
        if candidates: home_col = candidates[0]
        candidates = [c for c in o.columns if "away" in c.lower()]
        if candidates: away_col = candidates[0]
    if home_col is None or away_col is None:
        raise SystemExit("Odds file must have home/away team columns.")

    o["_home"] = _norm(o[home_col])
    o["_away"] = _norm(o[away_col])

    # Pick the columns we can map
    keep = [home_col, away_col, "_home", "_away"]
    for src, dst in MAP.items():
        if src in o.columns:
            o[dst] = _to_num(o[src])
            keep.append(dst)
    o2 = o[keep].drop_duplicates(subset=["_home","_away"]).copy()

    # Join into planner WEEK slice (team=HOME, opponent=AWAY)
    merged = p.merge(o2, left_on=["team","opponent"], right_on=["_home","_away"], how="left")

    # Update columns in the full planner
    for _, row in merged.iterrows():
        m = (full.get("season", args.season) == args.season) if "season" in full.columns else True
        if isinstance(m, bool): m = pd.Series([True]*len(full))
        if "week" in full.columns:
            m &= (full["week"] == args.week)
        m &= (_norm(full["team"]) == row["team"]) & (_norm(full["opponent"]) == row["opponent"])
        idx = full.index[m]
        if len(idx) == 0:
            continue
        for dst in MAP.values():
            if dst in merged.columns and pd.notna(row.get(dst)):
                full.loc[idx, dst] = row.get(dst)

    full.to_csv(planner_path, index=False)
    print(f"Planner updated with odds: {planner_path}")

if __name__ == "__main__":
    main()
