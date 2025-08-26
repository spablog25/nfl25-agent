#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_nfl_odds — unified season fetch + planner & survivor update (v3.3.3)
---------------------------------------------------------------------------
Fixes
- Resolves pandas NotImplementedError: "cannot align with a higher dimensional NDFrame"
  that could occur during Millions planner merge when duplicate column names
  caused a DataFrame to be passed into Series.where/fill paths. We now:
  • Merge with suffixes and **explicitly** read from right-hand (odds) columns.
  • Use `.fillna(src_series)` and safe getters instead of `.where(...)`.
  • Avoid any operation that relies on ambiguous duplicate column keys.
- Survivor chooser hardened against NaN/None team values.

Notes
- This is a "lite" build (no historical snapshots). It writes current spreads/totals,
  Circa, and implied win probabilities; then updates Millions planner & Survivor CSVs.

Typical usage (PowerShell)
  $env:ODDS_API_KEY = "<YOUR_KEY>"
  python scripts/fetch_nfl_odds.py \
    --season 2025 \
    --regions us \
    --out "data/odds/season_all.csv" \
    --update_planner "picks/millions/millions_planner.csv" \
    --update_survivor "picks/survivor/survivor_roadmap.csv" \
    --backup 1 \
    --history 0
"""

from __future__ import annotations
import argparse
import os
from pathlib import Path
from typing import Dict, List, Tuple, Any, Iterable

import requests
import pandas as pd

API_BASE = "https://api.the-odds-api.com/v4"

# ------------------------------------------------------------
# Team name → abbreviation map (planner compatible)
# ------------------------------------------------------------
TEAM_NAME_TO_ABBR: Dict[str, str] = {
    # AFC East
    "BUFFALO BILLS": "BUF", "MIAMI DOLPHINS": "MIA", "NEW ENGLAND PATRIOTS": "NE", "NEW YORK JETS": "NYJ",
    # AFC North
    "BALTIMORE RAVENS": "BAL", "CINCINNATI BENGALS": "CIN", "CLEVELAND BROWNS": "CLE", "PITTSBURGH STEELERS": "PIT",
    # AFC South
    "HOUSTON TEXANS": "HOU", "INDIANAPOLIS COLTS": "IND", "JACKSONVILLE JAGUARS": "JAX", "TENNESSEE TITANS": "TEN",
    # AFC West
    "DENVER BRONCOS": "DEN", "KANSAS CITY CHIEFS": "KC", "LAS VEGAS RAIDERS": "LV", "LOS ANGELES CHARGERS": "LAC",
    # NFC East
    "DALLAS COWBOYS": "DAL", "NEW YORK GIANTS": "NYG", "PHILADELPHIA EAGLES": "PHI", "WASHINGTON COMMANDERS": "WSH",
    # NFC North
    "CHICAGO BEARS": "CHI", "DETROIT LIONS": "DET", "GREEN BAY PACKERS": "GB", "MINNESOTA VIKINGS": "MIN",
    # NFC South
    "ATLANTA FALCONS": "ATL", "CAROLINA PANTHERS": "CAR", "NEW ORLEANS SAINTS": "NO", "TAMPA BAY BUCCANEERS": "TB",
    # NFC West
    "ARIZONA CARDINALS": "ARI", "LOS ANGELES RAMS": "LAR", "SAN FRANCISCO 49ERS": "SF", "SEATTLE SEAHAWKS": "SEA",
    # Common alternates/legacy
    "WASHINGTON REDSKINS": "WSH", "WASHINGTON FOOTBALL TEAM": "WSH",
    "LA RAMS": "LAR", "LA CHARGERS": "LAC", "OAKLAND RAIDERS": "LV", "SAN DIEGO CHARGERS": "LAC",
}
ALIASES: Dict[str, str] = {"WAS": "WSH", "ARZ": "ARI", "LA": "LAR"}


def to_abbr(name: str) -> str | None:
    if not isinstance(name, str):
        return None
    up = name.strip().upper()
    if up in TEAM_NAME_TO_ABBR:
        return TEAM_NAME_TO_ABBR[up]
    if up in ALIASES:
        return ALIASES[up]
    up2 = " ".join("".join(ch for ch in up if ch.isalnum() or ch.isspace()).split())
    return TEAM_NAME_TO_ABBR.get(up2)


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def season_window(season: int) -> Tuple[str, str]:
    start = f"{season}-08-01T00:00:00Z"
    end   = f"{season + 1}-02-15T08:00:00Z"
    return start, end


def normalize_pair(h: float | None, a: float | None) -> Tuple[float | None, float | None]:
    h = float(h) if h is not None else None
    a = float(a) if a is not None else None
    if h is None and a is None:
        return None, None
    if h is None:
        return (-a if a is not None else None), a
    if a is None:
        return h, -h
    if abs(h + a) > 1e-6:
        return h, -h
    return h, a


def american_to_prob(price: float | int | None) -> float | None:
    if price is None:
        return None
    try:
        p = float(price)
    except Exception:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    else:
        return (-p) / ((-p) + 100.0)


def market_lookup(book: Dict[str, Any], key: str) -> Dict[str, Any] | None:
    for m in book.get("markets", []) or []:
        if (m.get("key") or m.get("market_key") or "").lower() == key:
            return m
    return None


# ------------------------------------------------------------
# Core fetch (current only)
# ------------------------------------------------------------

def fetch_odds(api_key: str, sport: str, regions: str = "us", markets: str = "h2h,spreads,totals",
               odds_format: str = "american", commence_from: str | None = None,
               commence_to: str | None = None) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/sports/{sport}/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": "iso",
    }
    if commence_from:
        params["commenceTimeFrom"] = commence_from
    if commence_to:
        params["commenceTimeTo"] = commence_to
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    remain = r.headers.get("x-requests-remaining"); used = r.headers.get("x-requests-used")
    if remain is not None:
        print(f"The Odds API: used={used} remaining={remain}")
    return r.json()


def extract_row_current(ev: Dict[str, Any]) -> Dict[str, Any] | None:
    home_name = ev.get("home_team"); away_name = ev.get("away_team")
    h = to_abbr(home_name); a = to_abbr(away_name)
    if not h or not a:
        return None

    spread_home_pts: List[float] = []
    spread_away_pts: List[float] = []
    total_pts: List[float] = []
    ml_home_prices: List[float] = []
    ml_away_prices: List[float] = []

    circa_h = circa_a = circa_total = None

    for book in ev.get("bookmakers", []) or []:
        title = (book.get("title") or book.get("key") or "").lower()
        m_h2h = market_lookup(book, "h2h")
        m_spreads = market_lookup(book, "spreads")
        m_totals = market_lookup(book, "totals")

        if m_h2h and m_h2h.get("outcomes"):
            for o in m_h2h["outcomes"]:
                nm = (o.get("name") or "").upper(); price = o.get("price")
                if nm == home_name.upper():
                    ml_home_prices.append(float(price))
                elif nm == away_name.upper():
                    ml_away_prices.append(float(price))

        hh = aa = None
        if m_spreads and m_spreads.get("outcomes"):
            for o in m_spreads["outcomes"]:
                nm = (o.get("name") or "").upper(); pt = o.get("point")
                if nm == home_name.upper():
                    hh = pt
                elif nm == away_name.upper():
                    aa = pt
        hh, aa = normalize_pair(hh, aa)
        if hh is not None: spread_home_pts.append(float(hh))
        if aa is not None: spread_away_pts.append(float(aa))

        if m_totals and m_totals.get("outcomes"):
            tpt = None
            for o in m_totals["outcomes"]:
                nm = (o.get("name") or "").lower()
                if nm in ("over", "under") and o.get("point") is not None:
                    tpt = float(o["point"])  # Over/Under share the same total
                    break
            if tpt is not None:
                total_pts.append(tpt)

        if "circa" in title:
            if hh is not None and aa is not None:
                circa_h, circa_a = hh, aa
            if m_totals and m_totals.get("outcomes"):
                for o in m_totals["outcomes"]:
                    if (o.get("name") or "").lower() == "over" and o.get("point") is not None:
                        circa_total = float(o["point"])
                        break

    def med(vals: List[float]) -> float | None:
        if not vals: return None
        s = pd.Series(vals).astype(float)
        return float(s.median())

    cur_h = med(spread_home_pts); cur_a = med(spread_away_pts)
    cur_h, cur_a = normalize_pair(cur_h, cur_a)
    cur_tot = med(total_pts)

    cur_ml_home = med(ml_home_prices)
    cur_ml_away = med(ml_away_prices)

    win_home = american_to_prob(cur_ml_home) if cur_ml_home is not None else None
    win_away = american_to_prob(cur_ml_away) if cur_ml_away is not None else None

    return {
        "event_id": ev.get("id"),
        "commence_time": ev.get("commence_time"),
        "home_team": h,
        "away_team": a,
        "current_spread_home": cur_h,
        "current_spread_away": cur_a,
        "current_total": cur_tot,
        "current_ml_home": cur_ml_home,
        "current_ml_away": cur_ml_away,
        "win_prob_home": win_home,
        "win_prob_away": win_away,
        "circa_spread_home": circa_h,
        "circa_spread_away": circa_a,
        "circa_total": circa_total,
        "book_count_spreads": len(spread_home_pts) or len(spread_away_pts),
        "book_count_totals": len(total_pts),
        "book_count_h2h": len(ml_home_prices) or len(ml_away_prices),
    }


# ------------------------------------------------------------
# Merge helpers (Planner & Survivor)
# ------------------------------------------------------------

def make_game_key(a: str, b: str) -> str:
    return "|".join(sorted([(a or "").upper(), (b or "").upper()]))


def _safe_get(df: pd.DataFrame, col: str) -> pd.Series | None:
    """Return a single Series for column, preferring exact name then *odds.
    Avoids duplicate-name traps; returns None if neither found.
    """
    if col in df.columns:
        s = df[col]
        # If duplicates somehow exist, pick the first occurrence as Series
        if isinstance(s, pd.DataFrame):
            return s.iloc[:, 0]
        return s
    alt = f"{col}_odds"
    if alt in df.columns:
        s = df[alt]
        if isinstance(s, pd.DataFrame):
            return s.iloc[:, 0]
        return s
    return None


def update_planner(planner_csv: Path, df_odds: pd.DataFrame, backup: bool = True) -> Path:
    dfp = pd.read_csv(planner_csv)
    for col in ("team", "opponent", "home_or_away"):
        if col in dfp.columns:
            dfp[col] = dfp[col].astype(str).str.upper().str.strip()
    dfp["__game_key"] = [make_game_key(t, o) for t, o in zip(dfp.get("team"), dfp.get("opponent"))]

    dfo = df_odds.copy()
    dfo["__game_key"] = [make_game_key(h, a) for h, a in zip(dfo["home_team"], dfo["away_team"])]

    use_cols = [
        "__game_key",
        "current_spread_home", "current_spread_away", "current_total",
        "circa_spread_home", "circa_spread_away", "circa_total",
        "open_spread_home", "open_spread_away", "open_total",
        "line_delta_home",
    ]
    dfo = dfo[[c for c in use_cols if c in dfo.columns]]

    merged = dfp.merge(dfo, on="__game_key", how="left", suffixes=("", "_odds"))

    def put_from_odds(dst: str):
        src = _safe_get(merged, dst)
        if src is None:
            return
        if dst in merged.columns:
            merged[dst] = merged[dst].fillna(src)
        else:
            merged[dst] = src

    for name in ("current_spread_home", "current_spread_away", "current_total", "open_total", "line_delta_home"):
        put_from_odds(name)

    # Circa line perspective + open line perspective
    if "home_or_away" in merged.columns:
        csh = _safe_get(merged, "circa_spread_home")
        csa = _safe_get(merged, "circa_spread_away")
        if csh is not None or csa is not None:
            merged["circa_line"] = merged.get("circa_line")
            mask_home = merged["home_or_away"].astype(str).str.upper().eq("HOME")
            if csh is not None:
                merged.loc[mask_home, "circa_line"] = csh[mask_home]
            if csa is not None:
                merged.loc[~mask_home, "circa_line"] = csa[~mask_home]
        # Optional: open_line from odds
        osh = _safe_get(merged, "open_spread_home")
        osa = _safe_get(merged, "open_spread_away")
        if osh is not None or osa is not None:
            merged["open_line"] = merged.get("open_line")
            mask_home = merged["home_or_away"].astype(str).str.upper().eq("HOME")
            if osh is not None:
                merged.loc[mask_home, "open_line"] = osh[mask_home]
            if osa is not None:
                merged.loc[~mask_home, "open_line"] = osa[~mask_home]

    # Ensure circa_total present from odds if planner missing
    ct = _safe_get(merged, "circa_total")
    if ct is not None:
        if "circa_total" in merged.columns:
            merged["circa_total"] = merged["circa_total"].fillna(ct)
        else:
            merged["circa_total"] = ct

    merged = merged.drop(columns=[c for c in merged.columns if c.startswith("__")], errors="ignore")

    if backup:
        bak = planner_csv.with_suffix(planner_csv.suffix + ".bak")
        dfp.to_csv(bak, index=False)
        print(f"Planner backup → {bak}")

    merged.to_csv(planner_csv, index=False)
    print(f"Planner updated → {planner_csv}  (rows={len(merged)})")
    return planner_csv


def update_survivor(survivor_csv: Path, df_odds: pd.DataFrame, backup: bool = True) -> Path:
    dfs = pd.read_csv(survivor_csv)
    for col in ("team", "opponent"):
        if col in dfs.columns:
            dfs[col] = dfs[col].astype(str).str.upper().str.strip()
    dfs["__game_key"] = [make_game_key(t, o) for t, o in zip(dfs.get("team"), dfs.get("opponent"))]

    dfo = df_odds.copy()
    dfo["__game_key"] = [make_game_key(h, a) for h, a in zip(dfo["home_team"], dfo["away_team"])]

    keep = ["__game_key", "home_team", "away_team", "current_ml_home", "current_ml_away", "win_prob_home", "win_prob_away"]
    dfo = dfo[[c for c in keep if c in dfo.columns]]

    merged = dfs.merge(dfo, on="__game_key", how="left", suffixes=("", "_odds"))

    def _up(x: Any) -> str:
        return x.upper() if isinstance(x, str) else ""

    def choose(row, home_col, away_col, team_field="team"):
        t = _up(row.get(team_field))
        home = _up(row.get("home_team"))
        away = _up(row.get("away_team"))
        if t and t == home:
            return row.get(home_col)
        if t and t == away:
            return row.get(away_col)
        return None

    if set(["current_ml_home", "current_ml_away"]).issubset(merged.columns):
        merged["current_ml_team"] = merged.apply(lambda r: choose(r, "current_ml_home", "current_ml_away"), axis=1)
    if set(["win_prob_home", "win_prob_away"]).issubset(merged.columns):
        merged["win_prob_team"] = merged.apply(lambda r: choose(r, "win_prob_home", "win_prob_away"), axis=1)

    if backup:
        bak = survivor_csv.with_suffix(survivor_csv.suffix + ".bak")
        dfs.to_csv(bak, index=False)
        print(f"Survivor backup → {bak}")

    merged = merged.drop(columns=[c for c in merged.columns if c.startswith("__") or c.endswith("_odds")], errors="ignore")
    merged.to_csv(survivor_csv, index=False)
    print(f"Survivor updated → {survivor_csv}  (rows={len(merged)})")
    return survivor_csv


# ------------------------------------------------------------
# Orchestrator
# ------------------------------------------------------------

def run(api_key: str, sport: str, regions: str, out_csv: Path,
        commence_from: str | None, commence_to: str | None,
        season: int | None,
        update_planner_path: Path | None, update_survivor_path: Path | None,
        backup: bool,
        open_offsets_days: List[int] | None = None,
        history_enabled: bool = False) -> Tuple[Path, pd.DataFrame]:

    if season and not (commence_from or commence_to):
        commence_from, commence_to = season_window(season)
        print(f"Season window inferred: {commence_from} → {commence_to}")

    raw = fetch_odds(api_key=api_key, sport=sport, regions=regions,
                     commence_from=commence_from, commence_to=commence_to)
    rows: List[Dict[str, Any]] = []
    for ev in raw:
        row = extract_row_current(ev)
        if row:
            rows.append(row)
    if not rows:
        raise SystemExit("No rows extracted — check sport/regions/date window or rate limits.")

    df = pd.DataFrame(rows)

    for c in ("current_spread_home", "current_spread_away", "circa_spread_home", "circa_spread_away"):
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce"); s[(s.abs() > 60)] = pd.NA; df[c] = s
    if "current_total" in df.columns:
        s = pd.to_numeric(df["current_total"], errors="coerce"); s[(s < 20) | (s > 100)] = pd.NA; df["current_total"] = s

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Odds written → {out_csv}  (rows={len(df)})")

    if update_planner_path:
        update_planner(Path(update_planner_path), df, backup=backup)
    if update_survivor_path:
        update_survivor(Path(update_survivor_path), df, backup=backup)

    return out_csv, df


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch NFL odds (v4), season-wide, and update planner & survivor in one go")
    ap.add_argument("--sport", default="americanfootball_nfl", help="Sport key (e.g., americanfootball_nfl)")
    ap.add_argument("--regions", default="us", help="Regions (comma list). Use 'us' for US books")
    ap.add_argument("--from_iso", dest="from_iso", default=None, help="commenceTimeFrom filter (ISO8601 UTC)")
    ap.add_argument("--to_iso", dest="to_iso", default=None, help="commenceTimeTo filter (ISO8601 UTC)")
    ap.add_argument("--season", type=int, default=None, help="If set, infer a broad season window (Aug 1 → Feb 15)")
    ap.add_argument("--out", required=True, help="Output odds CSV path")
    ap.add_argument("--api_key", default=os.getenv("ODDS_API_KEY"), help="The Odds API key (or set env ODDS_API_KEY)")
    ap.add_argument("--update_planner", default=None, help="Path to Millions planner CSV to update (optional)")
    ap.add_argument("--update_survivor", default=None, help="Path to Survivor roadmap CSV to update (optional)")
    ap.add_argument("--backup", type=int, default=1, help="Back up CSVs before writing (1=yes, 0=no)")
    ap.add_argument("--open_offsets", default="21,10,5", help="(unused in v3.3.3 lite)")
    ap.add_argument("--history", type=int, default=0, help="(unused in v3.3.3 lite)")
    args = ap.parse_args()

    if not args.api_key:
        raise SystemExit("Missing API key. Set ODDS_API_KEY or pass --api_key")

    if args.season and not (args.from_iso or args.to_iso):
        args.from_iso, args.to_iso = season_window(args.season)

    run(api_key=args.api_key, sport=args.sport, regions=args.regions,
        out_csv=Path(args.out), commence_from=args.from_iso, commence_to=args.to_iso,
        season=args.season,
        update_planner_path=Path(args.update_planner) if args.update_planner else None,
        update_survivor_path=Path(args.update_survivor) if args.update_survivor else None,
        backup=bool(args.backup))


if __name__ == "__main__":
    main()
