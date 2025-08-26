# ===============================
# File: scripts/odds_snapshot_logger.py
# ===============================
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
odds_snapshot_logger — save time‑stamped odds snapshots (current only)
---------------------------------------------------------------------
Purpose
- Take a lightweight snapshot of current odds and write it to
  data/odds/snapshots/<UTC stamp>.csv (e.g., 2025-08-20T0200Z.csv).
- Use these snapshots later to compute **open_* fields** and **line deltas**
  without needing The Odds API historical plan.

Usage (PowerShell)
  $env:ODDS_API_KEY = "<YOUR_KEY>"
  python scripts/odds_snapshot_logger.py \
    --season 2025 \
    --regions us \
    --out_dir data/odds/snapshots

Schedule nightly (Windows Task Scheduler / cron) to build history.
"""
from __future__ import annotations
import argparse
import os
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime, timezone

import requests
import pandas as pd

API_BASE = "https://api.the-odds-api.com/v4"

TEAM_NAME_TO_ABBR: Dict[str, str] = {
    "BUFFALO BILLS": "BUF", "MIAMI DOLPHINS": "MIA", "NEW ENGLAND PATRIOTS": "NE", "NEW YORK JETS": "NYJ",
    "BALTIMORE RAVENS": "BAL", "CINCINNATI BENGALS": "CIN", "CLEVELAND BROWNS": "CLE", "PITTSBURGH STEELERS": "PIT",
    "HOUSTON TEXANS": "HOU", "INDIANAPOLIS COLTS": "IND", "JACKSONVILLE JAGUARS": "JAX", "TENNESSEE TITANS": "TEN",
    "DENVER BRONCOS": "DEN", "KANSAS CITY CHIEFS": "KC", "LAS VEGAS RAIDERS": "LV", "LOS ANGELES CHARGERS": "LAC",
    "DALLAS COWBOYS": "DAL", "NEW YORK GIANTS": "NYG", "PHILADELPHIA EAGLES": "PHI", "WASHINGTON COMMANDERS": "WSH",
    "CHICAGO BEARS": "CHI", "DETROIT LIONS": "DET", "GREEN BAY PACKERS": "GB", "MINNESOTA VIKINGS": "MIN",
    "ATLANTA FALCONS": "ATL", "CAROLINA PANTHERS": "CAR", "NEW ORLEANS SAINTS": "NO", "TAMPA BAY BUCCANEERS": "TB",
    "ARIZONA CARDINALS": "ARI", "LOS ANGELES RAMS": "LAR", "SAN FRANCISCO 49ERS": "SF", "SEATTLE SEAHAWKS": "SEA",
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


def normalize_pair(h, a):
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
    return 100.0 / (p + 100.0) if p > 0 else (-p) / ((-p) + 100.0)


def market_lookup(book: Dict[str, Any], key: str) -> Dict[str, Any] | None:
    for m in book.get("markets", []) or []:
        if (m.get("key") or m.get("market_key") or "").lower() == key:
            return m
    return None


def season_window(season: int) -> Tuple[str, str]:
    return (f"{season}-08-01T00:00:00Z", f"{season + 1}-02-15T08:00:00Z")


def fetch_odds(api_key: str, sport: str, regions: str, commence_from: str | None, commence_to: str | None):
    url = f"{API_BASE}/sports/{sport}/odds"
    params = {"apiKey": api_key, "regions": regions, "markets": "h2h,spreads,totals", "oddsFormat": "american", "dateFormat": "iso"}
    if commence_from: params["commenceTimeFrom"] = commence_from
    if commence_to:   params["commenceTimeTo"] = commence_to
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rem = r.headers.get("x-requests-remaining"); used = r.headers.get("x-requests-used")
    if rem is not None: print(f"The Odds API: used={used} remaining={rem}")
    return r.json()


def extract_row(ev: Dict[str, Any]) -> Dict[str, Any] | None:
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
                if nm == (home_name or "").upper():
                    ml_home_prices.append(float(price))
                elif nm == (away_name or "").upper():
                    ml_away_prices.append(float(price))

        hh = aa = None
        if m_spreads and m_spreads.get("outcomes"):
            for o in m_spreads["outcomes"]:
                nm = (o.get("name") or "").upper(); pt = o.get("point")
                if nm == (home_name or "").upper():
                    hh = pt
                elif nm == (away_name or "").upper():
                    aa = pt
        hh, aa = normalize_pair(hh, aa)
        if hh is not None: spread_home_pts.append(float(hh))
        if aa is not None: spread_away_pts.append(float(aa))

        if m_totals and m_totals.get("outcomes"):
            tpt = None
            for o in m_totals["outcomes"]:
                nm = (o.get("name") or "").lower()
                if nm in ("over", "under") and o.get("point") is not None:
                    tpt = float(o["point"])
                    break
            if tpt is not None: total_pts.append(tpt)

        if "circa" in title:
            if hh is not None and aa is not None:
                circa_h, circa_a = hh, aa
            if m_totals and m_totals.get("outcomes"):
                for o in m_totals["outcomes"]:
                    if (o.get("name") or "").lower() == "over" and o.get("point") is not None:
                        circa_total = float(o["point"]); break

    med = (lambda vals: float(pd.Series(vals).astype(float).median())) if spread_home_pts or spread_away_pts or total_pts or ml_home_prices or ml_away_prices else (lambda vals: None)
    cur_h = med(spread_home_pts) if spread_home_pts else None
    cur_a = med(spread_away_pts) if spread_away_pts else None
    cur_h, cur_a = normalize_pair(cur_h, cur_a)
    cur_tot = med(total_pts) if total_pts else None
    cur_ml_home = med(ml_home_prices) if ml_home_prices else None
    cur_ml_away = med(ml_away_prices) if ml_away_prices else None

    win_home = american_to_prob(cur_ml_home) if cur_ml_home is not None else None
    win_away = american_to_prob(cur_ml_away) if cur_ml_away is not None else None

    return {
        "snapshot_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "event_id": ev.get("id"),
        "commence_time": ev.get("commence_time"),
        "home_team": h, "away_team": a,
        "current_spread_home": cur_h, "current_spread_away": cur_a, "current_total": cur_tot,
        "current_ml_home": cur_ml_home, "current_ml_away": cur_ml_away,
        "win_prob_home": win_home, "win_prob_away": win_away,
        "circa_spread_home": circa_h, "circa_spread_away": circa_a, "circa_total": circa_total,
    }


def main():
    ap = argparse.ArgumentParser(description="Snapshot current NFL odds to data/odds/snapshots/")
    ap.add_argument("--api_key", default=os.getenv("ODDS_API_KEY"))
    ap.add_argument("--sport", default="americanfootball_nfl")
    ap.add_argument("--regions", default="us")
    ap.add_argument("--from_iso", default=None)
    ap.add_argument("--to_iso", default=None)
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--out_dir", default="data/odds/snapshots")
    args = ap.parse_args()

    if not args.api_key:
        raise SystemExit("Set ODDS_API_KEY or pass --api_key")

    if args.season and not (args.from_iso or args.to_iso):
        args.from_iso, args.to_iso = season_window(args.season)

    raw = fetch_odds(args.api_key, args.sport, args.regions, args.from_iso, args.to_iso)
    rows: List[Dict[str, Any]] = []
    for ev in raw:
        row = extract_row(ev)
        if row:
            rows.append(row)
    if not rows:
        print("No rows to snapshot — exiting cleanly."); return

    df = pd.DataFrame(rows)
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    out_path = out_dir / f"{stamp}.csv"
    df.to_csv(out_path, index=False)
    print(f"Snapshot written → {out_path} (rows={len(df)})")

if __name__ == "__main__":
    main()
