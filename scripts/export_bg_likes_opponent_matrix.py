#!/usr/bin/env python3
"""
Export a full opponent matrix (OPP (H/A)) for every team/week — Excel‑safe

What it does
- Reads your Y/N likes grid (team × weeks) and the master schedule
- Writes a wide CSV where **every** cell is "OPP (H)" or "OPP (A)"
- Optional: also adds Like_W* Y/N overlay columns

Run as a module (no Windows paths in this docstring to avoid escape issues):
  python -m scripts.export_bg_likes_opponent_matrix \
      --likes data/survivor_bg_likes_YN.csv \
      --schedule data/2025_nfl_schedule_cleaned.csv \
      --out data/survivor_bg_likes_OPP_matrix.csv \
      --with-like-flag

Notes
- Uses ASCII '-' for missing cells (Excel-safe)
- Writes with UTF‑8 BOM (utf-8-sig) so Excel opens cleanly
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

TEAMS_32 = [
    'ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET','GB',
    'HOU','IND','JAX','KC','LAC','LAR','LV','MIA','MIN','NE','NO','NYG','NYJ',
    'PHI','PIT','SEA','SF','TB','TEN','WSH'
]
WEEKS = [f"W{i}" for i in range(1,17)] + ["TG","CH"]
WEEK_TO_NUM = {**{f"W{i}": i for i in range(1,17)}, "TG": 12, "CH": 16}
MISSING = '-'  # Excel-safe placeholder


def load_likes(path: Path) -> pd.DataFrame:
    yn = pd.read_csv(path, dtype=str).fillna('')
    yn.columns = [c.strip() for c in yn.columns]
    if 'team' in yn.columns:
        yn.rename(columns={'team':'Team'}, inplace=True)
    yn['Team'] = yn['Team'].str.upper().str.strip()
    for w in WEEKS:
        if w not in yn.columns:
            yn[w] = 'N'
    missing = [t for t in TEAMS_32 if t not in yn['Team'].tolist()]
    if missing:
        yn = pd.concat([yn, pd.DataFrame({'Team': missing} | {w:'N' for w in WEEKS})], ignore_index=True)
    yn = yn[yn['Team'].isin(TEAMS_32)].drop_duplicates('Team').sort_values('Team').reset_index(drop=True)
    return yn


def load_schedule(path: Path) -> pd.DataFrame:
    sched = pd.read_csv(path, dtype=str).fillna('')
    for c in ('vistm','hometm','week'):
        if c not in sched.columns:
            raise SystemExit(f"Schedule CSV missing required column: {c}")
    for c in ('vistm','hometm'):
        sched[c] = sched[c].str.upper().str.strip().replace({'WAS':'WSH','ARZ':'ARI','LA':'LAR','JAC':'JAX'})
    sched['week_num'] = sched['week'].astype(str).str.extract(r'(\d+)')[0].astype(int)
    return sched


def build_schedule_map(sched: pd.DataFrame) -> dict:
    m: dict[str, dict[int, dict[str,str]]] = {}
    for _, r in sched.iterrows():
        v, h, wk = r['vistm'], r['hometm'], int(r['week_num'])
        if not v or not h:
            continue
        m.setdefault(v, {})[wk] = {'opp': h, 'hoa': 'A'}
        m.setdefault(h, {})[wk] = {'opp': v, 'hoa': 'H'}
    return m


def render_cell(team: str, week_label: str, sched_map: dict) -> str:
    wk = WEEK_TO_NUM[week_label]
    info = sched_map.get(team, {}).get(wk)
    if not info:
        return MISSING
    return f"{info['opp']} ({info['hoa']})"


def export_opponent_matrix(yn: pd.DataFrame, sched_map: dict, out_csv: Path, with_like_flag: bool=False):
    out = pd.DataFrame({'team': TEAMS_32})
    for w in WEEKS:
        out[w] = [render_cell(team, w, sched_map) for team in TEAMS_32]
    if with_like_flag:
        yn_idx = yn.set_index('Team')
        for w in WEEKS:
            out[f'Like_{w}'] = [
                'Y' if (team in yn_idx.index and str(yn_idx.at[team, w]).strip().upper()=='Y') else 'N'
                for team in TEAMS_32
            ]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False, encoding='utf-8-sig')  # Excel-safe
    return out_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--likes', type=Path, default=Path('data/survivor_bg_likes_YN.csv'))
    ap.add_argument('--schedule', type=Path, default=Path('data/2025_nfl_schedule_cleaned.csv'))
    ap.add_argument('--out', type=Path, default=Path('data/survivor_bg_likes_OPP_matrix.csv'))
    ap.add_argument('--with-like-flag', action='store_true')
    args = ap.parse_args()

    yn = load_likes(args.likes)
    sched = load_schedule(args.schedule)
    sched_map = build_schedule_map(sched)
    out = export_opponent_matrix(yn, sched_map, args.out, with_like_flag=args.with_like_flag)
    print('Wrote →', out)

if __name__ == '__main__':
    main()
