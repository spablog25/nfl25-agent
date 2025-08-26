#!/usr/bin/env python3
"""
Patch: enforce uppercase + alias mapping on planner teams/opponents in-place.
Usage:
  python -m scripts.patch_normalize_aliases \
    --planner picks/millions/millions_planner.csv \
    --aliases data/seeds/team_aliases.csv

This will overwrite the planner after normalizing TEAM and OPPONENT fields.
"""
import argparse
import pandas as pd

def load_alias_map(path: str) -> dict[str,str]:
    df = pd.read_csv(path)
    m: dict[str,str] = {}
    if {"team","alias"} <= set(df.columns):
        for _,r in df.iterrows():
            a = str(r["alias"]).strip().upper()
            t = str(r["team"]).strip().upper()
            m[a] = t
    return m

def normalize_col(s: pd.Series, m: dict[str,str]) -> pd.Series:
    s2 = s.astype(str).str.strip().str.upper()
    return s2.map(lambda x: m.get(x, x))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--planner", required=True)
    ap.add_argument("--aliases", required=True)
    args = ap.parse_args()

    p = pd.read_csv(args.planner)
    amap = load_alias_map(args.aliases)
    for col in ["team","opponent"]:
        if col in p.columns:
            p[col] = normalize_col(p[col], amap)

    p.to_csv(args.planner, index=False)
    print("[normalized]", args.planner)

if __name__ == "__main__":
    main()
