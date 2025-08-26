#!/usr/bin/env python3
# python scripts/planner_dedup.py --csv "picks/millions/millions_planner.csv"
# call it after A2 Enrich (and before export/audit):
from __future__ import annotations
import argparse
from pathlib import Path
import shutil
import sys
import pandas as pd


def _collapse_suffix_pairs(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Collapse base_x/base_y into base, preferring existing base → _y → _x.
    Returns (df, collapsed_base_names).
    """
    bases = sorted({c[:-2] for c in df.columns if c.endswith(("_x", "_y"))})
    collapsed: list[str] = []
    for base in bases:
        cx, cy = base + "_x", base + "_y"
        if base not in df.columns:
            df[base] = pd.NA
        if cy in df.columns:
            df[base] = df[base].where(df[base].notna(), df[cy])
        if cx in df.columns:
            df[base] = df[base].where(df[base].notna(), df[cx])
        for c in (cx, cy):
            if c in df.columns:
                del df[c]
        collapsed.append(base)
    return df, collapsed


def _drop_duplicate_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    seen: set[str] = set()
    keep: list[str] = []
    dropped: list[str] = []
    for c in df.columns:
        if c not in seen:
            keep.append(c)
            seen.add(c)
        else:
            dropped.append(c)
    return df.loc[:, keep], dropped


def _reorder_front(df: pd.DataFrame, front: list[str]) -> pd.DataFrame:
    front_present = [c for c in front if c in df.columns]
    others = [c for c in df.columns if c not in front_present]
    return df.loc[:, front_present + others]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Collapse *_x/*_y, drop duplicate columns, and optionally reorder."
    )
    ap.add_argument("--csv", required=True, help="Input CSV to clean (in-place by default).")
    ap.add_argument("--out", default=None, help="Optional output path; default overwrites --csv.")
    ap.add_argument("--no_backup", action="store_true", help="Do not write a .pre_dedup.bak.csv backup.")
    ap.add_argument("--front", default="", help="Comma-separated list of columns to put first.")
    ap.add_argument("--report_only", action="store_true", help="Only report; do not modify file.")
    args = ap.parse_args()

    src = Path(args.csv)
    if not src.exists():
        ap.error(f"File not found: {src}")

    df = pd.read_csv(src)

    # Pre-report
    dup_names_before = df.columns[df.columns.duplicated()].tolist()
    suffix_bases_before = sorted({c[:-2] for c in df.columns if c.endswith(("_x", "_y"))})

    if args.report_only:
        print("Duplicate names:", dup_names_before)
        print("Suffix bases:", suffix_bases_before)
        sys.exit(0)

    if not args.no_backup:
        bak = src.with_suffix(".pre_dedup.bak.csv")
        shutil.copy2(src, bak)
        print(f"Backup written → {bak}")

    df, collapsed = _collapse_suffix_pairs(df)
    df, dropped = _drop_duplicate_columns(df)

    front = [c.strip() for c in args.front.split(",") if c.strip()]
    if front:
        df = _reorder_front(df, front)

    out_path = Path(args.out) if args.out else src
    df.to_csv(out_path, index=False)

    # Post-report
    print("Collapsed bases:", collapsed)
    print("Dropped duplicate columns:", dropped)
    print("Remaining duplicate names:", df.columns[df.columns.duplicated()].tolist())
    print("Remaining *_x/*_y bases:", sorted({c[:-2] for c in df.columns if c.endswith(("_x", "_y"))}))
    print(f"Cleaned CSV → {out_path}")


if __name__ == "__main__":
    main()