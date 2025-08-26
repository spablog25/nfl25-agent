# scripts/backtest_future_thresholds.py
"""
Backtest FUTURE_GOOD_THRESH choices (e.g., 0.55 vs 0.60) using project paths & utils.

Run (from repo root):
  python -m scripts.backtest_future_thresholds --roadmap picks/survivor/survivor_schedule_roadmap.csv --out reports/backtests --t1 0.55 --t2 0.60
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# ----- Project paths -----
from scripts.paths import DATA_DIR, REPORTS_DIR, REPORTS_BACKTESTS, ensure_dirs

# ----- Optional utils (use if available, else fall back to pandas) -----
def _try_import_utils():
    utils = {}
    try:
        from scripts import utils_io as uio     # type: ignore
        utils["uio"] = uio
    except Exception:
        utils["uio"] = None
    try:
        from scripts import utils_schema as us  # type: ignore
        utils["us"] = us
    except Exception:
        utils["us"] = None
    try:
        from scripts import utils_read as ur    # type: ignore
        utils["ur"] = ur
    except Exception:
        utils["ur"] = None
    return utils

UTILS = _try_import_utils()

def load_csv(path: Path) -> pd.DataFrame:
    if UTILS["uio"] and hasattr(UTILS["uio"], "read_csv"):
        return UTILS["uio"].read_csv(path)
    return pd.read_csv(path)

def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if UTILS["uio"] and hasattr(UTILS["uio"], "atomic_write_csv"):
        UTILS["uio"].atomic_write_csv(df, path)
    else:
        df.to_csv(path, index=False)

# ----- Helpers -----
def find_col(df: pd.DataFrame, needles: list[str]) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for n in needles:
        for k, v in low.items():
            if n in k:
                return v
    return None

def infer_columns(df: pd.DataFrame):
    wk = find_col(df, ["week"])
    tm = find_col(df, ["team"])
    wp = find_col(df, ["proj", "win_prob", "win prob", "prob"])
    if wk is None:
        wk = "week"
        if wk not in df.columns:
            df[wk] = 1
    if tm is None:
        tm = "team"
        if tm not in df.columns:
            df[tm] = "UNKNOWN"
    if wp is None:
        num = df.select_dtypes(include=[np.number]).columns.tolist()
        wp = num[0] if num else None

    df["_week_num"] = pd.to_numeric(df[wk], errors="coerce")
    if df["_week_num"].isna().all():
        df["_week_num"] = df.groupby(tm).cumcount() + 1

    df["_winp"] = pd.to_numeric(df[wp], errors="coerce") if wp else np.nan
    return wk, tm

def summarize_future(df_team: pd.DataFrame, t1: float, t2: float) -> pd.DataFrame:
    df_team = df_team.sort_values("_week_num").reset_index(drop=True)
    n = len(df_team)
    fmax, fg1, fg2 = [], [], []
    for i in range(n):
        fut = df_team.iloc[i+1:]
        fm  = fut["_winp"].max() if len(fut) else np.nan
        c1  = int((fut["_winp"] >= t1).sum()) if len(fut) else 0
        c2  = int((fut["_winp"] >= t2).sum()) if len(fut) else 0
        fmax.append(fm); fg1.append(c1); fg2.append(c2)
    df_team["_future_max"] = fmax
    df_team[f"_future_good_{str(t1).replace('.','_')}"] = fg1
    df_team[f"_future_good_{str(t2).replace('.','_')}"] = fg2
    return df_team

def backtest(roadmap_path: Path, out_dir: Path, t1: float, t2: float):
    ensure_dirs()
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_csv(roadmap_path)
    wk_col, tm_col = infer_columns(df)

    df_bt = (df.groupby(tm_col, as_index=False, group_keys=False)
             .apply(lambda g: summarize_future(g, t1, t2), include_groups=False))

    df_bt[f"_scarcity@{t1}"] = (df_bt["_winp"] >= t1) & (
        df_bt["_future_max"].isna() | (df_bt["_future_max"] <= df_bt["_winp"])
    )
    df_bt[f"_scarcity@{t2}"] = (df_bt["_winp"] >= t2) & (
        df_bt["_future_max"].isna() | (df_bt["_future_max"] <= df_bt["_winp"])
    )

    # outputs
    affected = df_bt.loc[df_bt[f"_scarcity@{t1}"] != df_bt[f"_scarcity@{t2}"]].copy()
    band = df_bt[(df_bt["_winp"] >= 0.55) & (df_bt["_winp"] <= 0.65)]
    band_changed = band.loc[band[f"_scarcity@{t1}"] != band[f"_scarcity@{t2}"]].copy()

    friendly_cols = [
        wk_col, tm_col, "_winp",
        f"_future_good_{str(t1).replace('.','_')}",
        f"_future_good_{str(t2).replace('.','_')}",
        "_future_max", f"_scarcity@{t1}", f"_scarcity@{t2}"
    ]

    affected_out = out_dir / "threshold_backtest_affected_rows.csv"
    band_out     = out_dir / "threshold_backtest_band_055_065_affected.csv"
    ann_out      = out_dir / "roadmap_with_backtest_annotations.csv"

    save_csv(affected[friendly_cols].sort_values([tm_col, wk_col]), affected_out)
    save_csv(band_changed[friendly_cols].sort_values([tm_col, wk_col]), band_out)
    save_csv(df_bt, ann_out)

    # console summary
    total_changes = len(affected)
    band_changes  = len(band_changed)
    bins = [0.50, 0.55, 0.60, 0.65, 0.70, 1.00]
    labels = ["50-55%", "55-60%", "60-65%", "65-70%", "70%+"]
    if total_changes:
        affected["win_prob_bucket"] = pd.cut(affected["_winp"], bins=bins, labels=labels, right=False)
        by_bucket = affected["win_prob_bucket"].value_counts().sort_index()
    else:
        by_bucket = pd.Series({lab: 0 for lab in labels})

    print("=== Backtest Summary ===")
    print(f"Roadmap: {roadmap_path}")
    print(f"Thresholds compared: {t1} vs {t2}")
    print(f"Total rows with changed scarcity behavior: {total_changes}")
    print(f"Rows changed within 0.55–0.65: {band_changes}")
    print("Changes by win prob bucket:")
    for lab in labels:
        print(f"  {lab}: {int(by_bucket.get(lab, 0))}")
    print("Artifacts saved:")
    print(f"  - {affected_out}")
    print(f"  - {band_out}")
    print(f"  - {ann_out}")

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--roadmap", default=str((Path.cwd() / "picks" / "survivor" / "survivor_schedule_roadmap.csv")),
                    help="Path to survivor_schedule_roadmap.csv")
    ap.add_argument("--out", default=str(REPORTS_BACKTESTS), help="Output directory for backtest reports")
    ap.add_argument("--t1", type=float, default=0.55, help="Lower threshold (default 0.55)")
    ap.add_argument("--t2", type=float, default=0.60, help="Upper threshold (default 0.60)")
    args = ap.parse_args(argv)

    roadmap_path = Path(args.roadmap)
    out_dir      = Path(args.out)
    backtest(roadmap_path, out_dir, args.t1, args.t2)

if __name__ == "__main__":
    main()
