
"""
Backtest FUTURE_GOOD_THRESH choices (e.g., 0.55 vs 0.60) for Survivor Tool.

USAGE (examples):
  python backtest_future_thresholds.py --roadmap /path/to/survivor_schedule_roadmap.csv --out ./reports --t1 0.55 --t2 0.60
  python backtest_future_thresholds.py --roadmap /mnt/data/survivor_roadmap_preview.csv --out /mnt/data/reports --t1 0.55 --t2 0.60

What it does:
- Loads the roadmap CSV with per-team-week win probabilities (and other fields).
- Infers columns for week/team/win_prob when names vary ("week", "team", "proj"/"win_prob"/"prob"...).
- For each team-week, computes:
    * _future_max : max projected win prob in strictly future weeks
    * _future_good_[t] : count of future weeks with win prob >= t
    * _scarcity@[t] : True if win_prob >= t and there's no strictly-better future week
- Writes artifacts:
    * threshold_backtest_affected_rows.csv               # rows where _scarcity@t1 != _scarcity@t2
    * threshold_backtest_band_055_065_affected.csv       # affected rows with 0.55 <= win_prob <= 0.65
    * roadmap_with_backtest_annotations.csv              # original + added columns above
- Prints a concise summary to stdout.
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def find_col(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        for k, v in cols.items():
            if cand in k:
                return v
    return None

def load_and_infer_cols(path):
    df = pd.read_csv(path)
    # normalize colnames
    df.columns = [c.strip() for c in df.columns]
    week_col = find_col(df, ["week"])
    team_col = find_col(df, ["team"])
    winp_col = find_col(df, ["proj", "win_prob", "win prob", "prob"])

    # fallbacks
    if week_col is None:
        week_col = "week"
        if week_col not in df.columns:
            df[week_col] = 1
    if team_col is None:
        team_col = "team"
        if team_col not in df.columns:
            df[team_col] = "UNKNOWN"
    if winp_col is None:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            winp_col = numeric_cols[0]
        else:
            winp_col = None

    df["_week_num"] = pd.to_numeric(df[week_col], errors="coerce")
    if df["_week_num"].isna().all():
        # fallback to positional week index per team
        df["_week_num"] = df.groupby(team_col).cumcount() + 1

    if winp_col is None:
        df["_winp"] = np.nan
    else:
        df["_winp"] = pd.to_numeric(df[winp_col], errors="coerce")

    return df, week_col, team_col

def summarize_future(df_team, T1, T2):
    df_team = df_team.sort_values("_week_num").reset_index(drop=True)
    n = len(df_team)
    future_max, future_good_1, future_good_2 = [], [], []
    for i in range(n):
        fut = df_team.iloc[i+1:]
        fm = fut["_winp"].max() if len(fut) else np.nan
        c1 = int((fut["_winp"] >= T1).sum()) if len(fut) else 0
        c2 = int((fut["_winp"] >= T2).sum()) if len(fut) else 0
        future_max.append(fm)
        future_good_1.append(c1)
        future_good_2.append(c2)
    df_team["_future_max"] = future_max
    df_team[f"_future_good_{str(T1).replace('.','_')}"] = future_good_1
    df_team[f"_future_good_{str(T2).replace('.','_')}"] = future_good_2
    return df_team

def run_backtest(roadmap_path: Path, out_dir: Path, T1: float, T2: float):
    out_dir.mkdir(parents=True, exist_ok=True)

    df, week_col, team_col = load_and_infer_cols(roadmap_path)
    df_bt = df.groupby(team_col, as_index=False, group_keys=False).apply(
        lambda x: summarize_future(x, T1, T2)
    )

    df_bt[f"_scarcity@{T1}"] = (df_bt["_winp"] >= T1) & (
        df_bt["_future_max"].isna() | (df_bt["_future_max"] <= df_bt["_winp"])
    )
    df_bt[f"_scarcity@{T2}"] = (df_bt["_winp"] >= T2) & (
        df_bt["_future_max"].isna() | (df_bt["_future_max"] <= df_bt["_winp"])
    )

    # Affected rows
    affected = df_bt.loc[df_bt[f"_scarcity@{T1}"] != df_bt[f"_scarcity@{T2}"]].copy()

    # Band-limited affected rows (0.55–0.65)
    band_low, band_high = 0.55, 0.65
    band_changed = df_bt[(df_bt["_winp"] >= band_low) & (df_bt["_winp"] <= band_high)].copy()
    band_changed = band_changed.loc[band_changed[f"_scarcity@{T1}"] != band_changed[f"_scarcity@{T2}"]].copy()

    # Save outputs
    affected_csv = out_dir / "threshold_backtest_affected_rows.csv"
    band_changed_csv = out_dir / "threshold_backtest_band_055_065_affected.csv"
    annotated_csv = out_dir / "roadmap_with_backtest_annotations.csv"

    # Friendly column view for affected outputs
    friendly_cols = [week_col, team_col, "_winp",
                     f"_future_good_{str(T1).replace('.','_')}",
                     f"_future_good_{str(T2).replace('.','_')}",
                     "_future_max", f"_scarcity@{T1}", f"_scarcity@{T2}"]
    affected[friendly_cols].sort_values([team_col, week_col]).to_csv(affected_csv, index=False)
    band_changed[friendly_cols].sort_values([team_col, week_col]).to_csv(band_changed_csv, index=False)
    df_bt.to_csv(annotated_csv, index=False)

    # Print concise summary
    total_changes = len(affected)
    band_changes = len(band_changed)

    # bucket summary
    bins = [0.50, 0.55, 0.60, 0.65, 0.70, 1.00]
    labels = ["50-55%", "55-60%", "60-65%", "65-70%", "70%+"]
    affected["win_prob_bucket"] = pd.cut(affected["_winp"], bins=bins, labels=labels, right=False)
    bucket_counts = affected["win_prob_bucket"].value_counts().sort_index()

    print("=== Backtest Summary ===")
    print(f"Roadmap: {roadmap_path}")
    print(f"Thresholds compared: {T1} vs {T2}")
    print(f"Total rows with changed scarcity behavior: {total_changes}")
    print(f"Rows changed within 0.55–0.65: {band_changes}")
    print("Changes by win prob bucket:")
    for b in labels:
        cnt = int(bucket_counts.get(b, 0))
        print(f"  {b}: {cnt}")
    print("Artifacts saved:")
    print(f"  - {affected_csv}")
    print(f"  - {band_changed_csv}")
    print(f"  - {annotated_csv}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--roadmap", required=True, help="Path to survivor_schedule_roadmap.csv (or preview)")
    ap.add_argument("--out", required=True, help="Output directory for reports")
    ap.add_argument("--t1", type=float, default=0.55, help="Lower threshold (default 0.55)")
    ap.add_argument("--t2", type=float, default=0.60, help="Upper threshold (default 0.60)")
    args = ap.parse_args()

    run_backtest(Path(args.roadmap), Path(args.out), args.t1, args.t2)

if __name__ == "__main__":
    main()
