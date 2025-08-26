# scripts/update_spot_values.py
from pathlib import Path
import pandas as pd
import numpy as np

from scripts.paths import SURVIVOR_DIR
from scripts.utils_read import read_csv_safe
from scripts.utils_io import snapshot_csv, write_csv_atomic

# === Inputs/Outputs (schedule, not planner) ===
IN_PATH  = SURVIVOR_DIR / "survivor_schedule_roadmap_expanded.csv"
OUT_PATH = SURVIVOR_DIR / "survivor_schedule_roadmap_expanded.csv"  # overwrite in place

# === Weights & knobs (current) ===
W_WIN, W_HOME, W_REST = 0.80, 0.10, 0.10

MAX_DVOA_ADJ   = 0.06     # max +/- added to score from DVOA gap
DVOA_WIDTH     = 15.0     # ~half-saturation around +/- 15 pts
DAMPEN_AT      = 0.70     # dampen DVOA by 50% when proj win >= this
FUTURE_GOOD_T  = 0.55     # try 0.60 in backtest
W_SCARCITY_TOT = 0.10     # test up to 0.14
MAX_SCARCITY   = 0.12

HI, MED = 0.70, 0.55

def dvoa_sigmoid(gap):
    # smooth, bounded in [-1, 1]
    return np.tanh(gap / DVOA_WIDTH)

def compute_score(row):
    if pd.isna(row.get("projected_win_prob")):
        return np.nan

    win   = float(row["projected_win_prob"])
    home  = 1.0 if str(row.get("home_or_away","")).strip().lower()=="home" else 0.0
    restd = float(row.get("rest_days", 0) or 0)

    # normalize rest to a modest 0..1 scale (cap at +/-3 days)
    rest_norm = np.clip(restd, -3, 3) / 6.0 + 0.5  # -3→~0.0 , +3→~1.0

    base = W_WIN*win + W_HOME*home + W_REST*rest_norm

    # DVOA adj
    gap = float(row.get("dvoa_gap", 0) or 0)
    adj = MAX_DVOA_ADJ * dvoa_sigmoid(gap)
    if win >= DAMPEN_AT:
        adj *= 0.5

    score = base + adj

    # Scarcity boost — if this week is >= FUTURE_GOOD_T and there is no better future week
    # We’ll compute per-team after vectorizing, so return base for now
    return score

def main():
    if not IN_PATH.exists():
        raise FileNotFoundError(f"Missing schedule at {IN_PATH}. Run expand/fetch first.")

    df = read_csv_safe(IN_PATH.as_posix())

    # basic sanity
    need = {"team","week","projected_win_prob"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"Schedule missing columns: {missing}. Make sure win probs are populated first.")

    # compute base+adj score
    df["spot_value_score"] = df.apply(compute_score, axis=1).astype(float)

    # --- Scarcity boost ---
    # per team: does any FUTURE week have projected_win_prob strictly greater than this week's?
    # if not, and this week >= FUTURE_GOOD_T, give a boost scaled by "how much worse the future is"
    df["scarcity_boost"] = 0.0
    for team, g in df.groupby("team", group_keys=False):
        g = g.sort_values("week")
        # future max from each week forward
        future_best = g["projected_win_prob"][::-1].cummax()[::-1]
        # “is this week the best or tied for best?”
        no_better_future = g["projected_win_prob"] >= (future_best - 1e-12)

        # define a simple opportunity delta: current - next_best_future (ignoring ties)
        next_best_future = future_best.where(~no_better_future, g["projected_win_prob"])
        op_delta = (g["projected_win_prob"] - next_best_future).clip(lower=0)

        boost = (
            (g["projected_win_prob"] >= FUTURE_GOOD_T)
            * no_better_future.astype(int)
            * np.minimum(W_SCARCITY_TOT * (1.0 + op_delta*2.0), MAX_SCARCITY)
        ).astype(float)

        df.loc[g.index, "scarcity_boost"] = boost

    df["spot_value_score"] = (df["spot_value_score"].fillna(0) + df["scarcity_boost"]).clip(0, 1)

    # buckets
    def bucket(x):
        if pd.isna(x): return ""
        if x >= HI:   return "High"
        if x >= MED:  return "Medium"
        return "Low"

    df["spot_value"] = df["spot_value_score"].apply(bucket)

    # save safely
    snapshot_csv(OUT_PATH, suffix="prewrite")
    write_csv_atomic(df, OUT_PATH)
    print(f"✅ Scored and wrote: {OUT_PATH}")

if __name__ == "__main__":
    main()
