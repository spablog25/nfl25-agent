#!/usr/bin/env python3
# --- path bootstrap: keep at very top ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ---------------------------------------

"""
Spot Value updater — recomputes scores for ALL rows (pandas‑compat)

This version removes the Pandas 2.1+ only argument `include_groups` so it works on
older Pandas too. It also guards against NaNs and supports week‑scoped previews.

Usage examples (from repo root):
  # Full season, write to roadmap
  python -m scripts.spot_value_updates

  # Preview only (no overwrite)
  python -m scripts.spot_value_updates --dry-run

  # Limit to specific weeks and preview to a custom file
  python -m scripts.spot_value_updates --week 2,4-6 --dry-run --out "picks/survivor/spot_preview.csv"
"""

import argparse
import numpy as np
import pandas as pd

# Optional utils (used if present)
try:
    from scripts.utils_read import read_csv_safe
except Exception:
    read_csv_safe = None

try:
    from scripts.utils_schema import coerce_roadmap_dtypes
except Exception:
    coerce_roadmap_dtypes = None

try:
    from scripts.utils_guardrails import validate_roadmap
except Exception:
    validate_roadmap = None

try:
    from scripts.utils_io import snapshot_csv, write_csv_atomic
except Exception:
    snapshot_csv = None
    write_csv_atomic = None

ROADMAP = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

# ---------------- Tunables ----------------
W_WIN   = 0.80
W_HOME  = 0.10
W_REST  = 0.10

W_RATING      = 0.10   # rating_gap / power_gap contribution
RATING_WIDTH  = 6.0

MAX_DVOA_ADJ = 0.06
DVOA_WIDTH   = 15.0

W_INJURY   = 1.0
INJURY_CAP = 0.05

W_HOLIDAY = 0.03  # penalty on TG/BF/Christmas

FUTURE_GOOD_THRESH   = 0.55
W_SCARCITY_TOTAL     = 0.10
OD_MARGIN_LOW        = -0.20
OD_MARGIN_HIGH       =  0.30
MAX_SCARCITY_CONTRIB = 0.12

HI_THRESH   = 0.70
MED_THRESH  = 0.55

essential_cols = ["week","team","opponent","home_or_away","rest_days","projected_win_prob"]


def _clamp(v, lo, hi):
    return lo if v < lo else hi if v > hi else v


def _parse_weeks(s: str | None):
    if not s or str(s).lower() == "all":
        return None
    parts = [p.strip() for p in str(s).split(',') if p.strip()]
    weeks = set()
    for p in parts:
        if '-' in p:
            a, b = p.split('-', 1)
            weeks.update(range(int(a), int(b) + 1))
        else:
            weeks.add(int(p))
    return sorted(weeks)


def read_roadmap(path: Path) -> pd.DataFrame:
    df = read_csv_safe(path) if read_csv_safe else pd.read_csv(path)
    if coerce_roadmap_dtypes:
        df = coerce_roadmap_dtypes(df)
    return df


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure required/optional columns exist with safe defaults."""
    for col, default in {
        "projected_win_prob": 0.50,
        "home_or_away": "Home",
        "rest_days": 6,
        # optional layers
        "rating_gap": np.nan,
        "power_gap": np.nan,
        "injury_adjustment": 0.0,
        "holiday_flag": "",
        # DVOA variants
        "team_tot_dvoa": np.nan,
        "opp_tot_dvoa": np.nan,
        "team_total_dvoa": np.nan,
        "opp_total_dvoa": np.nan,
    }.items():
        if col not in df.columns:
            df[col] = default

    df["projected_win_prob"] = pd.to_numeric(df["projected_win_prob"], errors="coerce").fillna(0.50)
    df["week"] = pd.to_numeric(df.get("week"), errors="coerce")
    df["rest_days"] = pd.to_numeric(df.get("rest_days"), errors="coerce").fillna(6)
    df["home_or_away"] = df.get("home_or_away", "Home").astype(str).str.title()

    # rating gap: prefer rating_gap; fallback to power_gap; missing -> 0 adj
    rg = pd.to_numeric(df.get("rating_gap"), errors="coerce")
    if rg.isna().all() and "power_gap" in df.columns:
        rg = pd.to_numeric(df["power_gap"], errors="coerce")
    df["rating_gap"] = rg

    df["injury_adjustment"] = pd.to_numeric(df.get("injury_adjustment"), errors="coerce").fillna(0.0)
    return df


def base_score(df: pd.DataFrame) -> pd.DataFrame:
    """Recompute base score from win prob, home/away, rest."""
    win_norm = (df["projected_win_prob"].clip(0.30, 0.85) - 0.30) / (0.85 - 0.30)
    home_norm = (df["home_or_away"] == "Home").astype(float)
    rest_norm = ((df["rest_days"].astype(float).clip(4, 10) - 4) / (10 - 4))

    df["sv_win"]  = W_WIN  * win_norm
    df["sv_home"] = W_HOME * home_norm
    df["sv_rest"] = W_REST * rest_norm

    df["spot_value_score"] = (df["sv_win"] + df["sv_home"] + df["sv_rest"]).clip(0, 1)
    return df


def add_rating_component(df: pd.DataFrame) -> pd.DataFrame:
    g = pd.to_numeric(df.get("rating_gap"), errors="coerce").fillna(0.0)
    rating_adj = W_RATING * np.tanh(g / RATING_WIDTH)
    df["sv_rating"] = rating_adj
    df["spot_value_score"] = (df["spot_value_score"] + rating_adj).clip(0, 1)
    return df


def add_dvoa_component(df: pd.DataFrame) -> pd.DataFrame:
    team_dvoa = pd.to_numeric(
        df["team_tot_dvoa"].where(df["team_tot_dvoa"].notna(), df.get("team_total_dvoa")),
        errors="coerce"
    )
    opp_dvoa = pd.to_numeric(
        df["opp_tot_dvoa"].where(df["opp_tot_dvoa"].notna(), df.get("opp_total_dvoa")),
        errors="coerce"
    )
    gap = (team_dvoa - opp_dvoa).fillna(0.0)
    dv_adj = MAX_DVOA_ADJ * np.tanh(gap / DVOA_WIDTH)
    pnow = df["projected_win_prob"].astype(float).fillna(0.0)
    dv_adj = np.where(pnow >= 0.70, dv_adj * 0.5, dv_adj)
    df["sv_dvoa"] = dv_adj
    df["spot_value_score"] = (df["spot_value_score"] + dv_adj).clip(0, 1)
    return df


def add_injury_component(df: pd.DataFrame) -> pd.DataFrame:
    inj = pd.to_numeric(df.get("injury_adjustment"), errors="coerce").fillna(0.0)
    inj = inj.clip(-INJURY_CAP, INJURY_CAP)
    df["sv_injury"] = W_INJURY * inj
    df["spot_value_score"] = (df["spot_value_score"] + df["sv_injury"]).clip(0, 1)
    return df


def add_holiday_penalty(df: pd.DataFrame) -> pd.DataFrame:
    is_holiday = df.get("holiday_flag", "").astype(str).str.title().isin(["Thanksgiving", "Christmas"]) \
                 & df.get("holiday_flag").notna()
    df["sv_holiday"] = np.where(is_holiday, -W_HOLIDAY, 0.0)
    df["spot_value_score"] = (df["spot_value_score"] + df["sv_holiday"]).clip(0, 1)
    return df


def add_future_scarcity_boost(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["team", "week"]).reset_index(drop=True)
    df["week"] = pd.to_numeric(df["week"], errors="coerce")
    df["projected_win_prob"] = pd.to_numeric(df["projected_win_prob"], errors="coerce").fillna(0.0)

    def per_team_future_metrics(g: pd.DataFrame) -> pd.DataFrame:
        p = g["projected_win_prob"].values
        rev_cummax = np.maximum.accumulate(p[::-1])[::-1]
        max_future_prob = np.roll(rev_cummax, -1); max_future_prob[-1] = 0.0
        future_good = (p >= FUTURE_GOOD_THRESH).astype(int)
        rev_cumsum = np.cumsum(future_good[::-1])[::-1]
        future_depth_ge = np.roll(rev_cumsum, -1); future_depth_ge[-1] = 0
        gg = g.copy()
        gg["max_future_prob"] = max_future_prob
        col = f"future_depth_ge{int(round(FUTURE_GOOD_THRESH*100))}"
        gg[col] = future_depth_ge
        return gg

    # pandas‑compat: DO NOT pass include_groups (older pandas errors). group_keys=False preserves shape.
    df = df.groupby("team", group_keys=False).apply(per_team_future_metrics)

    df["opportunity_delta"] = df["projected_win_prob"] - df["max_future_prob"]

    def norm_od(x):
        lo, hi = OD_MARGIN_LOW, OD_MARGIN_HIGH
        x = _clamp(x, lo, hi)
        return (x - lo) / (hi - lo) if hi != lo else 0.5

    df["OD_norm"] = df["opportunity_delta"].apply(norm_od)
    depth_col = f"future_depth_ge{int(round(FUTURE_GOOD_THRESH*100))}"
    df["FD_norm"] = 1.0 - (df[depth_col].clip(lower=0, upper=3) / 3.0)

    df["future_scarcity_boost_raw"] = (0.6 * df["OD_norm"] + 0.4 * df["FD_norm"]).clip(0, MAX_SCARCITY_CONTRIB)
    df["future_scarcity_boost"] = W_SCARCITY_TOTAL * (df["future_scarcity_boost_raw"] / MAX_SCARCITY_CONTRIB)

    df["sv_scarcity"] = df["future_scarcity_boost"]
    df["spot_value_score"] = (df["spot_value_score"] + df["sv_scarcity"]).clip(0, 1)
    return df


def add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    def bucket(x):
        if x >= HI_THRESH: return "High"
        if x >= MED_THRESH: return "Medium"
        return "Low"
    df["spot_value"] = df["spot_value_score"].apply(bucket)
    return df


def validate_and_write(df: pd.DataFrame, path: Path):
    if validate_roadmap:
        errs = validate_roadmap(df)
        if errs:
            raise ValueError("Roadmap validation failed:\n- " + "\n- ".join(errs))
    if snapshot_csv and path == ROADMAP:
        snapshot_csv(path)
    if write_csv_atomic:
        write_csv_atomic(df, path)
    else:
        df.to_csv(path, index=False)


def main():
    ap = argparse.ArgumentParser(description="Compute spot_value scores; optional week filter and preview")
    ap.add_argument("--week", type=str, default="all", help="Week number, comma list, or range (e.g., 7 or 7,9-11). Use 'all' for full season.")
    ap.add_argument("--dry-run", action="store_true", help="Write to a preview CSV instead of overwriting the roadmap")
    ap.add_argument("--out", type=Path, default=None, help="Custom output path (implies dry-run)")
    args = ap.parse_args()

    if not ROADMAP.exists():
        raise FileNotFoundError(f"Roadmap not found: {ROADMAP}")

    df_orig = read_roadmap(ROADMAP)

    # sanity: essential columns must exist
    for c in essential_cols:
        if c not in df_orig.columns:
            raise SystemExit(f"Missing required column in roadmap: {c}")

    df = ensure_columns(df_orig.copy())
    df = base_score(df)
    df = add_rating_component(df)
    df = add_dvoa_component(df)
    df = add_injury_component(df)
    df = add_holiday_penalty(df)
    df = add_future_scarcity_boost(df)
    df = add_buckets(df)

    # guard: fail loudly if any rows remain NaN
    if df["spot_value_score"].isna().any():
        missing = int(df["spot_value_score"].isna().sum())
        bad = df.loc[df["spot_value_score"].isna(), ["week","team","opponent","projected_win_prob","home_or_away","rest_days","rating_gap"]].head(12)
        raise RuntimeError(f"spot_value_updates: {missing} rows have NaN spot_value_score after compute\nSample:\n{bad.to_string(index=False)}")

    weeks = _parse_weeks(args.week)
    if weeks is not None:
        mask = df["week"].isin(weeks)
        df_final = df_orig.copy()
        for col in df.columns:
            if col in df_final.columns:
                df_final.loc[mask, col] = df.loc[mask, col]
            else:
                df_final[col] = pd.NA
                df_final.loc[mask, col] = df.loc[mask, col]
    else:
        df_final = df

    target_path = ROADMAP
    if args.dry_run or args.out:
        target_path = args.out or (ROADMAP.parent / (f"survivor_roadmap_spot_preview_wk{','.join(map(str, weeks)) if weeks else 'all'}.csv"))
        df_final.to_csv(target_path, index=False)
        try:
            rel = target_path.relative_to(ROOT)
        except Exception:
            rel = target_path
        print(f"📝 Preview written to: {rel}")
        return

    validate_and_write(df_final, target_path)
    print(f"✅ Updated spot values saved to: {target_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
