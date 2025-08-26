#!/usr/bin/env python3
# Survivor spot value scorer — v1.0 (LOCKED)
# LIVE DVOA only (no projections) + scarcity + holiday highlights

from pathlib import Path
import sys, argparse
import numpy as np, pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ROADMAP = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

# ==========================
# Tuned weights & thresholds
# ==========================
# Core components
W_WIN   = 0.42
W_HOME  = 0.12
W_REST  = 0.10
W_RATING, RATING_WIDTH = 0.12, 8.0
W_INJURY, INJURY_CAP   = 0.06, 0.10
W_SCARCITY_TOTAL = 0.12  # was 0.10

# DVOA (LIVE snapshot)
W_DVOA_LEVEL   = 0.18   # was 0.16
W_DVOA_TREND   = 0.06    # was 0.05
LEVEL_CAP      = 0.20   # was 0.18
TREND_SCALE_PP = 10.0    # 10 pp ~ 1.0 unit
MAX_TREND_BONUS= 0.03
BAND_BUMP      = {"UP": 0.015, "DOWN": -0.012}  # unchanged

# Bucket thresholds (fixed; no quantiles)
HI_THRESH  = 0.51   # was 0.55
MED_THRESH = 0.41   # was 0.44

# Holiday penalties (unchanged; applied only to holiday weeks)
HOLIDAY_TG_PENALTY  = -0.12
HOLIDAY_BF_PENALTY  = -0.08
HOLIDAY_XMAS_PENALTY= -0.12
HOLIDAY_COMBO_EXTRA = -0.05

# Scarcity/Now-or-Never knobs
NOW_NEVER_MARGIN = 0.05   # need +5pp vs best remaining week to qualify
NOW_NEVER_BONUS  = 0.025 # was 0.02

# Holiday highlight (UI guidance)
HOLIDAY_HOLDOUT_SOFT = 0.00  # 0.00 = no score effect; set -0.02 to gently discourage early use


def _parse_weeks(s):
    if not s or str(s).lower() == "all":
        return None
    out = set()
    for part in str(s).split(','):
        part = part.strip()
        if not part: continue
        if '-' in part:
            a,b = part.split('-',1)
            out.update(range(int(a), int(b)+1))
        else:
            out.add(int(part))
    return sorted(out)


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    defaults = {
        "projected_win_prob": 0.50,
        "home_or_away": "Home",
        "rest_days": 6,
        "rating_gap": np.nan,
        "injury_adjustment": 0.0,
        # holiday flags
        "is_thanksgiving": 0, "is_black_friday": 0, "is_christmas": 0, "plays_both_tg_xmas": 0,
        # LIVE DVOA fields from compute_dvoa_trends.py
        "dvoa_gap_dec": np.nan,   # decimal gap of total DVOA (team - opp)/100
        "trend3_pp": np.nan,      # EMA(3) delta in pp
        "trend_band": "",        # Up / Flat / Down / Unknown
    }
    for k,v in defaults.items():
        if k not in df.columns:
            df[k] = v
    # types
    for c in ["projected_win_prob","rest_days","rating_gap","injury_adjustment","dvoa_gap_dec","trend3_pp"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["is_thanksgiving","is_black_friday","is_christmas","plays_both_tg_xmas"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    if "week" in df.columns:
        df["week"] = pd.to_numeric(df["week"], errors="coerce").astype("Int64")
    return df


def win_component(df: pd.DataFrame) -> pd.Series:
    wp = df["projected_win_prob"].clip(0,1).fillna(0.5)
    k = 6.0
    return W_WIN * (1.0 / (1.0 + np.exp(-k*(wp-0.5))))


def base_score(df: pd.DataFrame) -> pd.DataFrame:
    df["sv_win"]  = win_component(df)
    df["sv_home"] = (df["home_or_away"].str.lower()=="home").astype(float) * W_HOME
    df["sv_rest"] = ((df["rest_days"].fillna(0).clip(4,10)-4)/6.0) * W_REST
    df["spot_value_score"] = (df["sv_win"] + df["sv_home"] + df["sv_rest"]).clip(0,1)
    return df


def add_rating_component(df: pd.DataFrame) -> pd.DataFrame:
    g = df["rating_gap"].fillna(0.0)
    adj = W_RATING * np.tanh(g / RATING_WIDTH)
    df["sv_rating"] = adj
    df["spot_value_score"] = (df["spot_value_score"] + adj).clip(0,1)
    return df


def add_dvoa_component(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Level (LIVE)
    level = df["dvoa_gap_dec"].fillna(0.0).clip(-LEVEL_CAP, LEVEL_CAP)
    sv_level = W_DVOA_LEVEL * level

    # 2) Trend (LIVE)
    trend_norm = (df["trend3_pp"].fillna(0.0) / TREND_SCALE_PP).clip(-1.0, 1.0)
    sv_trend = (W_DVOA_TREND * trend_norm).clip(-MAX_TREND_BONUS, MAX_TREND_BONUS)
    if "week" in df.columns:
        early = df["week"].fillna(99).astype(int) < 5
        sv_trend = sv_trend.where(~early, sv_trend * 0.4)

    # 3) Band nudge
    bump = df["trend_band"].astype(str).str.upper().map(BAND_BUMP).fillna(0.0)

    df["sv_dvoa_level"], df["sv_dvoa_trend"], df["sv_dvoa_band"] = sv_level, sv_trend, bump
    df["sv_dvoa"] = sv_level + sv_trend + bump
    df["spot_value_score"] = (df["spot_value_score"] + df["sv_dvoa"]).clip(0,1)
    return df


def add_injury_component(df: pd.DataFrame) -> pd.DataFrame:
    inj = df["injury_adjustment"].fillna(0.0).clip(-INJURY_CAP, INJURY_CAP)
    df["sv_injury"] = W_INJURY * inj
    df["spot_value_score"] = (df["spot_value_score"] + df["sv_injury"]).clip(0,1)
    return df


def add_holiday_penalty(df: pd.DataFrame) -> pd.DataFrame:
    tg = df["is_thanksgiving"].astype(int)
    bf = df["is_black_friday"].astype(int)
    xm = df["is_christmas"].astype(int)
    both = df["plays_both_tg_xmas"].astype(int)
    base = tg*HOLIDAY_TG_PENALTY + bf*HOLIDAY_BF_PENALTY + xm*HOLIDAY_XMAS_PENALTY
    combo = (((tg+bf)>0)&(xm>0) | (both>0)&((tg+bf+xm)>0)).astype(int)*HOLIDAY_COMBO_EXTRA
    df["sv_holiday"] = base + combo
    df["spot_value_score"] = (df["spot_value_score"] + df["sv_holiday"]).clip(0,1)
    return df


def add_future_scarcity_boost(df: pd.DataFrame) -> pd.DataFrame:
    # Also computes now-or-never bump and flags
    def per_team(g: pd.DataFrame) -> pd.DataFrame:
        p = pd.to_numeric(g["projected_win_prob"], errors="coerce").fillna(0.0).to_numpy()
        rev_cummax = np.maximum.accumulate(p[::-1])[::-1]
        max_future = np.roll(rev_cummax, -1); max_future[-1] = 0.0
        g = g.copy()
        g["max_future_prob"] = max_future
        # scarcity (bonus only)
        raw = (g["projected_win_prob"] - g["max_future_prob"]).clip(lower=0.0)
        g["sv_scarcity_raw"] = (raw / 0.30).clip(0.0, 1.0)
        g["sv_scarcity"] = W_SCARCITY_TOTAL * g["sv_scarcity_raw"]
        # now-or-never
        g["is_now_or_never"] = g["projected_win_prob"] >= g["max_future_prob"] + NOW_NEVER_MARGIN
        gap_over = (g["projected_win_prob"] - g["max_future_prob"] - NOW_NEVER_MARGIN).clip(lower=0.0)
        scale = (gap_over / 0.10).clip(0.0, 1.0)
        g["sv_now_or_never"] = NOW_NEVER_BONUS * scale
        return g

    df = df.groupby("team", group_keys=False).apply(per_team)
    df["spot_value_score"] = (df["spot_value_score"] + df["sv_scarcity"] + df["sv_now_or_never"]).clip(0,1)
    return df


def add_holiday_highlights(df: pd.DataFrame) -> pd.DataFrame:
    hol_any = (df[["is_thanksgiving","is_black_friday","is_christmas"]].fillna(0).astype(int).sum(axis=1) > 0)
    df["holiday_any"] = hol_any.astype(int)
    anchor = (df[df["holiday_any"] == 1].groupby("team")["week"].min())
    df["holiday_anchor_week"] = df["team"].map(anchor).astype("Int64")
    df["is_holiday_team"] = df["holiday_anchor_week"].notna()
    df["suggest_save_for_holiday"] = df["is_holiday_team"] & (df["week"].astype("Int64").fillna(99) < df["holiday_anchor_week"].astype("Int64"))
    if HOLIDAY_HOLDOUT_SOFT != 0.0:
        soft = df["suggest_save_for_holiday"].astype(int) * HOLIDAY_HOLDOUT_SOFT
        df["sv_holiday_holdout_soft"] = soft
        df["spot_value_score"] = (df["spot_value_score"] + soft).clip(0,1)
    return df


def add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    def bucket_row(r):
        x = r["spot_value_score"]
        if x >= HI_THRESH and r.get("week", 99) <= 6: return "Medium"  # early-week demotion
        if x >= HI_THRESH: return "High"
        if x >= MED_THRESH: return "Medium"
        return "Low"
    df["spot_value"] = df.apply(bucket_row, axis=1)
    return df


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--week", type=str, default="all"); ap.add_argument("--dry-run", action="store_true"); ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    if not ROADMAP.exists():
        raise FileNotFoundError(ROADMAP)

    base = pd.read_csv(ROADMAP)
    base = base.loc[:, ~base.columns.duplicated(keep="first")]

    df = ensure_columns(base.copy())
    df = base_score(df)
    df = add_rating_component(df)
    df = add_dvoa_component(df)          # LIVE DVOA level + trend + band
    df = add_injury_component(df)
    df = add_holiday_penalty(df)
    df = add_future_scarcity_boost(df)   # scarcity + now-or-never
    df = add_holiday_highlights(df)      # UI flags (no score effect by default)
    df = add_buckets(df)                 # fixed thresholds

    weeks = _parse_weeks(args.week)
    if weeks is not None:
        mask = df["week"].isin(weeks)
        out = base.copy()
        for col in df.columns:
            if col in out.columns:
                out.loc[mask, col] = df.loc[mask, col]
            else:
                out[col] = pd.NA; out.loc[mask, col] = df.loc[mask, col]
    else:
        out = df

    target = args.out or ROADMAP
    out.to_csv(target, index=False)
    print("✅ Saved:", target)

if __name__ == "__main__":
    main()