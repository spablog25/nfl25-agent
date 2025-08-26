from __future__ import annotations
from pathlib import Path
import pandas as pd
from scripts.teams import norm_team

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SURV = ROOT / "picks" / "survivor"
LATEST_PATH = DATA / "dvoa" / "dvoa_weekly_latest.csv"
TS_PATH = DATA / "dvoa" / "dvoa_timeseries_2025.csv"
ROADMAP = SURV / "survivor_roadmap_expanded.csv"


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=1).mean()


def main():
    # --- load latest DVOA ---
    latest = pd.read_csv(LATEST_PATH)
    latest.columns = latest.columns.str.strip().str.upper()

    level_col = "TOT_DVOA_PCT" if "TOT_DVOA_PCT" in latest.columns else (
        "TOT DVOA" if "TOT DVOA" in latest.columns else None
    )
    if level_col is None:
        raise ValueError(f"Expected TOT_DVOA_PCT or TOT DVOA in {LATEST_PATH}, found: {list(latest.columns)}")

    latest["TEAM"] = latest["TEAM"].astype(str).apply(norm_team)
    latest_level = pd.to_numeric(latest[level_col].astype(str).str.replace('%','', regex=False), errors='coerce')
    latest = latest.assign(LEVEL_PP=latest_level)

    # --- load roadmap and normalize keys ---
    sched = pd.read_csv(ROADMAP)
    if "opponent" not in sched.columns and "opponent_team" in sched.columns:
        sched = sched.rename(columns={"opponent_team": "opponent"})
    if "team" not in sched.columns and "Team" in sched.columns:
        sched = sched.rename(columns={"Team": "team"})

    for c in ("team", "opponent", "week"):
        if c not in sched.columns:
            raise KeyError(f"Roadmap missing required column: {c}")

    sched["team"] = sched["team"].astype(str).apply(norm_team)
    sched["opponent"] = sched["opponent"].astype(str).apply(norm_team)
    sched["week"] = sched["week"].astype(int)

    # --- remove any old DVOA columns so we can overwrite cleanly ---
    to_drop = [
        "team_tot_dvoa_pp", "opp_tot_dvoa_pp", "dvoa_gap_pp", "dvoa_gap_dec",
        "trend3_pp", "trend_band"
    ]
    sched = sched.drop(columns=[c for c in to_drop if c in sched.columns])

    # --- merges (explicit, no suffixes) ---
    team = latest.rename(columns={"TEAM": "team"})[["team", "LEVEL_PP"]].rename(columns={"LEVEL_PP": "team_tot_dvoa_pp"})
    opp  = latest.rename(columns={"TEAM": "opponent"})[["opponent", "LEVEL_PP"]].rename(columns={"LEVEL_PP": "opp_tot_dvoa_pp"})

    out = sched.merge(team, on="team", how="left")
    out = out.merge(opp, on="opponent", how="left")

    # quick diagnostics if anything is missing
    miss_team = out["team_tot_dvoa_pp"].isna().sum()
    miss_opp  = out["opp_tot_dvoa_pp"].isna().sum()
    if miss_team or miss_opp:
        print(f"WARN: missing team DVOA on {miss_team} rows, opp DVOA on {miss_opp} rows")

    # compute gaps
    out["dvoa_gap_pp"] = out["team_tot_dvoa_pp"] - out["opp_tot_dvoa_pp"]
    out["dvoa_gap_dec"] = out["dvoa_gap_pp"] / 100.0

    # --- trend from timeseries (EMA3) ---
    ts = pd.read_csv(TS_PATH)
    ts.columns = ts.columns.str.strip().str.upper()
    if "TOT_DVOA_PCT" not in ts.columns and "TOT DVOA" in ts.columns:
        ts["TOT_DVOA_PCT"] = pd.to_numeric(ts["TOT DVOA"].astype(str).str.replace('%','', regex=False), errors='coerce')
    for c in ("TEAM", "TOT_DVOA_PCT", "SNAPSHOT_DATE"):
        if c not in ts.columns:
            raise ValueError(f"Timeseries missing {c}")

    ts["TEAM"] = ts["TEAM"].astype(str).apply(norm_team)
    ts = ts.sort_values(["TEAM", "SNAPSHOT_DATE"])
    ts["EMA3_PP"] = ts.groupby("TEAM")["TOT_DVOA_PCT"].transform(lambda s: ema(s, 3))
    last = ts.groupby("TEAM", as_index=False).tail(1)[["TEAM", "TOT_DVOA_PCT", "EMA3_PP"]]
    last = last.rename(columns={"TEAM": "team"})
    last["trend3_pp"] = last["TOT_DVOA_PCT"] - last["EMA3_PP"]

    out = out.merge(last[["team", "trend3_pp"]], on="team", how="left")

    def band(x):
        if pd.isna(x): return "Unknown"
        if x >= 3.0: return "Up"
        if x <= -3.0: return "Down"
        return "Flat"
    out["trend_band"] = out["trend3_pp"].apply(band)

    out.to_csv(ROADMAP, index=False)
    print("✅ DVOA level+trend features written →", ROADMAP)


if __name__ == "__main__":
    main()