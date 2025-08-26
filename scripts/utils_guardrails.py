# scripts/utils_guardrails.py
import pandas as pd

# ------------------------------
# Cleaned schedule guardrails
# ------------------------------

TEAM_ABBRS = {
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB",
    "HOU","IND","JAX","KC","LV","LAC","LAR","MIA","MIN","NE","NO","NYG","NYJ",
    "PHI","PIT","SF","SEA","TB","TEN","WSH"
}

def validate_clean_schedule(df: pd.DataFrame) -> list[str]:
    errs: list[str] = []

    required = {"week","date","time","vistm","hometm"}
    missing = required - set(df.columns)
    if missing:
        errs.append(f"Missing columns: {sorted(missing)}")
        return errs

    wk = pd.to_numeric(df["week"], errors="coerce")
    if wk.isna().any():
        errs.append("Non-numeric values in 'week'")
    else:
        bad_wk = ~wk.between(1, 23)
        if bad_wk.any():
            errs.append(f"'week' outside 1..23 at rows: {wk.index[bad_wk].tolist()[:10]}")

    dt = pd.to_datetime(df["date"], errors="coerce")
    if dt.isna().any():
        errs.append(f"Unparseable dates at rows: {dt.index[dt.isna()].tolist()[:10]}")

    vis_ok = df["vistm"].astype(str).str.upper().isin(TEAM_ABBRS)
    home_ok = df["hometm"].astype(str).str.upper().isin(TEAM_ABBRS)
    if (~vis_ok).any():
        errs.append(f"Unknown vistm abbreviations at rows: {vis_ok.index[~vis_ok].tolist()[:10]}")
    if (~home_ok).any():
        errs.append(f"Unknown hometm abbreviations at rows: {home_ok.index[~home_ok].tolist()[:10]}")

    same = (df["vistm"].astype(str).str.upper() == df["hometm"].astype(str).str.upper())
    if same.any():
        errs.append(f"vistm==hometm at rows: {same.index[same].tolist()[:10]}")

    pairs = pd.DataFrame({
        "week": wk,
        "a": df["vistm"].astype(str).str.upper(),
        "b": df["hometm"].astype(str).str.upper(),
    })
    norm_a = pairs[["a","b"]].min(axis=1)
    norm_b = pairs[["a","b"]].max(axis=1)
    dup_key = pairs["week"].astype(str) + "|" + norm_a + "|" + norm_b
    dup_mask = dup_key.duplicated(keep=False)
    if dup_mask.any():
        errs.append(f"Duplicate games detected at rows: {dup_mask.index[dup_mask].tolist()[:10]}")

    return errs


# ------------------------------
# Survivor roadmap guardrails
# ------------------------------

ROADMAP_REQUIRED = {
    "week","date","team","opponent","home_or_away","time","rest_days",
    "reserved","is_locked_out","expected_avail",
    "spot_value","projected_win_prob","notes_future","spot_quality",
    "preferred","must_use","save_for_later","notes","spot_value_score"
}

def validate_roadmap(df: pd.DataFrame) -> list[str]:
    errs: list[str] = []

    missing = ROADMAP_REQUIRED - set(df.columns)
    if missing:
        errs.append(f"Missing columns: {sorted(missing)}")

    if "week" in df:
        wk = pd.to_numeric(df["week"], errors="coerce")
        if wk.isna().any():
            errs.append("Non-numeric values in 'week'")

    if "home_or_away" in df:
        hoa = df["home_or_away"].astype(str).str.strip()
        bad = ~hoa.isin(["Home","Away"])
        if bad.any():
            errs.append(f"Invalid home_or_away values at rows: {df.index[bad].tolist()[:10]}")

    if "projected_win_prob" in df:
        wp = pd.to_numeric(df["projected_win_prob"], errors="coerce")
        bad = wp.notna() & ~wp.between(0.0, 1.0)
        if bad.any():
            errs.append(f"projected_win_prob outside [0,1] at rows: {df.index[bad].tolist()[:10]}")

    if "spot_value" in df:
        sv = df["spot_value"].astype(str)
        mask = sv.ne("") & ~sv.isin(["Low","Medium","High"])
        if mask.any():
            errs.append(f"spot_value contains non-buckets at rows: {df.index[mask].tolist()[:10]}")

    return errs

