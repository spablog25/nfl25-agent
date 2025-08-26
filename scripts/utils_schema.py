# scripts/utils_schema.py
from __future__ import annotations
import pandas as pd

"""
Coerce and enforce the Survivor roadmap schema without dropping columns.
- Lowercases headers (consistent with read_csv_safe)
- Ensures required columns exist (adds with sensible defaults)
- Parses booleans to nullable boolean dtype and fills defaults only where NA
- Keeps a canonical front-of-file order and appends any extra columns at the end
- Sets expected_avail default to True (unless explicitly set otherwise)
"""

# Canonical front-of-file order (others are appended after)
CANONICAL_ORDER = [
    # keys & basics
    "week", "date", "time", "team", "opponent", "home_or_away",
    # static
    "holiday_flag", "rest_days",
    # planner flags / notes
    "reserved", "is_locked_out", "expected_avail",
    "preferred", "must_use", "save_for_later",
    "notes_future", "notes", "spot_quality",
    # core inputs
    "projected_win_prob",
    # DVOA
    "team_tot_dvoa", "opp_tot_dvoa", "dvoa_gap",
    # outputs
    "spot_value", "spot_value_score",
    # placeholders / dynamic inputs (keep present even if blank)
    "moneyline", "spread", "implied_wp",
    "power_rating", "opp_power_rating", "power_gap",
    "rest_diff", "travel_miles",
    "rating_gap", "injury_adjustment", "future_scarcity_bonus",
]

# Defaults used when a column is missing; cells are filled later where NA
REQUIRED_DEFAULTS = {
    # basics
    "week": pd.NA, "date": "", "time": "", "team": "", "opponent": "", "home_or_away": "",
    # static
    "holiday_flag": "", "rest_days": pd.NA,
    # planner flags / notes
    "reserved": pd.NA, "is_locked_out": pd.NA, "expected_avail": pd.NA,
    "preferred": pd.NA, "must_use": pd.NA, "save_for_later": pd.NA,
    "notes_future": "", "notes": "", "spot_quality": "",
    # inputs
    "projected_win_prob": pd.NA,
    # DVOA
    "team_tot_dvoa": pd.NA, "opp_tot_dvoa": pd.NA, "dvoa_gap": pd.NA,
    # outputs
    "spot_value": "", "spot_value_score": pd.NA,
    # dynamic placeholders
    "moneyline": pd.NA, "spread": pd.NA, "implied_wp": pd.NA,
    "power_rating": pd.NA, "opp_power_rating": pd.NA, "power_gap": pd.NA,
    "rest_diff": pd.NA, "travel_miles": pd.NA,
    "rating_gap": pd.NA, "injury_adjustment": pd.NA, "future_scarcity_bonus": pd.NA,
}

INT_COLS = ["week", "rest_days"]
FLOAT_COLS = [
    "projected_win_prob", "team_tot_dvoa", "opp_tot_dvoa", "dvoa_gap",
    "spot_value_score", "moneyline", "spread", "implied_wp",
    "power_rating", "opp_power_rating", "power_gap",
    "rest_diff", "travel_miles",
    "rating_gap", "injury_adjustment", "future_scarcity_bonus",
]
BOOL_COLS = [
    "reserved", "is_locked_out", "expected_avail",
    "preferred", "must_use", "save_for_later",
]
STRING_COLS = [
    "date", "time", "team", "opponent", "home_or_away",
    "holiday_flag", "spot_value", "notes_future", "notes", "spot_quality",
]

TRUE_SET = {True, 1, "1", "true", "t", "yes", "y"}
FALSE_SET = {False, 0, "0", "false", "f", "no", "n"}

def _parse_bool(x):
    if pd.isna(x):
        return pd.NA
    if isinstance(x, str):
        s = x.strip()
        if s == "":
            return pd.NA
        s_low = s.lower()
        if s_low in {v.lower() if isinstance(v, str) else v for v in TRUE_SET}:
            return True
        if s_low in {v.lower() if isinstance(v, str) else v for v in FALSE_SET}:
            return False
        return pd.NA
    if x in TRUE_SET:
        return True
    if x in FALSE_SET:
        return False
    try:
        return bool(int(x))
    except Exception:
        return pd.NA


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col, default in REQUIRED_DEFAULTS.items():
        if col not in out.columns:
            out[col] = default
    return out


def coerce_roadmap_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize survivor roadmap types, preserve extras, and set sane defaults."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("coerce_roadmap_dtypes expects a pandas DataFrame")

    out = df.copy()

    # Normalize headers
    out.columns = out.columns.str.strip().str.lower()

    # Ensure required columns exist (don’t drop others)
    out = _ensure_required_columns(out)

    # Core string columns
    for c in STRING_COLS:
        if c in out.columns:
            out[c] = out[c].astype("string").fillna("")

    # Integers (nullable)
    for c in INT_COLS:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    # Floats (nullable)
    for c in FLOAT_COLS:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Float64")

    # Booleans (nullable) — keep NA so we can fill with defaults below
    for c in BOOL_COLS:
        if c in out.columns:
            out[c] = out[c].map(_parse_bool).astype("boolean")

    # Fill boolean defaults **only where NA**
    BOOL_DEFAULTS = {
        "reserved": False,
        "is_locked_out": False,
        "expected_avail": True,  # preferred default is True
        "preferred": False,
        "must_use": False,
        "save_for_later": False,
    }
    for col, default in BOOL_DEFAULTS.items():
        if col in out.columns:
            out[col] = out[col].fillna(default)

    # Normalize home/away capitalization if present
    if "home_or_away" in out.columns:
        out["home_or_away"] = out["home_or_away"].astype("string").str.strip().str.capitalize()
        mask_bad = ~out["home_or_away"].isin(["Home", "Away", ""]) & out["home_or_away"].notna()
        out.loc[mask_bad, "home_or_away"] = ""

    # Sanitize spot_value to allowed buckets or blank
    if "spot_value" in out.columns:
        allowed = pd.Index(["Low", "Medium", "High"], dtype="string")
        mask_bad = ~out["spot_value"].isin(allowed)
        out.loc[mask_bad, "spot_value"] = out.loc[mask_bad, "spot_value"].where(out.loc[mask_bad, "spot_value"].eq(""), "")

    # Reorder to canonical, append any extra columns at the end
    preferred = [c for c in CANONICAL_ORDER if c in out.columns]
    extras = [c for c in out.columns if c not in CANONICAL_ORDER]
    out = out[preferred + extras]

    return out
