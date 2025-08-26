# scripts/seed_spot_values.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

# Prefer config paths if available
try:
    from scripts.config import ROOT
    CSV_PATH = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"
except Exception:
    HERE = Path(__file__).resolve().parents[1]
    CSV_PATH = HERE / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

def seed_spot_value(row: pd.Series) -> str:
    # Your actual heuristics go here. Simple placeholder:
    # Could consider projected_win_prob thresholds, home_or_away, dvoa_gap, etc.
    return "Medium"

def main():
    df = pd.read_csv(CSV_PATH)

    # Ensure 'spot_value' is object (string) so we can assign string safely
    if "spot_value" not in df.columns:
        df["spot_value"] = pd.Series([None] * len(df), dtype="object")
    else:
        df["spot_value"] = df["spot_value"].astype("object")

    # Seed only where missing/NaN/empty
    mask_missing = df["spot_value"].isna() | (df["spot_value"].astype(str).str.strip() == "")
    df.loc[mask_missing, "spot_value"] = df.loc[mask_missing].apply(seed_spot_value, axis=1)

    # If holiday_flag exists, apply special rules; otherwise skip gracefully
    if "holiday_flag" in df.columns:
        # normalize
        hf = df["holiday_flag"].fillna("").astype(str).str.lower()
        df.loc[hf.eq("thanksgiving"), "spot_value"] = "Hold"
        df.loc[hf.eq("christmas"), "spot_value"] = "Hold"

    # Optional: ensure these columns exist; if not, create them empty so downstream scripts don’t error
    for col in ["spot_value_score", "scarcity_boost"]:
        if col not in df.columns:
            df[col] = pd.NA

    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Scored and wrote: {CSV_PATH}")

if __name__ == "__main__":
    main()
