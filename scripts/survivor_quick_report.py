# scripts/survivor_quick_report.py
# --- path bootstrap ---
# --- path bootstrap: keep at very top ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import pandas as pd

# local utils if available
try:
    from scripts.utils_read import read_csv_safe
except Exception:
    read_csv_safe = None
try:
    from scripts.utils_schema import coerce_roadmap_dtypes
except Exception:
    coerce_roadmap_dtypes = None

ROADMAP = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"
OUT_DIR = ROOT / "picks" / "survivor"


def load_roadmap() -> pd.DataFrame:
    if not ROADMAP.exists():
        raise FileNotFoundError(f"Missing roadmap: {ROADMAP}")
    df = read_csv_safe(ROADMAP) if read_csv_safe else pd.read_csv(ROADMAP)
    if coerce_roadmap_dtypes:
        df = coerce_roadmap_dtypes(df)

    # enforce numeric & basic hygiene
    df["week"] = pd.to_numeric(df.get("week"), errors="coerce")
    df["spot_value_score"] = pd.to_numeric(df.get("spot_value_score"), errors="coerce")
    if "projected_win_prob" in df.columns:
        df["projected_win_prob"] = pd.to_numeric(df["projected_win_prob"], errors="coerce").fillna(0.0)
    else:
        df["projected_win_prob"] = 0.0
    df = df.dropna(subset=["week", "spot_value_score"])
    return df


def bucket_counts_by_week(df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        df.groupby(["week", "spot_value"])["team"]
        .count()
        .unstack(fill_value=0)
        .reindex(columns=["High", "Medium", "Low"], fill_value=0)
        .reset_index()
        .sort_values("week")
    )
    return counts


def top_picks_by_week(df: pd.DataFrame, top_n: int = 1) -> pd.DataFrame:
    ordered = df.sort_values(
        ["week", "spot_value_score", "projected_win_prob"],
        ascending=[True, False, False]
    )
    cols = [c for c in ["week", "team", "opponent", "home_or_away", "date", "time",
                        "spot_value", "spot_value_score", "projected_win_prob"]
            if c in ordered.columns]
    if top_n == 1:
        picks = ordered.groupby("week", as_index=False).head(1)[cols]
        return picks.sort_values("week").assign(
            spot_value_score=lambda x: x["spot_value_score"].astype(float).round(4),
            projected_win_prob=lambda x: x["projected_win_prob"].astype(float).round(4),
        )
    picks = ordered.groupby("week", as_index=False).head(top_n)[cols]
    return picks.sort_values(["week", "spot_value_score"], ascending=[True, False]).assign(
        spot_value_score=lambda x: x["spot_value_score"].astype(float).round(4),
        projected_win_prob=lambda x: x["projected_win_prob"].astype(float).round(4),
    )


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = load_roadmap()

    counts = bucket_counts_by_week(df)
    print("\n=== Bucket counts by week (High / Medium / Low) ===")
    print(counts.to_string(index=False))

    top1 = top_picks_by_week(df, top_n=1)
    print("\n=== Top pick per week (by spot_value_score; tiebreak: projected_win_prob) ===")
    print(top1.to_string(index=False))

    top3 = top_picks_by_week(df, top_n=3)
    print("\n=== Top 3 options per week (planning depth) ===")
    print(top3.to_string(index=False))

    (OUT_DIR / "survivor_bucket_counts.csv").write_text(counts.to_csv(index=False), encoding="utf-8")
    top1.to_csv(OUT_DIR / "survivor_top_picks.csv", index=False)
    top3.to_csv(OUT_DIR / "survivor_top3_picks.csv", index=False)

    print(f"\nSaved: {OUT_DIR / 'survivor_bucket_counts.csv'}")
    print(f"Saved: {OUT_DIR / 'survivor_top_picks.csv'}")
    print(f"Saved: {OUT_DIR / 'survivor_top3_picks.csv'}")
    print("\n✅ Survivor quick report complete.")


if __name__ == "__main__":
    main()
