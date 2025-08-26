import pandas as pd
from pathlib import Path

# Load final roadmap with scores
ROADMAP = Path("picks/survivor/survivor_roadmap_expanded.csv")

df = pd.read_csv(ROADMAP)

if "spot_value_score" not in df.columns:
    print("No 'spot_value_score' column found — run spot_value_updates.py first.")
else:
    # Overall score stats
    print("\nOverall Spot Value Score Stats:")
    print(df["spot_value_score"].describe())

    # Per-week score stats
    print("\nPer-Week Spot Value Score Stats:")
    per_week_stats = df.groupby("week")["spot_value_score"].describe()
    print(per_week_stats)

    # Histogram bucket counts (0.0-0.1, 0.1-0.2, ...)
    print("\nScore Distribution Buckets:")
    bins = [i / 10 for i in range(0, 11)]
    df["score_bucket"] = pd.cut(df["spot_value_score"], bins)
    print(df["score_bucket"].value_counts().sort_index())
