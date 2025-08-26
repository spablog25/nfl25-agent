import pandas as pd
from pathlib import Path

# Load the FINAL survivor roadmap after spot values are applied
ROADMAP = Path("picks/survivor/survivor_roadmap_expanded.csv")

df = pd.read_csv(ROADMAP)

if "spot_value" not in df.columns:
    print(
        "No 'spot_value' column found in the final roadmap — ensure spot_value_updates.py has been run and the output saved.")
else:
    # Overall counts and percentages
    counts = df["spot_value"].value_counts(dropna=False)
    percentages = (counts / counts.sum() * 100).round(1).astype(str) + "%"

    print("\nOverall Spot Value Counts:")
    print(counts)
    print("\nOverall Spot Value Percentages:")
    print(percentages)

    # Per-week counts
    print("\nPer-Week Spot Value Counts:")
    per_week_counts = df.groupby("week")["spot_value"].value_counts().unstack(fill_value=0)
    print(per_week_counts)

    # Per-week percentages
    print("\nPer-Week Spot Value Percentages:")
    per_week_percentages = per_week_counts.div(per_week_counts.sum(axis=1), axis=0).round(3) * 100
    print(per_week_percentages.astype(str) + "%")
