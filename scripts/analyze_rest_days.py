import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# === Load the survivor roadmap with rest_day data ===
csv_path = Path(__file__).resolve().parent.parent / "picks" / "survivor" / "survivor_roadmap.csv"
df = pd.read_csv(csv_path)

# Ensure 'rest_days' is numeric
df['rest_days'] = pd.to_numeric(df['rest_days'], errors='coerce')
valid_rest = df['rest_days'].dropna()

# === Print summary statistics ===
print("🔍 Summary Statistics for Rest Days:")
print(valid_rest.describe())

# === Count how many games had X rest days ===
rest_day_counts = valid_rest.value_counts().sort_index()
print("\n🔢 Rest Day Frequencies:")
print(rest_day_counts)

# === Plot rest day distribution ===
plt.figure(figsize=(10, 5))
plt.bar(rest_day_counts.index, rest_day_counts.values, color="steelblue")
plt.title("Distribution of Rest Days (2025 NFL Season)")
plt.xlabel("Rest Days")
plt.ylabel("Number of Games")
plt.grid(axis='y')
plt.xticks(rest_day_counts.index)
plt.tight_layout()
plt.show()