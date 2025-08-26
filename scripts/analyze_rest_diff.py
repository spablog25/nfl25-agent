import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# === Load team-level data with rest_days ===
schedule_path = Path(__file__).resolve().parent.parent / "picks" / "survivor" / "survivor_roadmap.csv"
df = pd.read_csv(schedule_path)

# 🛠 Normalize columns to lowercase (prevents KeyError: 'Date')
df.columns = [col.strip().lower() for col in df.columns]

# ✅ Convert 'date' to datetime
df['date'] = pd.to_datetime(df['date'])
# === For each game, group by week+opponent matchups and calculate rest difference ===
# Only keep the relevant fields
game_df = df[['week', 'team', 'opponent', 'rest_days', 'home_or_away']].copy()

# Split into home and away teams
home_df = game_df[game_df['home_or_away'] == 'Home'].copy()
away_df = game_df[game_df['home_or_away'] == 'Away'].copy()

# Merge home and away to get both teams’ rest days in one row
merged = pd.merge(
    home_df,
    away_df,
    left_on=['week', 'team', 'opponent'],
    right_on=['week', 'opponent', 'team'],
    suffixes=('_home', '_away')
)

# === Calculate rest day difference per game ===
merged['rest_diff'] = (merged['rest_days_home'] - merged['rest_days_away']).abs()

# === Print summary statistics ===
print("📊 Rest Day Difference Summary:")
print(merged['rest_diff'].describe())
print("\nTop Rest Mismatches:")
print(merged.sort_values(by='rest_diff', ascending=False)[['team_home', 'team_away', 'rest_days_home', 'rest_days_away', 'rest_diff']].head(10))

# === Plot distribution ===
plt.figure(figsize=(10, 6))
plt.hist(merged['rest_diff'], bins=range(0, int(merged['rest_diff'].max()) + 2), edgecolor='black')
plt.title("Rest Day Differences per Game (2025 NFL Season)")
plt.xlabel("Rest Day Difference")
plt.ylabel("Number of Games")
plt.grid(True)
plt.tight_layout()
plt.show()
