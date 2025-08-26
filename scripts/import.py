import pandas as pd
import os

# === Paths ===
base_dir = os.path.dirname(os.path.dirname(__file__))  # goes up from /scripts/
roadmap_file = os.path.join(base_dir, 'picks', 'survivor', 'survivor_schedule_roadmap.csv')

# === Load Data ===
df = pd.read_csv(roadmap_file)

# === Stage 1: Column + Head Check ===
print("\n[Survivor Roadmap Columns]:")
print(df.columns.tolist())

print("\n[First 10 Rows Preview]:")
print(df.head(10))

# === Thanksgiving + Christmas Filters ===
thanksgiving_teams = ["GB", "DET", "KC", "DAL", "CIN", "BAL"]
christmas_teams = ["DET", "MIN", "DAL", "WSH", "DEN", "KC"]

df_thanksgiving = df[df['team'].isin(thanksgiving_teams)]
df_christmas = df[df['team'].isin(christmas_teams)]

print("\n[Thanksgiving Games Detected]:")
print(df_thanksgiving[['week', 'team', 'opponent', 'home_or_away']])

print("\n[Christmas Games Detected]:")
print(df_christmas[['week', 'team', 'opponent', 'home_or_away']])