import pandas as pd
from pathlib import Path

# Load the cleaned schedule with full dates and team abbreviations
schedule_path = Path(r"C:\Users\Spencer\OneDrive\Desktop\nfl25-agent\data\2025_nfl_schedule_cleaned.csv")
sched_df = pd.read_csv(schedule_path)

# Remove preseason rows (e.g., "Pre0", "Pre1", etc.)
sched_df = sched_df[sched_df['Week'].astype(str).str.match(r'^\d+$')]
sched_df['Week'] = sched_df['Week'].astype(int)  # Convert to int for sorting/logic

# === Create a long-form schedule: one row per team per game ===
home_games = sched_df[['Week', 'Date', 'HomeTm', 'VisTm', 'Time']].copy()
home_games.rename(columns={'HomeTm': 'team', 'VisTm': 'opponent'}, inplace=True)
home_games['home_or_away'] = 'Home'

away_games = sched_df[['Week', 'Date', 'VisTm', 'HomeTm', 'Time']].copy()
away_games.rename(columns={'VisTm': 'team', 'HomeTm': 'opponent'}, inplace=True)
away_games['home_or_away'] = 'Away'

# Combine both into a single DataFrame
team_games = pd.concat([home_games, away_games], ignore_index=True)
team_games = team_games.sort_values(by=['team', 'Date']).reset_index(drop=True)

# Convert 'Date' column to datetime
team_games['Date'] = pd.to_datetime(team_games['Date'])

# === Calculate rest days for each team ===
team_games['rest_days'] = team_games.groupby('team')['Date'].diff().dt.days

# Fill in first game with default rest (e.g., assume 7 days before Week 1)
team_games['rest_days'] = team_games['rest_days'].fillna(7)

# Convert to int for consistency
team_games['rest_days'] = team_games['rest_days'].astype(int)

# Preview what it looks like now
print(team_games[['team', 'Week', 'Date', 'rest_days']].head(10))