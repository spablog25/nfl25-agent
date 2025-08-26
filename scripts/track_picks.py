import pandas as pd
import os

# === Paths ===
base_dir = os.path.dirname(os.path.dirname(__file__))  # goes up from /scripts/
millions_dir = os.path.join(base_dir, 'picks', 'millions')
survivor_dir = os.path.join(base_dir, 'picks', 'survivor')

# === Files ===
millions_planner_file = os.path.join(millions_dir, 'millions_planner.csv')
survivor_picks_file = os.path.join(survivor_dir, 'survivor_weekly_picks.csv')
survivor_usage_file = os.path.join(survivor_dir, 'survivor_usage_tracker.csv')

# === Load CSVs ===
millions_df = pd.read_csv(millions_planner_file)
survivor_df = pd.read_csv(survivor_picks_file)
usage_df = pd.read_csv(survivor_usage_file)

# === Normalize columns to lowercase ===
millions_df.columns = millions_df.columns.str.strip().str.lower()
survivor_df.columns = survivor_df.columns.str.strip().str.lower()
usage_df.columns = usage_df.columns.str.strip().str.lower()

# === Debug check: print survivor columns once ===
print("DEBUG Survivor columns:", survivor_df.columns.tolist())  # Can remove after it works

# === Config ===
week_to_view = 3  # Change this to switch weeks

# === Helper ===
def filter_picks_by_week(df, week):
    return df[df["week"] == week]

# === Output ===
print(f"\n=== Millions Weekly Picks ===")
print(filter_picks_by_week(millions_df, week_to_view))

print(f"\n=== Survivor Weekly Picks ===")
print(filter_picks_by_week(survivor_df, week_to_view))

print(f"\n=== Survivor Team Usage ===")
print(usage_df.sum(numeric_only=True).sort_values(ascending=False).head(5))

print("\n[DEBUG] Survivor DataFrame Columns:", survivor_df.columns.tolist())  # <- ADD THIS


print(f"\n🏈 NFL25 Agent Script is Running")