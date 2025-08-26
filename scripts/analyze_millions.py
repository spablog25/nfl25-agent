import pandas as pd
import os

# === Paths ===
base_dir = os.path.dirname(os.path.dirname(__file__))  # Goes up from /scripts/
millions_dir = os.path.join(base_dir, 'picks', 'millions')
millions_planner_file = os.path.join(millions_dir, 'millions_planner.csv')

# === Load Millions Data ===
millions_df = pd.read_csv(millions_planner_file)
millions_df.columns = millions_df.columns.str.lower()

# === Config ===
week_to_view = 1  # Change this to view different weeks

# === Helper Function ===
def filter_by_week(df, week):
    return df[df['week'] == week]

# === Output ===
week_df = filter_by_week(millions_df, week_to_view)

print(f"\n=== Circa Millions - Week {week_to_view} ===")
print(week_df[['game_num', 'team', 'opponent', 'pick_side', 'circa_line', 'closing_line', 'line_value', 'power_rating', 'record', 'ats_record']])

print("\n🏈 Millions Week Analysis Ready")