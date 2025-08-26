import pandas as pd

# === Load the full 2025 NFL schedule ===
schedule_path = "../picks/survivor/nfl_2025_schedule.csv"
df = pd.read_csv(schedule_path)

# === Keep only rows where the team is the home team ===
df_cleaned = df[df['home_or_away'].str.lower() == 'home'].copy()

# === Optional: sort by week and team for clarity ===
df_cleaned = df_cleaned.sort_values(by=['week', 'team']).reset_index(drop=True)

# === Save to a new CSV so original stays intact ===
output_path = "../picks/survivor/nfl_2025_schedule_cleaned.csv"
df_cleaned.to_csv(output_path, index=False)

print(f"✅ Cleaned schedule saved to: {output_path}")
print(df_cleaned.head())