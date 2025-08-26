import pandas as pd

# === Load Key CSVs ===
roadmap = pd.read_csv("../picks/survivor/survivor_roadmap.csv")
schedule = pd.read_csv("../picks/survivor/survivor_schedule_roadmap.csv")

# === Show Columns ===
print("\n[Roadmap Columns]:")
print(roadmap.columns.tolist())

print("\n[Schedule Columns]:")
print(schedule.columns.tolist())