import pandas as pd
from pathlib import Path

# === Load raw schedule ===
input_path = Path(__file__).resolve().parents[1] / "data" / "2025_nfl_schedule.csv"
sched_df = pd.read_csv(input_path)

# === Drop any unnamed columns (e.g., '@' column or index artifacts) ===
sched_df = sched_df.loc[:, ~sched_df.columns.str.contains('^Unnamed')]

# === Rename headers to standard format (if needed) ===
sched_df.columns = [
    "Week", "Day", "Date", "VisTm", "Pts_Vis", "@", "HomeTm", "Pts_Home", "Time"
]

# Drop the "@" column
if "@" in sched_df.columns:
    sched_df.drop(columns=["@"], inplace=True)

# === Normalize column names ===
sched_df.columns = [col.strip().lower() for col in sched_df.columns]  # all lowercase

# === Remove preseason games (e.g., 'Pre0', 'Pre1', 'Pre2', 'Pre3') ===
sched_df = sched_df[~sched_df['week'].astype(str).str.lower().str.contains('pre')]

# === Parse proper year into 'date' column ===
for i, row in sched_df.iterrows():
    week = row["week"]
    date_str = str(row["date"]) + " 2025"
    parsed_date = pd.to_datetime(date_str, errors="coerce")

    # Handle games in January belonging to following year
    if parsed_date.month == 1 and str(week).isdigit() and int(week) >= 14:
        parsed_date = parsed_date.replace(year=2026)

    sched_df.at[i, "date"] = parsed_date

# === Map full team names to abbreviations ===
TEAM_NAME_MAP = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE", "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC", "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR", "Miami Dolphins": "MIA", "Minnesota Vikings": "MIN",
    "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF", "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WSH"
}

sched_df["vístm"] = sched_df["vistm"].map(TEAM_NAME_MAP).fillna(sched_df["vistm"])
sched_df["hometm"] = sched_df["hometm"].map(TEAM_NAME_MAP).fillna(sched_df["hometm"])

# === Final Output ===
output_path = Path(__file__).resolve().parents[1] / "data" / "2025_nfl_schedule_cleaned.csv"
sched_df.to_csv(output_path, index=False)
print(f"✅ Cleaned and preseason-removed schedule saved to:\n{output_path}")
