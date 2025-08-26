import pandas as pd
from pathlib import Path

# Team name to abbreviation mapping (match DVOA to Survivor tool's abbreviations)
TEAM_NAME_MAPPING = {
    "Arizona Cardinals": "ARI",
    "Atlanta Falcons": "ATL",
    "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "Houston Texans": "HOU",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV",
    "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New England Patriots": "NE",
    "New Orleans Saints": "NO",
    "New York Giants": "NYG",
    "New York Jets": "NYJ",
    "Philadelphia Eagles": "PHI",
    "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN",
    "Washington Commanders": "WSH"  # Adjusted for Survivor tool abbreviation
}

# Load DVOA CSV file
dvoa_csv_path = Path(r"C:\Users\Spencer\OneDrive\Desktop\nfl25-agent\data\dvoa_data.csv")
dvoa_df = pd.read_csv(dvoa_csv_path)

# Print the column names to verify
print("DVOA CSV Columns:", dvoa_df.columns)

# Normalize team names in the DVOA data (apply mapping)
dvoa_df['team'] = dvoa_df['TEAM'].apply(lambda x: TEAM_NAME_MAPPING.get(x, x))  # Apply mapping to DVOA teams

# Check the first few rows of the data after applying the team name mapping
print("\nFirst few rows after applying team name mapping:")
print(dvoa_df[['TEAM', 'team', 'TOT DVOA']].head())  # Show the original and mapped team names

# Next steps will involve analyzing DVOA gaps, as planned