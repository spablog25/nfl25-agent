import requests
from bs4 import BeautifulSoup
import pandas as pd

# === Step 1: Load the schedule page with browser-like headers ===
url = "https://www.espn.com/nfl/schedulegrid"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}
response = requests.get(url, headers=headers)

print(f"Status Code: {response.status_code}")
print("First 500 characters of the page:\n")
print(response.text[:500])

# === Step 2: Parse HTML ===
if response.status_code != 200:
    print("❌ Failed to load the schedule page.")
    exit()

soup = BeautifulSoup(response.text, "html.parser")
table = soup.find("table")

if table is None:
    print("⚠️ Could not find the schedule table. Exiting early.")
    exit()

# === Step 3: Extract teams and weekly matchups ===
headers = [th.get_text(strip=True) for th in table.find_all("tr")[0].find_all("th")]
teams = []
rows = table.find_all("tr")[1:]

schedule_data = []

for row in rows:
    cols = row.find_all("td")
    if not cols:
        continue

    team_name = cols[0].get_text(strip=True)
    teams.append(team_name)

    for week, opponent_cell in enumerate(cols[1:], start=1):
        opponent = opponent_cell.get_text(strip=True)
        if opponent and opponent != '—':
            home_team = team_name
            away_team = opponent.replace("@", "").strip()
            is_away = "@" in opponent

            schedule_data.append({
                "week": week,
                "team": home_team,
                "opponent": away_team,
                "home_or_away": "Away" if is_away else "Home"
            })

# === Step 4: Create DataFrame and sort ===
df = pd.DataFrame(schedule_data)
df = df.sort_values(by=["week", "team"]).reset_index(drop=True)

# === Step 5: Save to CSV ===
output_path = "../picks/survivor/nfl_2025_schedule.csv"
df.to_csv(output_path, index=False)
print(f"\n✅ Schedule saved to: {output_path}")
print(df.head(10))