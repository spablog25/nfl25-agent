from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
from bs4 import BeautifulSoup

# Setup headless Chrome
options = Options()
options.add_argument("--headless")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=options)

# Load the PFR 2025 schedule page
url = "https://www.pro-football-reference.com/years/2025/games.htm"
driver.get(url)
time.sleep(5)

# Parse page with BeautifulSoup
html = driver.page_source
soup = BeautifulSoup(html, "html.parser")
driver.quit()

# Find the schedule table directly (no longer inside HTML comments)
table = soup.find("table", {"id": "games"})
if table is None:
    print("❌ Could not locate schedule table directly.")
    exit()

# Extract table rows
rows = table.find("tbody").find_all("tr")

# Manually define headers (account for blank columns like Date and @)
headers = ['Week', 'Day', 'Date', 'VisTm', 'Pts_Vis', '@', 'HomeTm', 'Pts_Home', 'Time']

# Parse row data
data = []
for row in rows:
    if "class" in row.attrs and "thead" in row["class"]:
        continue

    th = row.find("th").text.strip()  # Week or subheader cell
    tds = [td.text.strip() for td in row.find_all("td")]
    row_data = [th] + tds

    if len(row_data) >= 6:
        data.append(row_data)
    else:
        print(f"Skipping row (too short): {row_data}")

# Align column count if needed
if data and len(headers) > len(data[0]):
    headers = headers[:len(data[0])]
elif data and len(headers) < len(data[0]):
    headers += [f"extra_{i}" for i in range(len(headers), len(data[0]))]

# Build DataFrame
df = pd.DataFrame(data, columns=headers)

# Save cleaned schedule to /data folder
output_path = "../data/2025_nfl_schedule.csv"
df.to_csv(output_path, index=False)
print(f"✅ Schedule saved to: {output_path}")
