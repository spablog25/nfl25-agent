# TD Tracker — Documentation, Framework & UI Delivery Plan (NFL Agent Project)

## Purpose
The TD Tracker script (`generate_td_digits.py`) logs and analyzes NFL touchdown scorers by the **last digit of their jersey number** (0–9). This is used for both fun contest tracking and potential future gambling games.

## Key Features
1. **Data Source**: Pulls play-by-play and weekly roster data from the `nfl_data_py` (nflverse) API.
2. **Jersey Digit Mapping**: Extracts the last digit from each scorer's jersey number for touchdowns only.
3. **Weekly Tracking**:
   - `--week` argument allows pulling a single week's data.
   - Merges into a master season CSV without overwriting prior weeks.
4. **Season Totals**: Automatically calculates cumulative totals by digit across the season.
5. **Multi-Year Analysis**:
   - `--start` and `--end` arguments pull a range of seasons.
   - Optional `--write-matrix` writes a cross-year digit x year CSV.
6. **ASCII Output**: Prints bar charts for easy terminal viewing.
7. **Ranking**: Sorts TOTAL season counts from highest to lowest.

## File Outputs (stored in `data/td_digits/`)
- `td_digits_weekly_{season}.csv` → Weekly breakdown (1–N + TOTAL).
- `td_digits_season_totals.csv` → One row per season, totals by digit.
- `td_digits_year_columns_{start}_{end}.csv` → Matrix of digits (rows) vs years (columns).

## Example Usage
**Full season:**
```powershell
python scripts/generate_td_digits.py --season 2024
```
**Single week:**
```powershell
python scripts/generate_td_digits.py --season 2024 --week 12
```
**Multi-year with matrix:**
```powershell
python scripts/generate_td_digits.py --start 2015 --end 2024 --write-matrix
```

## Integration Potential
- **UI/Dashboard Ready**: CSV outputs can feed charts, leaderboards, and tables.
- **Automation**: Can be scheduled weekly via Task Scheduler or cron.
- **API Integration**: Core functions can be wrapped for web APIs.
- **Historical Analysis**: Multi-year pulls for trends and statistical patterns.

## UI Delivery Plan — Target: Next Friday
**Day 1–2: Data Formatting for UI**
- Add JSON export option alongside CSV for direct use in a UI.
- Include metadata (week number, season-to-date totals, rankings) in JSON.
- Validate output schema for consistency.

**Day 3–4: UI Prototype (Local)**
- Build a simple HTML/JavaScript dashboard that reads the JSON.
- Display:
  - Weekly digit bar chart.
  - Season-to-date standings.
  - Digit rankings leaderboard.
- Add filters for season and week.

**Day 5: Contest Picks Module**
- Add a lightweight input form for tracking users’ weekly digit picks.
- Store picks in a CSV/JSON for standings comparison.

**Day 6: Styling & Polish**
- Apply basic CSS for a clean, mobile-friendly layout.
- Add tooltips for digit explanations.
- Include links to historical data/matrix.

**Day 7 (Friday): Final Testing & Delivery**
- Run through several week/season scenarios.
- Fix any data rendering or file path issues.
- Package instructions for running locally or hosting.

## Future Enhancements
1. Implement exacta/trifecta contest logic.
2. Add live data refresh during games.
3. Provide shareable public leaderboard.

This plan ensures the current data engine powers a simple, clean UI by the deadline while leaving room for game features in future updates.
