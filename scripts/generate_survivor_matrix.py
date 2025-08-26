import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import os
import pandas as pd

# --- Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SURVIVOR_DIR = os.path.join(BASE_DIR, "picks", "survivor")

ROADMAP_CSV = os.path.join(SURVIVOR_DIR, "survivor_roadmap_expanded.csv")
if not os.path.exists(ROADMAP_CSV):
    ROADMAP_CSV = os.path.join(SURVIVOR_DIR, "survivor_schedule_roadmap.csv")

PICKS_CSV = os.path.join(SURVIVOR_DIR, "survivor_weekly_picks.csv")
MATRIX_OUT = os.path.join(SURVIVOR_DIR, "survivor_matrix.csv")

# --- Config ---
WEEKS = list(range(1, 19))      # Weeks 1–18
TG_WEEK = 13
XMAS_WEEK = 17

# Matrix column order (20 “picks” slots with holiday columns)
MATRIX_COLS = (
    [f"Week {w}" for w in range(1, 13)]      # Week 1–12
    + ["Thanksgiving"]                        # TG window
    + [f"Week {w}" for w in range(13, 17)]    # Week 13–16
    + ["Christmas"]                           # Xmas window
    + [f"Week {w}" for w in range(17, 19)]    # Week 17–18
)

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower()
    return df

def load_data():
    roadmap = pd.read_csv(ROADMAP_CSV)
    picks = pd.read_csv(PICKS_CSV) if os.path.exists(PICKS_CSV) else pd.DataFrame()
    roadmap = norm_cols(roadmap)
    picks = norm_cols(picks) if not picks.empty else picks
    return roadmap, picks

def build_maps(roadmap: pd.DataFrame):
    """Build roadmap_map and holiday_map."""
    roadmap_map = {}
    holiday_map = {}

    for _, r in roadmap.iterrows():
        week = int(r.get("week"))
        team = str(r.get("team")).upper()
        opp = str(r.get("opponent")).upper() if pd.notna(r.get("opponent")) else ""
        val = r.get("spot_value")
        spot_val = str(val).strip().title() if pd.notna(val) and str(val).strip() != "" else ""
        if opp == "BYE":
            spot_val = "BYE"
        roadmap_map[(week, team)] = spot_val

        hf = r.get("holiday_flag")
        if pd.notna(hf) and str(hf).strip():
            holiday_map[(week, team)] = str(hf).strip().title()
        else:
            # leave as-is if previously set, else empty
            holiday_map[(week, team)] = holiday_map.get((week, team), "")

    return roadmap_map, holiday_map

def build_used_weeks(picks: pd.DataFrame):
    """team -> earliest used week (only rows where used == 'yes')."""
    used_weeks = {}
    if picks.empty:
        return used_weeks
    for _, r in picks.iterrows():
        used_val = str(r.get("used")).strip().lower() if pd.notna(r.get("used")) else ""
        if used_val == "yes":
            team = str(r.get("team")).upper()
            week = int(r.get("week"))
            used_weeks[team] = min(week, used_weeks.get(team, week))
    return used_weeks

def build_matrix(roadmap: pd.DataFrame, roadmap_map, holiday_map, used_weeks):
    teams = sorted(roadmap["team"].str.upper().unique().tolist())

    # Initialize all matrix columns up front (fixes KeyError: 'Thanksgiving')
    data = {"Team": teams}
    for col in MATRIX_COLS:
        data[col] = []

    for team in teams:
        team_used_week = used_weeks.get(team, None)

        for col_label in MATRIX_COLS:
            # Holiday columns
            if col_label == "Thanksgiving":
                holiday = holiday_map.get((TG_WEEK, team), "")
                if holiday == "Thanksgiving":
                    if team_used_week == TG_WEEK:
                        cell = "USED"
                    elif team_used_week is not None and team_used_week < TG_WEEK:
                        cell = "LOCKED"
                    else:
                        cell = "TG Game"  # Mark the Thanksgiving game
                else:
                    cell = ""  # not a TG team
                data[col_label].append(cell)
                continue

            if col_label == "Christmas":
                holiday = holiday_map.get((XMAS_WEEK, team), "")
                if holiday == "Christmas":
                    if team_used_week == XMAS_WEEK:
                        cell = "USED"
                    elif team_used_week is not None and team_used_week < XMAS_WEEK:
                        cell = "LOCKED"
                    else:
                        cell = "Xmas Game"  # Mark the Christmas game
                else:
                    cell = ""  # not a Christmas team
                data[col_label].append(cell)
                continue

            # Regular weeks
            week_num = int(col_label.split()[-1])  # "Week N" -> N
            base = roadmap_map.get((week_num, team), "")  # includes BYE if present

            # USED / LOCKED rules
            if team_used_week == week_num:
                cell = "USED"
            elif team_used_week is not None and week_num > team_used_week:
                cell = "LOCKED"
            else:
                cell = base  # show BYE or spot_value or blank

            data[col_label].append(cell)

    df = pd.DataFrame(data, columns=["Team"] + MATRIX_COLS)
    return df

def main():
    roadmap, picks = load_data()
    roadmap_map, holiday_map = build_maps(roadmap)
    used_weeks = build_used_weeks(picks)
    matrix_df = build_matrix(roadmap, roadmap_map, holiday_map, used_weeks)
    matrix_df.to_csv(MATRIX_OUT, index=False)
    print(f"\n✅ Survivor matrix generated and saved to: {MATRIX_OUT}")

if __name__ == "__main__":
    main()
