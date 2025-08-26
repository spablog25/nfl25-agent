import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import os
import pandas as pd

# --- Paths (relative to /scripts) ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
ROADMAP_PATH = os.path.join(BASE_DIR, "picks", "survivor", "survivor_roadmap_expanded.csv")
PICKS_PATH   = os.path.join(BASE_DIR, "picks", "survivor", "survivor_weekly_picks.csv")

HOLIDAY_MAP = {"Thanksgiving": ("is_thanksgiving", 1), "Christmas": ("is_christmas", 1)}

def load_csv_safe(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

def ensure_lower_cols(df):
    if not df.empty:
        df.columns = df.columns.str.lower()
    return df

def main():
    # Load sources
    roadmap = ensure_lower_cols(load_csv_safe(ROADMAP_PATH))
    picks   = ensure_lower_cols(load_csv_safe(PICKS_PATH))

    if roadmap.empty:
        print(f"⚠️ Roadmap not found or empty at: {ROADMAP_PATH}")
        return

    # Normalize expected columns (we only read what we need)
    for col in ["week","team","opponent","home_or_away","projected_win_prob","spot_value","notes_future","holiday_flag"]:
        if col not in roadmap.columns:
            roadmap[col] = pd.NA

    # Ask for input like: 4 MIN
    raw = input("Week & Team (e.g., 4 MIN): ").strip()
    parts = raw.split()
    if len(parts) != 2:
        print("⚠️ Please enter exactly two items, e.g. '4 MIN'.")
        return

    try:
        week = int(parts[0])
    except ValueError:
        print("⚠️ Week must be a number, e.g. '4 MIN'.")
        return
    team = parts[1].upper()

    # Look up the schedule row for this (week, team)
    # Prefer non-BYE rows if multiple exist
    cand = roadmap[(roadmap["week"] == week) & (roadmap["team"].str.upper() == team)]
    if cand.empty:
        print(f"⚠️ No roadmap row found for week {week}, team {team}.")
        return

    # Prefer non-BYE if present
    non_bye = cand[cand["opponent"].str.upper() != "BYE"] if "opponent" in cand.columns else cand
    row = non_bye.iloc[0] if not non_bye.empty else cand.iloc[0]

    # Build record to write
    is_tg = 1 if str(row.get("holiday_flag", "")).lower() == "thanksgiving" else 0
    is_xmas = 1 if str(row.get("holiday_flag", "")).lower() == "christmas" else 0

    record = {
        "week": week,
        "team": team,
        "used": "yes",
        "eliminated": "No",  # user can change later if needed
        "win_prob": row.get("projected_win_prob", pd.NA),
        "opponent": row.get("opponent", pd.NA),
        "home/away": row.get("home_or_away", pd.NA),
        "moneyline": pd.NA,  # fill later when we wire up odds
        "is_thanksgiving": is_tg,
        "is_christmas": is_xmas,
        "future_value": row.get("spot_value", pd.NA),
        "notes": str(row.get("notes_future", "") or "")
    }

    extra = input("Extra notes to append (optional): ").strip()
    if extra:
        record["notes"] = (record["notes"] + " | " if record["notes"] else "") + extra

    new_row = pd.DataFrame([record])

    # Create picks df if missing
    expected_cols = list(record.keys())
    if picks.empty:
        picks = pd.DataFrame(columns=expected_cols)

    # Ensure columns alignment/ordering
    for c in expected_cols:
        if c not in picks.columns:
            picks[c] = pd.NA
    picks = picks[expected_cols]

    # Overwrite protection: if week already exists, ask
    exists = picks[picks["week"] == week]
    if not exists.empty:
        ans = input(f"Week {week} already has a pick ({exists.iloc[0]['team']}). Overwrite? [y/N]: ").strip().lower()
        if ans != "y":
            print("❌ Canceled. No changes made.")
            return
        picks = picks[picks["week"] != week]  # drop existing

    # Append and save
    updated = pd.concat([picks, new_row], ignore_index=True)
    updated.to_csv(PICKS_PATH, index=False)
    print(f"✅ Logged week {week} pick: {team} -> {PICKS_PATH}")

if __name__ == "__main__":
    main()
