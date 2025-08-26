# scripts/clean_schedule_teams.py
# --- path bootstrap: keep at very top ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ---------------------------------------
from pathlib import Path
import argparse, sys
import pandas as pd

from scripts.utils_guardrails import validate_clean_schedule
from scripts.utils_io import snapshot_csv, write_csv_atomic


PROJECT_ROOT = Path(__file__).resolve().parents[1]
IN_PATH  = PROJECT_ROOT / "data" / "2025_nfl_schedule.csv"
OUT_PATH = PROJECT_ROOT / "data" / "2025_nfl_schedule_cleaned.csv"

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

def main():
    parser = argparse.ArgumentParser(description="Clean raw NFL schedule into pipeline-ready CSV.")
    parser.add_argument("--force-write", action="store_true",
                        help="Allow overwriting cleaned schedule file.")
    args = parser.parse_args()

    if not IN_PATH.exists():
        raise FileNotFoundError(f"Raw schedule not found: {IN_PATH}")

    # Opt-in overwrite guard
    if OUT_PATH.exists() and not args.force_write:
        print(f"ℹ️ Cleaned schedule already exists:\n   {OUT_PATH}")
        print("   Skipping overwrite (use --force-write to regenerate).")
        sys.exit(0)

    # === Load raw schedule ===
    df = pd.read_csv(IN_PATH)

    # Drop unnamed/index artifact columns
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed", case=False, regex=True)]

    # Try to normalize headers. If it looks like a 9-col PFR export, rename positions.
    cols = list(df.columns)
    if len(cols) >= 9:
        std = ["Week", "Day", "Date", "VisTm", "Pts_Vis", "@", "HomeTm", "Pts_Home", "Time"]
        rename_pos = {old: std[i] for i, old in enumerate(cols[:9])}
        df = df.rename(columns=rename_pos)

    # Drop explicit home/away marker if present; we infer from teams
    if "@" in df.columns:
        df = df.drop(columns=["@"])

    # Lowercase headers for consistency
    df.columns = [c.strip().lower() for c in df.columns]

    # Ensure required columns exist
    required_any = {"week", "date", "time", "vistm", "hometm"}
    missing = required_any - set(df.columns)
    if missing:
        raise ValueError(f"Expected columns missing from raw schedule: {sorted(missing)}")

    # Remove preseason rows (PFR often labels week with 'Pre')
    df = df[~df["week"].astype(str).str.lower().str.contains("pre")].copy()

    # Parse date strings; attach 2025, roll to 2026 for January games late in season
    for i, row in df.iterrows():
        week_val = row["week"]
        ds = f"{row['date']} 2025"
        parsed = pd.to_datetime(ds, errors="coerce")
        if pd.notna(parsed) and parsed.month == 1:
            try:
                if str(week_val).isdigit() and int(week_val) >= 14:
                    parsed = parsed.replace(year=2026)
            except Exception:
                pass
        df.at[i, "date"] = parsed

    # Map team full names -> abbreviations; if already abbreviations, keep as-is
    df["vistm"]  = df["vistm"].map(TEAM_NAME_MAP).fillna(df["vistm"])
    df["hometm"] = df["hometm"].map(TEAM_NAME_MAP).fillna(df["hometm"])

    # Remove any weird BOM/encoding columns if present
    if "vÃ­stm" in df.columns:
        df = df.drop(columns=["vÃ­stm"])

    # Minimal schema our pipeline expects
    out = pd.DataFrame({
        "week":   df["week"],
        "date":   df["date"],
        "time":   df["time"],
        "vistm":  df["vistm"],
        "hometm": df["hometm"],
    })
    out.columns = out.columns.str.lower()

    # === Validate → Snapshot → Atomic write ===
    errs = validate_clean_schedule(out)
    if errs:
        raise ValueError("Refusing to write corrupted cleaned schedule:\n- " + "\n- ".join(errs))

    snapshot_csv(OUT_PATH, suffix="prewrite")
    write_csv_atomic(out, OUT_PATH)

    print("✅ Final cleaned schedule saved to:")
    print(f"   {OUT_PATH}")
    print(f"   Shape: {out.shape}; Columns: {list(out.columns)}")

if __name__ == "__main__":
    main()
