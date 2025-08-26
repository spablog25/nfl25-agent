# scripts/fetch_epa_team_s2d.py
# ------------------------------------------------------------
# Purpose (Plan A):
#   Fetch team-level season-to-date EPA & Success Rate from an nflverse/nflfastR
#   team stats CSV (via direct GitHub Releases URL or a local CSV),
#   normalize columns, apply team alias map, and write a clean file:
#     data/epa_team_s2d.csv
#
# Beginner notes:
#   • You can point this at ANY compatible CSV using --url or --file.
#   • The script tries multiple likely column names and standardizes them to:
#       team, off_epa_per_play, def_epa_per_play, off_success_rate, def_success_rate, updated_at
#   • If a column is missing, you’ll get a clear error showing what was found.
#
# Usage examples:
#   python -m scripts.fetch_epa_team_s2d --url "https://<nflverse-release>/team_stats_2025.csv"
#   python -m scripts.fetch_epa_team_s2d --file "C:/path/to/team_stats.csv"
#   (optional) add --season 2025 to filter a season column if present
# ------------------------------------------------------------
from __future__ import annotations
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_PATH = DATA_DIR / "epa_team_s2d.csv"

# Minimal alias registry (extend as you encounter variants)
ALIAS = {
    "WAS": "WSH", "JAC": "JAX", "NOR": "NO", "TAM": "TB", "SFO": "SF", "ARZ": "ARI",
    "GNB": "GB", "KAN": "KC", "NWE": "NE", "SDG": "LAC", "STL": "LAR", "OAK": "LV",
    # Common full names → abbreviations
    "Washington": "WSH", "Jacksonville": "JAX", "New Orleans": "NO", "Tampa Bay": "TB",
    "San Francisco": "SF", "Arizona": "ARI", "Green Bay": "GB", "Kansas City": "KC",
    "New England": "NE", "San Diego": "LAC", "St. Louis": "LAR", "Oakland": "LV",
}

# Helper: case-insensitive column finder across multiple candidate names
def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None

# Core normalization: map source columns → our canonical schema
def normalize_input_columns(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # Common possibilities across nflverse/nflfastR aggregates
    season_cols   = ["season", "year"]
    team_cols     = ["team", "team_abbr", "abbr", "posteam", "defteam"]  # prefer offense/team_abbr
    off_epa_cols  = ["off_epa_per_play", "offense_epa_per_play", "epa_offense", "off_epa", "off_epa_pp"]
    def_epa_cols  = ["def_epa_per_play", "defense_epa_per_play", "epa_defense", "def_epa", "def_epa_pp"]
    off_sr_cols   = ["off_success_rate", "off_sr", "success_rate_offense", "off_success"]
    def_sr_cols   = ["def_success_rate", "def_sr", "success_rate_defense", "def_success"]

    col_team    = find_column(df, team_cols)
    col_off_epa = find_column(df, off_epa_cols)
    col_def_epa = find_column(df, def_epa_cols)
    col_off_sr  = find_column(df, off_sr_cols)
    col_def_sr  = find_column(df, def_sr_cols)

    missing = []
    for label, col in [
        ("team", col_team),
        ("off_epa_per_play", col_off_epa),
        ("def_epa_per_play", col_def_epa),
    ]:
        if col is None:
            missing.append(label)
    if missing:
        raise ValueError(
            f"Input file is missing required columns: {missing}.\n"
            f"Available: {list(df.columns)}\n"
            f"Tip: open the CSV once to inspect, then add a new alias/candidate if needed."
        )

    out = pd.DataFrame()
    out["team"] = df[col_team].astype(str).str.strip().map(lambda x: ALIAS.get(x, x))
    out["off_epa_per_play"] = pd.to_numeric(df[col_off_epa], errors="coerce")
    out["def_epa_per_play"] = pd.to_numeric(df[col_def_epa], errors="coerce")
    out["off_success_rate"] = pd.to_numeric(df[col_off_sr], errors="coerce") if col_off_sr else pd.NA
    out["def_success_rate"] = pd.to_numeric(df[col_def_sr], errors="coerce") if col_def_sr else pd.NA

    # Remove duplicate teams (keep last occurrence)
    out = out.drop_duplicates(subset=["team"], keep="last")

    # Stamp freshness
    out["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return out


def main():
    parser = argparse.ArgumentParser(description="Fetch/normalize team-level EPA per play (season-to-date).")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--url", type=str, help="HTTP(S) CSV endpoint (e.g., nflverse GitHub Releases)")
    src.add_argument("--file", type=str, help="Local CSV path")
    parser.add_argument("--season", type=int, default=None, help="Optional: filter to this season if the CSV has a season column")
    parser.add_argument("--out", type=str, default=str(OUT_PATH), help="Output path (default: data/epa_team_s2d.csv)")
    parser.add_argument("--show", action="store_true", help="Print a small preview after writing")
    args = parser.parse_args()

    # Load raw CSV
    if args.url:
        df_raw = pd.read_csv(args.url)
        print(f"Loaded {len(df_raw)} rows from URL")
    else:
        df_raw = pd.read_csv(args.file)
        print(f"Loaded {len(df_raw)} rows from file")

    # Optional season filter if present in the file
    if args.season is not None:
        season_col = find_column(df_raw, ["season", "year"])  # quietly ignore if not present
        if season_col is not None:
            before = len(df_raw)
            df_raw = df_raw[df_raw[season_col] == args.season]
            print(f"Filtered by {season_col} == {args.season}: {before} → {len(df_raw)} rows")
        else:
            print("Note: --season provided but no season/year column found; skipping filter.")

    # Normalize & validate
    df_norm = normalize_input_columns(df_raw)

    # Ensure output dir
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write
    df_norm.to_csv(out_path, index=False)
    print(f"Wrote {len(df_norm)} rows to {out_path}")

    if args.show:
        print("\nPreview:")
        print(df_norm.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
