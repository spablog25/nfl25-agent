from __future__ import annotations
from pathlib import Path
import pandas as pd, sys, datetime as dt
from scripts.teams import norm_team, ABBREV_SET

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW_DIR = DATA / "ftn" / "dvoa"       # dated raw exports
PROC_DIR = DATA / "dvoa"                # processed files
PROC_DIR.mkdir(parents=True, exist_ok=True)
TS_PATH = PROC_DIR / "dvoa_timeseries_2025.csv"
LATEST_PATH = PROC_DIR / "dvoa_weekly_latest.csv"

# Set the project season here
PROJECT_SEASON = 2025

REQ_COLS = {"TEAM", "TOT DVOA"}


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize: trim, collapse spaces, upper-case
    df.columns = (
        df.columns.astype(str)
          .str.replace("\\ufeff", "", regex=True)
          .str.strip()
          .str.replace(r"\s+", " ", regex=True)
          .str.upper()
    )
    return df


def normalize_ftn(csv_path: Path, snapshot_date: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str)  # read as strings first to avoid Excel-like coercions
    df = normalize_headers(df)

    if not REQ_COLS.issubset(set(df.columns)):
        raise ValueError(f"Expected columns {sorted(REQ_COLS)}, found: {list(df.columns)}")

    # Add metadata columns
    df.insert(0, "SEASON", PROJECT_SEASON)
    df.insert(1, "SNAPSHOT_DATE", snapshot_date)

    # Ensure WEEK / YEAR present and named consistently
    if "WEEK" not in df.columns and "WEEK" in df.columns:
        pass  # kept for symmetry
    df["WEEK"] = pd.to_numeric(df.get("WEEK"), errors='coerce') if "WEEK" in df.columns else None
    if "YEAR" in df.columns:
        df["YEAR"] = pd.to_numeric(df["YEAR"], errors='coerce')

    # Normalize TEAM to our house standard
    df["TEAM"] = df["TEAM"].astype(str).apply(norm_team)

    # Parse total DVOA as percent-points (e.g., '8.8' or '8.8%')
    df["TOT_DVOA_PCT"] = pd.to_numeric(df["TOT DVOA"].str.replace('%','', regex=False), errors='coerce')

    # Filter to NFL teams and drop rows without total DVOA
    df = df[df["TEAM"].isin(ABBREV_SET)].copy()
    df = df.dropna(subset=["TOT_DVOA_PCT"])

    return df


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.ingest_ftn_dvoa_snapshot <path-to-ftn-csv>")
        sys.exit(2)

    src = Path(sys.argv[1]).expanduser()
    if not src.exists():
        raise FileNotFoundError(src)

    snap_date = dt.date.today().isoformat()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_dst = RAW_DIR / f"{snap_date}_ftn_dvoa.csv"
    raw_dst.write_bytes(src.read_bytes())

    new = normalize_ftn(raw_dst, snap_date)

    # Append to timeseries (preserve all FTN columns + our metadata)
    if TS_PATH.exists():
        ts = pd.read_csv(TS_PATH)
        ts = pd.concat([ts, new], ignore_index=True, sort=False)
    else:
        ts = new
    ts.to_csv(TS_PATH, index=False)

    # Latest view: keep most recent snapshot per TEAM
    latest = (ts.sort_values(["TEAM","SNAPSHOT_DATE"])
                .groupby(["TEAM"], as_index=False).tail(1))
    latest.to_csv(LATEST_PATH, index=False)

    print("✅ Ingested snapshot →", raw_dst.as_posix())
    print("   Updated:", TS_PATH.name)
    print("   Wrote:  ", LATEST_PATH.name)


if __name__ == "__main__":
    main()