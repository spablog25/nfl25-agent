# scripts/sync_roadmap_with_schedule.py
from __future__ import annotations
import argparse
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

# Paths
HERE = Path(__file__).resolve().parents[1]
SURV_DIR = HERE / "picks" / "survivor"
SNAP_DIR = SURV_DIR / "_snapshots"
SNAP_DIR.mkdir(parents=True, exist_ok=True)

TARGET_PATH = SURV_DIR / "survivor_roadmap_expanded.csv"

def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def safe_write_csv(df: pd.DataFrame, target: Path, overwrite: bool, label: str) -> Path:
    """
    Always write a snapshot. Only replace target if overwrite=True.
    When overwriting, make a pre-overwrite backup snapshot first.
    """
    # 1) Always write a snapshot of what we WOULD write
    snap = SNAP_DIR / f"{target.stem}_prewrite_{label}_{timestamp()}.csv"
    df.to_csv(snap, index=False)

    # 2) If not overwriting, stop here
    if not overwrite:
        print(f"ℹ️  Not overwriting. Preview snapshot written:\n   {snap}")
        return snap

    # 3) If target exists, back it up before replacing
    if target.exists():
        pre_sync = SNAP_DIR / f"{target.stem}_pre_sync_{timestamp()}.csv"
        try:
            pd.read_csv(target).to_csv(pre_sync, index=False)
            print(f"📦 Backed up current target to:\n   {pre_sync}")
        except Exception as e:
            print(f"⚠️  Could not snapshot existing file (continuing): {e}")

    # 4) Replace target atomically (write tmp → replace)
    tmp = target.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, target)
    print(f"✅ Wrote roadmap (OVERWRITE): {target}")
    return target

def main():
    parser = argparse.ArgumentParser(description="Sync roadmap with schedule (safe write).")
    parser.add_argument("--overwrite", action="store_true",
                        help="Actually replace survivor_roadmap_expanded.csv. Otherwise only snapshot/preview.")
    args = parser.parse_args()

    # === Your existing logic to build `roadmap_df` goes here ===
    # Minimal stub — replace with your current transforms:
    # (You likely read cleaned schedule + merge onto existing roadmap etc.)
    # For now, read the current file so this script still runs end-to-end:
    try:
        roadmap_df = pd.read_csv(TARGET_PATH)
    except FileNotFoundError:
        print(f"❌ Target not found to sync from: {TARGET_PATH}")
        return

    print(f"[SYNC] Prepared dataframe shape={roadmap_df.shape} cols={list(roadmap_df.columns)[:10]}...")

    safe_write_csv(roadmap_df, TARGET_PATH, args.overwrite, label="sync")

if __name__ == "__main__":
    main()
