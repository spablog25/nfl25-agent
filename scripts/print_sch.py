# scripts/print_sch.py
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
csv_path = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"

print("Path:", csv_path.resolve())
if not csv_path.exists():
    raise FileNotFoundError(f"CSV not found at {csv_path}")

df = pd.read_csv(csv_path)
print("Modified epoch:", csv_path.stat().st_mtime)
print("Shape:", df.shape)
print("Columns:", list(df.columns))
print(df.head(5).to_string(index=False))