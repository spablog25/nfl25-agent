# scripts/dvoa_columns.py
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
csv_path = ROOT / "picks" / "survivor" / "survivor_schedule_roadmap_expanded.csv"

df = pd.read_csv(csv_path)
print("Total columns:", len(df.columns))
print("First 30 cols:", df.columns[:30].tolist())

dups = df.columns[df.columns.duplicated(keep=False)]
print("Duplicate column names:", sorted(set(dups)))

print([c for c in df.columns if "dvoa" in c.lower()])