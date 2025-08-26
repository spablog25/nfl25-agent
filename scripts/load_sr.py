import pandas as pd
from pathlib import Path
root = Path.cwd()
p = root/'picks'/'survivor'/'survivor_roadmap_expanded.csv'  # aka survivor_schedule_roadmap
df = pd.read_csv(p, nrows=10)
print("\n=== survivor_roadmap_expanded.csv (first 10) ===")
print(df.to_string(index=False))
(Path(root/'reports'/'handoff')).mkdir(parents=True, exist_ok=True)
df.to_csv(root/'reports'/'handoff'/'survivor_roadmap_preview.csv', index=False)
