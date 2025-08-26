import pandas as pd
from pathlib import Path

csv_path = Path(r"C:\Users\Spencer\OneDrive\Desktop\nfl25-agent\picks\survivor\survivor_roadmap_expanded.csv")
df = pd.read_csv(csv_path)

# Normalize column names
df.columns = [col.strip().lower() for col in df.columns]

# Print basic info
print("✅ Loaded file with", len(df), "rows")
print("🔍 Sample win_prob values:")
print(df['projected_win_prob'].head(10))

# Check for missing or invalid values
missing = df['projected_win_prob'].isna().sum()
non_numeric = pd.to_numeric(df['projected_win_prob'], errors='coerce').isna().sum()

print(f"\n⚠️ Missing values: {missing}")
print(f"⚠️ Non-numeric values: {non_numeric}")
