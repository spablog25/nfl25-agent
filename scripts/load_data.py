import pandas as pd
from pathlib import Path

# Get the current working directory
root = Path.cwd()

# Define path to the CSV file
p = root / 'picks' / 'survivor' / 'survivor_matrix.csv'

# Read the first 10 rows of the CSV
df = pd.read_csv(p, nrows=10)

# Print the first 10 rows
print("\n=== survivor_matrix.csv (first 10) ===")
print(df.to_string(index=False))

# Save the preview to a new CSV file
df.to_csv(root / 'reports' / 'handoff' / 'survivor_matrix_preview.csv', index=False)
