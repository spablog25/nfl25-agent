from pathlib import Path
import os


# Expected key files (project-relative paths) — trimmed to current Survivor flow
EXPECTED_PATHS = [
    "data/dvoa_data.csv",
    "picks/survivor/survivor_roadmap_expanded.csv",
]

print("=== Working Directory Check ===")
print("Current working directory:", Path.cwd())
print()

errors = 0
for rel_path in EXPECTED_PATHS:
    p = Path(rel_path)
    if p.exists():
        print(f"✅ Found: {rel_path}")
    else:
        print(f"❌ MISSING: {rel_path}")
        errors += 1

print("\n=== Summary ===")
if errors == 0:
    print("All expected files are accessible with current working directory settings.")
else:
    print(f"{errors} files missing — check paths or working directory.")