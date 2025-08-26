import os
from pathlib import Path

# Define the directory containing all your scripts
scripts_dir = Path(__file__).resolve().parent  # Assumes this script is in /scripts

# Define the old and new filenames
old_filename = "survivor_schedule_roadmap_expanded.csv"
new_filename = "survivor_roadmap_expanded.csv"

# Loop through all .py scripts in the directory
for file in scripts_dir.glob("*.py"):
    if file.name == "refactor_survivor_paths.py":
        continue  # Skip this script itself

    with open(file, "r", encoding="utf-8") as f:
        content = f.read()

    if old_filename in content:
        updated_content = content.replace(old_filename, new_filename)
        with open(file, "w", encoding="utf-8") as f:
            f.write(updated_content)
        print(f"✅ Updated: {file.name}")
    else:
        print(f"⏭️  No changes needed: {file.name}")

