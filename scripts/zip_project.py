import os
import zipfile
from pathlib import Path
import datetime

# Define project root and output zip path
project_root = Path(r"C:\Users\Spencer\OneDrive\Desktop\nfl25-agent")
output_zip = Path(r"C:\Users\Spencer\OneDrive\Desktop") / f"nfl25-agent-2025-08-20.zip"

# Folders/files to exclude
exclude_dirs = ['venv', '.idea', '__pycache__', 'diagnostics']  # Excluded diagnostics if large
exclude_extensions = ['.docx', '.doc']  # Skip Word files
exclude_files = []  # Add sensitive files, e.g., ['secrets.txt']

def zip_project(root_dir, output_path):
    skipped_files = []
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(root_dir):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            for file in files:
                if file not in exclude_files and not any(file.endswith(ext) for ext in exclude_extensions):
                    file_path = Path(root) / file
                    rel_path = file_path.relative_to(root_dir)
                    try:
                        zipf.write(file_path, rel_path)
                        print(f"Added: {rel_path}")
                    except PermissionError:
                        skipped_files.append(str(file_path))
                        print(f"Skipped (permission denied): {file_path}")
    if skipped_files:
        print("\nSkipped files due to permissions:")
        for f in skipped_files:
            print(f"- {f}")
    print(f"\nCreated zip: {output_path}")

if __name__ == "__main__":
    if project_root.exists():
        zip_project(project_root, output_zip)
    else:
        print(f"Error: Directory {project_root} not found!")