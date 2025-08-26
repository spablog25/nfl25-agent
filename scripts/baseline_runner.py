import subprocess
import sys
from pathlib import Path
from datetime import datetime

def run(cmd):
    print("\n$", " ".join(cmd))
    res = subprocess.run(cmd, text=True)
    if res.returncode != 0:
        sys.exit(res.returncode)

REPO = Path(__file__).resolve().parents[1]
SCHEDULE = REPO / "data" / "2025_nfl_schedule_cleaned.csv"
STAGING  = REPO / "picks" / "survivor" / "survivor_schedule_roadmap_expanded.csv"
ROADMAP  = REPO / "picks" / "survivor" / "survivor_roadmap_expanded.csv"
PREVIEW  = REPO / "picks" / "survivor" / "spot_preview_all_baseline.csv"
DIAGDIR  = REPO / "picks" / "survivor" / "diagnostics"
STAMP    = datetime.now().strftime("%Y-%m-%d")


def main():
    DIAGDIR.mkdir(parents=True, exist_ok=True)

    # 1) Regenerate roadmap
    run([
        sys.executable, "-m", "scripts.generate_survivor_roadmap",
        "--schedule", str(SCHEDULE),
        "--staging",  str(STAGING),
        "--out",      str(ROADMAP),
    ])

    # 2) Score spot values (preview + write)
    run([
        sys.executable, "-m", "scripts.spot_value_updates",
        "--dry-run", "--out", str(PREVIEW),
    ])
    run([sys.executable, "-m", "scripts.spot_value_updates"])  # write to roadmap

    # 3) Diagnostics to files
    counts_path = DIAGDIR / f"bucket_counts_{STAMP}.txt"
    stats_path  = DIAGDIR / f"score_stats_{STAMP}.txt"

    with open(counts_path, "w", encoding="utf-8") as f:
        subprocess.run([sys.executable, "-m", "scripts.spot_value_bucket_counts"], text=True, stdout=f)
    with open(stats_path, "w", encoding="utf-8") as f:
        subprocess.run([sys.executable, "-m", "scripts.spot_value_score_stats"], text=True, stdout=f)

    # 4) Optional: quick matrix slice (Weeks 1–6)
    matrix_py = f"""
import pandas as pd
from pathlib import Path
roadmap = Path(r"{ROADMAP}")
df = pd.read_csv(roadmap)
cols = ['week','team','opponent','home_or_away','projected_win_prob','spot_value','spot_value_score']
out = (df.loc[df['week'].between(1,6), cols]
         .sort_values(['week','spot_value_score'], ascending=[True,False]))
out.to_csv(Path(r"{REPO}") / 'reports' / f'survivor_matrix_wk1_6_{STAMP}.csv', index=False)
print(f'Wrote reports/survivor_matrix_wk1_6_{STAMP}.csv')
"""
    run([sys.executable, "-c", matrix_py])

    print("\n✅ Baseline refresh complete.")
    print(f"- Preview: {PREVIEW}")
    print(f"- Roadmap: {ROADMAP}")
    print(f"- Diagnostics: {DIAGDIR}")
    print(f"- Matrix slice: {REPO / 'reports' / f'survivor_matrix_wk1_6_{STAMP}.csv'}")

if __name__ == "__main__":
    main()
