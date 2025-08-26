# scripts/quick_verify_millions.py
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

def show_csv(headline: str, path: Path, n=5):
    print(f"\n=== {headline} ===")
    if not path.exists():
        print(f"Missing: {path}")
        return
    try:
        df = pd.read_csv(path)
        print(f"Rows: {len(df)} | Cols: {len(df.columns)}")
        print("Columns:", list(df.columns))
        print(df.head(n))
        # quick missingness snapshot
        miss = df.isna().mean().sort_values(ascending=False)
        print("\nTop 10 missingness (%):")
        print((miss*100).round(1).head(10))
    except Exception as e:
        print(f"Error reading {path.name}: {e}")

def main():
    show_csv("Millions Week 1 (weekly)", ROOT / "picks" / "millions" / "millions_weekly_games.csv")
    show_csv("Millions Roadmap (season)", ROOT / "picks" / "millions" / "millions_roadmap_game.csv")
    show_csv("DVOA Projections", ROOT / "data" / "2025_dvoa_projections.csv")

if __name__ == "__main__":
    main()