"""
Audit the weekly Millions game file for missing/odd values so we can patch the builder quickly.

Reads:  picks/millions/millions_weekly_games.csv
Outputs: console summary + optional markdown report (diagnostics/millions_week_audit.md)

Run:
  python -m scripts.millions_audit_week --week 1
  python -m scripts.millions_audit_week --week 1 --write-report
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MIL = ROOT / "picks" / "millions"
IN = MIL / "millions_weekly_games.csv"
OUT_DIR = MIL / "diagnostics"
REPORT = OUT_DIR / "millions_week_audit.md"

CORE_COLS = [
    "week","game_num","matchup","kickoff_local",
    "open_spread_home","open_spread_away",
    "circa_spread_home","circa_spread_away",
    "current_spread_home","current_spread_away",
    "line_value_home","line_value_away",
    "total_dvoa_home","total_dvoa_away",
    "off_dvoa_home","off_dvoa_away","def_dvoa_home","def_dvoa_away","st_dvoa_home","st_dvoa_away",
    "off_epa_per_play_home","off_epa_per_play_away",
    "def_epa_per_play_home","def_epa_per_play_away",
    "rest_days_home","rest_days_away","rest_days_diff",
    "injuries_key_home","injuries_key_away","weather_notes",
]


def pct_missing(s: pd.Series) -> float:
    n = len(s)
    return float(s.isna().sum()) / n if n else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--write-report", action="store_true")
    args = ap.parse_args()

    if not IN.exists():
        raise SystemExit(f"Missing {IN}. Run millions_build_game_view.py with --week {args.week} first.")

    df = pd.read_csv(IN)
    df = df[df["week"] == args.week].copy()
    if df.empty:
        raise SystemExit(f"No rows for week {args.week} in {IN}")

    # Column presence check
    missing_cols = [c for c in CORE_COLS if c not in df.columns]
    # Completeness summary
    comp = (
        pd.DataFrame({
            "column": [c for c in CORE_COLS if c in df.columns],
            "missing_pct": [round(pct_missing(df[c]) * 100, 1) for c in CORE_COLS if c in df.columns]
        })
        .sort_values(["missing_pct","column"], ascending=[False, True])
        .reset_index(drop=True)
    )

    # Specific problem lists
    no_circa = df[df[["circa_spread_home","circa_spread_away"]].isna().any(axis=1)][["game_num","matchup","circa_spread_home","circa_spread_away"]]
    no_current = df[df[["current_spread_home","current_spread_away"]].isna().any(axis=1)][["game_num","matchup","current_spread_home","current_spread_away"]]
    no_open = df[df[["open_spread_home","open_spread_away"]].isna().any(axis=1)][["game_num","matchup","open_spread_home","open_spread_away"]]

    # Print console summary
    print("\n=== Millions Week Audit ===")
    print(f"Week: {args.week}  |  Games: {len(df)}")
    if missing_cols:
        print("\nMissing columns (not in file):")
        for c in missing_cols:
            print("  -", c)
    print("\nNulls by column (%):")
    print(comp.to_string(index=False))

    if not no_circa.empty:
        print("\nGames missing Circa spreads (expected pre-Thursday):")
        print(no_circa.to_string(index=False))
    if not no_current.empty:
        print("\nGames missing CURRENT spreads (should be rare):")
        print(no_current.to_string(index=False))
    if not no_open.empty:
        print("\nGames missing OPEN spreads (ok if we don't track opens yet):")
        print(no_open.to_string(index=False))

    # Optional markdown report
    if args.write_report:
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        def md_or_text(df):
            try:
                return df.to_markdown(index=False)
            except Exception:
                return df.to_string(index=False)

        lines = []
        lines.append(f"# Millions Week {args.week} Audit\n")
        lines.append(f"Games: {len(df)}\n")
        if missing_cols:
            lines.append("\n## Missing Columns (not in file)\n")
            for c in missing_cols:
                lines.append(f"- {c}")
        lines.append("\n## Nulls by Column (%)\n")
        lines.append(md_or_text(comp))
        if not no_circa.empty:
            lines.append("\n## Missing Circa Spreads\n")
            lines.append(md_or_text(no_circa))
        if not no_current.empty:
            lines.append("\n## Missing Current Spreads\n")
            lines.append(md_or_text(no_current))
        if not no_open.empty:
            lines.append("\n## Missing Open Spreads\n")
            lines.append(md_or_text(no_open))
        REPORT.write_text("\n".join(lines), encoding="utf-8")
        print(f"\n✅ wrote report: {REPORT.relative_to(ROOT)}")

if __name__ == "__main__":
    main()
