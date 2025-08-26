
#python -m scripts.millions_audit_week --season 2025 --week 1 --planner "picks/millions/millions_planner.csv" --out_dir "picks/millions/diagnostics" --line_tolerance 0.5 --spread_range="-20,20" --dvoa_range="-0.6,0.6"
from __future__ import annotations
import argparse
from pathlib import Path
import sys
import pandas as pd
from datetime import datetime

# -----------------------------
# Millions Structural Audit (v1.1)
# -----------------------------
# Beginner-friendly, pandas-only. Produces a Markdown report and a flags CSV.
# Exits with 0/1/2 depending on severity.

REQUIRED_COLS = [
    "season", "week", "team", "opponent",
]

KEY_FIELDS_FOR_MISSINGNESS = [
    "team", "opponent", "circa_line",
    "team_total_dvoa_proj", "opp_total_dvoa_proj",
]

HOME_AWAY_VALUES = {"HOME", "AWAY", "H", "A"}


def pct(x: float) -> str:
    return f"{x*100:.1f}%"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run structural audit for Millions planner")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--week", type=int, required=True)
    p.add_argument("--planner", type=str, required=True)
    p.add_argument("--survivor", type=str, required=False, default=None)
    p.add_argument("--aliases", type=str, required=False, default=None)
    p.add_argument("--out_dir", type=str, required=True)
    # NOTE: argparse formats help strings with % interpolation.
    # If you include a literal percent sign, escape it as '%%' to avoid ValueError.
    p.add_argument(
        "--fail_on_missing",
        type=float,
        default=0.01,
        help="Max allowed missing ratio in key fields (e.g., 0.01 for 1%%)",
    )
    p.add_argument(
        "--spread_range",
        type=str,
        default="-20,20",
        help="Min,max allowed spreads",
    )
    p.add_argument(
        "--dvoa_range",
        type=str,
        default="-0.6,0.6",
        help="Min,max allowed DVOA projections (fractional)",
    )
    # add inside parse_args()
    p.add_argument("--roadmap", type=str, required=False, default=None,
                   help="Path to millions_roadmap_game.csv for lines consistency checks")
    p.add_argument("--line_tolerance", type=float, default=0.5,
                   help="Allowed absolute difference when comparing lines (points)")
    return p.parse_args()


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_csv(path)


def load_aliases(path: Path | None) -> set[str] | None:
    if not path or not Path(path).exists():
        return None
    df = pd.read_csv(path)
    # Any column that looks like a code
    cols = [c for c in df.columns if "alias" in c.lower() or c.lower() in {"code", "team", "abbr", "alias"}]
    vals: set[str] = set()
    for c in cols:
        vals.update(df[c].astype(str).str.strip().str.upper().tolist())
    return {v for v in vals if v and v != "NAN"}


def normalize_hoa(x: pd.Series) -> pd.Series:
    return x.astype(str).str.strip().str.upper().replace({"H": "HOME", "A": "AWAY", "NAN": pd.NA})


def compute_dvoa_diff(df: pd.DataFrame) -> None:
    if {"team_total_dvoa_proj", "opp_total_dvoa_proj"}.issubset(df.columns):
        try:
            df["dvoa_diff_proj"] = df["team_total_dvoa_proj"] - df["opp_total_dvoa_proj"]
        except Exception:
            pass


def audit(
    planner: pd.DataFrame,
    season: int,
    week: int,
    survivor: pd.DataFrame | None,
    alias_set: set[str] | None,
    out_dir: Path,
    miss_thr: float,
    spread_min: float,
    spread_max: float,
    dvoa_min: float,
    dvoa_max: float,
    roadmap: pd.DataFrame | None = None,
    line_tol: float = 0.5
) -> tuple[int, str, pd.DataFrame]:

    out_dir.mkdir(parents=True, exist_ok=True)

    # Filter season/week when present
    if "season" in planner.columns:
        planner = planner[planner["season"] == season]
    if "week" in planner.columns:
        planner = planner[planner["week"] == week]
    planner = planner.copy()

    # Coerce basic fields
    for c in ["team", "opponent"]:
        if c in planner.columns:
            planner[c] = planner[c].astype(str).str.strip().str.upper()
    if "home_or_away" in planner.columns:
        planner["home_or_away"] = normalize_hoa(planner["home_or_away"])

    # Ensure derived
    compute_dvoa_diff(planner)

    # Start report
    lines: list[str] = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"# Millions Week Audit — Season {season} Week {week}")
    lines.append("")
    lines.append(f"Generated: {ts}")
    lines.append("")

    severity = 0  # 0 ok, 1 warnings, 2 failure
    flags_rows = []

    # 1) Schema check
    missing_required = [c for c in REQUIRED_COLS if c not in planner.columns]
    if missing_required:
        lines.append("## ❌ Missing required columns")
        for c in missing_required:
            lines.append(f"- {c}")
        severity = max(severity, 2)
    else:
        lines.append("## ✅ Required columns present")
    lines.append("")

    # 2) Missingness check
    lines.append("## Missingness (key fields)")
    for c in KEY_FIELDS_FOR_MISSINGNESS:
        if c in planner.columns:
            ratio = planner[c].isna().mean()
            status = "OK" if ratio <= miss_thr else "FAIL"
            lines.append(f"- {c}: {pct(ratio)} ({status})")
            if ratio > miss_thr:
                severity = max(severity, 2)
                for idx, is_na in planner[c].isna().items():
                    if is_na:
                        flags_rows.append({"row": int(idx), "field": c, "issue": "missing"})
        else:
            lines.append(f"- {c}: n/a (column not present)")
    lines.append("")

    # 3) Ranges: spreads
    if "circa_line" in planner.columns:
        bad_spread = planner[
            ~planner["circa_line"].between(spread_min, spread_max, inclusive="both") & planner["circa_line"].notna()
        ]
        lines.append(f"## Spread range check [{spread_min},{spread_max}] — bad: {len(bad_spread)}")
        if not bad_spread.empty:
            severity = max(severity, 2)
            for idx, r in bad_spread.iterrows():
                flags_rows.append({
                    "row": int(idx),
                    "field": "circa_line",
                    "issue": "out_of_range",
                    "value": r["circa_line"],
                })
        lines.append("")

    # 4) Ranges: DVOA projections
    for col in ["team_total_dvoa_proj", "opp_total_dvoa_proj"]:
        if col in planner.columns:
            bad = planner[
                ~planner[col].between(dvoa_min, dvoa_max, inclusive="both") & planner[col].notna()
            ]
            lines.append(f"## {col} range check [{dvoa_min},{dvoa_max}] — bad: {len(bad)}")
            if not bad.empty:
                severity = max(severity, 2)
                for idx, r in bad.iterrows():
                    flags_rows.append({
                        "row": int(idx),
                        "field": col,
                        "issue": "out_of_range",
                        "value": r[col],
                    })
            lines.append("")

    # 5) Alias resolution
    if alias_set is not None:
        missing_alias_team = (
            planner[~planner["team"].isin(alias_set)] if "team" in planner.columns else pd.DataFrame()
        )
        missing_alias_opp = (
            planner[~planner["opponent"].isin(alias_set)] if "opponent" in planner.columns else pd.DataFrame()
        )
        total_miss = len(missing_alias_team) + len(missing_alias_opp)
        lines.append(f"## Alias resolution — unresolved rows: {total_miss}")
        if total_miss > 0:
            severity = max(severity, 2)
            for idx, r in missing_alias_team.iterrows():
                flags_rows.append({
                    "row": int(idx),
                    "field": "team",
                    "issue": "alias_unresolved",
                    "value": r.get("team"),
                })
            for idx, r in missing_alias_opp.iterrows():
                flags_rows.append({
                    "row": int(idx),
                    "field": "opponent",
                    "issue": "alias_unresolved",
                    "value": r.get("opponent"),
                })
        lines.append("")

    # 6) HOME/AWAY checks
    if "home_or_away" in planner.columns:
        invalid = planner[~planner["home_or_away"].isin({"HOME", "AWAY", pd.NA})]
        lines.append(f"## HOME/AWAY validity — invalid rows: {len(invalid)}")
        if not invalid.empty:
            severity = max(severity, 1)
            for idx, r in invalid.iterrows():
                flags_rows.append({
                    "row": int(idx),
                    "field": "home_or_away",
                    "issue": "invalid_value",
                    "value": r.get("home_or_away"),
                })
        lines.append("")

    # 7) Duplicate rows per matchup
    dup_cols = [c for c in ["season", "week", "team", "opponent"] if c in planner.columns]
    if dup_cols:
        dups = planner.duplicated(subset=dup_cols, keep=False)
        dup_rows = planner[dups]
        lines.append(f"## Duplicate matchup rows: {len(dup_rows)}")
        if not dup_rows.empty:
            severity = max(severity, 1)
            for idx, r in dup_rows.iterrows():
                flags_rows.append({
                    "row": int(idx),
                    "field": ",".join(dup_cols),
                    "issue": "duplicate_row",
                })
        lines.append("")
        # 9) Lines consistency vs roadmap (optional)
    if roadmap is not None and not roadmap.empty:
        r = roadmap.copy()
        for c in ("home_team", "away_team"):
            if c in r.columns:
                r[c] = r[c].astype(str).str.strip().str.upper()
        # Build orientation-agnostic key to join
        if {"home_team", "away_team"}.issubset(r.columns) and {"team", "opponent"}.issubset(planner.columns):
            planner["_key"] = planner[["team", "opponent"]].apply(lambda x: "::".join(sorted([x.team, x.opponent])),
                                                                  axis=1)
            r["_key"] = r[["home_team", "away_team"]].apply(lambda x: "::".join(sorted([x.home_team, x.away_team])),
                                                            axis=1)
            keep = [c for c in (
                "current_spread_home", "current_spread_away",
                "closing_spread_home", "closing_spread_away",
                "circa_spread_home", "circa_spread_away",
                "open_spread_home", "open_spread_away"
            ) if c in r.columns]
            rr = r[["_key"] + keep].drop_duplicates("_key")
            merged = planner.merge(rr, on="_key", how="left", suffixes=("", "_rm"))

            # Compare home-perspective current spread if present on both sides
            inconsistencies = 0

            def diff_ok(a, b):
                if pd.isna(a) or pd.isna(b):
                    return True  # ignore missing in either
                try:
                    return abs(float(a) - float(b)) <= line_tol
                except Exception:
                    return True

            # Track problems by field
            field_issues = []
            for fld in ("current_spread_home", "closing_spread_home", "open_spread_home"):
                f_rm = f"{fld}_rm"
                if fld in merged.columns and f_rm in merged.columns:
                    bad = merged[~merged.apply(lambda r: diff_ok(r.get(fld), r.get(f_rm)), axis=1)]
                    if not bad.empty:
                        inconsistencies += len(bad)
                        for idx, row in bad.iterrows():
                            field_issues.append({
                                "row": int(idx), "field": fld,
                                "issue": "line_mismatch",
                                "planner": row.get(fld), "roadmap": row.get(f_rm)
                            })

            lines.append(f"## Lines consistency vs roadmap — mismatches: {inconsistencies} (tol=±{line_tol})")
            if inconsistencies > 0:
                severity = max(severity, 1)
                flags_rows.extend(field_issues)
            lines.append("")
            # 10) Spread symmetry sanity: expect away ~= -home (within tol)
            if roadmap is not None and not roadmap.empty:
                r = roadmap.copy()
                bad_sym = []
                for side in ("current", "closing", "open"):
                    h = f"{side}_spread_home";
                    a = f"{side}_spread_away"
                    if h in r.columns and a in r.columns:
                        rr = r[["home_team", "away_team", h, a]].dropna()

                        def not_symmetric(x):
                            try:
                                return abs(float(x[h]) + float(x[a])) > line_tol
                            except Exception:
                                return False

                        viol = rr[rr.apply(not_symmetric, axis=1)]
                        if not viol.empty:
                            for _, row in viol.iterrows():
                                bad_sym.append(
                                    {"field": f"{side}_spread", "home": row["home_team"], "away": row["away_team"],
                                     "home_val": row[h], "away_val": row[a]})
                lines.append(f"## Spread symmetry violations: {len(bad_sym)} (expect away ~= -home)")
                if bad_sym:
                    severity = max(severity, 1)
                    flags_rows.extend(bad_sym)
                lines.append("")
            planner.drop(columns=["_key"], inplace=True, errors="ignore")
    # 8) Survivor cross-check (presence-only)
    if survivor is not None and not survivor.empty:
        for c in ["team", "opponent"]:
            if c in survivor.columns:
                survivor[c] = survivor[c].astype(str).str.strip().str.upper()
        lines.append("## Survivor cross-check (presence only)")
        if {"team", "opponent"}.issubset(survivor.columns):
            keys_planner = set(zip(planner.get("team", pd.Series(dtype=str)), planner.get("opponent", pd.Series(dtype=str))))
            keys_surv = set(zip(survivor.get("team", pd.Series(dtype=str)), survivor.get("opponent", pd.Series(dtype=str))))
            overlap = len(keys_planner & keys_surv)
            lines.append(f"- Overlapping matchup keys: {overlap}")
        lines.append("")

    # Write outputs
    md_path = out_dir / "millions_week_audit.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    flags_df = pd.DataFrame(flags_rows)
    if not flags_df.empty:
        flags_df.to_csv(out_dir / "millions_week_audit_flags.csv", index=False)

    return severity, str(md_path), flags_df


def main() -> None:
    args = parse_args()
    planner = load_csv(Path(args.planner))
    roadmap = load_csv(Path(args.roadmap)) if args.roadmap and Path(args.roadmap).exists() else None
    survivor = load_csv(Path(args.survivor)) if args.survivor and Path(args.survivor).exists() else None
    alias_set = load_aliases(Path(args.aliases)) if args.aliases else None

    smin, smax = [float(x) for x in args.spread_range.split(",")]
    dmin, dmax = [float(x) for x in args.dvoa_range.split(",")]

    code, md_path, flags_df = audit(
        planner=planner,
        season=args.season,
        week=args.week,
        survivor=survivor,
        alias_set=alias_set,
        out_dir=Path(args.out_dir),
        miss_thr=args.fail_on_missing,
        spread_min=smin,
        spread_max=smax,
        dvoa_min=dmin,
        dvoa_max=dmax,
        roadmap=roadmap,
        line_tol=args.line_tolerance,
    )

    print(f"Audit report: {md_path}")
    if not flags_df.empty:
        print(f"Flags CSV: {(Path(args.out_dir) / 'millions_week_audit_flags.csv')}  (rows={len(flags_df)})")

    # Map severity to exit code: 0 ok, 1 warn, 2 fail
    sys.exit(code)


if __name__ == "__main__":
    main()
