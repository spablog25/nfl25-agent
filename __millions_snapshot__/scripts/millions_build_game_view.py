"""
CLEAN REWRITE: build from schedule keys, join DVOA on base, then attach team rows.
This avoids any ambiguity about which frame has `home_team`/`away_team` during merges.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SURV_ROADMAP = ROOT / "picks" / "survivor" / "survivor_roadmap_expanded.csv"
MIL_DIR = ROOT / "picks" / "millions"
OUT_ALL = MIL_DIR / "millions_roadmap_game.csv"
OUT_WEEK = MIL_DIR / "millions_weekly_games.csv"

# ---------------- helpers ----------------
def _fill_spreads_with_fallbacks(out: pd.DataFrame, games: pd.DataFrame,
                                 label: str,
                                 primary_cols: list[str],
                                 secondary_cols: list[str] | None = None,
                                 debug_matchup: str | None = None) -> None:
    """Populate {label}_spread_home/away with a robust hierarchy of sources.

    Hierarchy for each side (home/away):
      1) First non-null among `primary_cols` (e.g., current_spread, line, spread)
      2) First non-null among `secondary_cols` (e.g., consensus_spread)
      3) Sign-flip from the opposite side if one is present
      4) Final fallback: 0.0 and mark in a status column

    Also writes a `{label}_spread_status` column with the source used per row.
    """
    h = f"{label}_spread_home"
    a = f"{label}_spread_away"
    status_col = f"{label}_spread_status"
    if status_col not in out.columns:
        out[status_col] = ""

    def _pull_side(side: str, cols: list[str]) -> pd.Series:
        # side is 'h' or 'a'
        vals = None
        for c in cols:
            colname = f"{c}_{side}"
            if colname in games.columns:
                s = pd.to_numeric(games[colname], errors="coerce")
                vals = s if vals is None else vals.where(vals.notna(), s)
        return vals if vals is not None else pd.Series(np.nan, index=games.index)

    prim_home = _pull_side('h', primary_cols)
    prim_away = _pull_side('a', primary_cols)

    out[h] = prim_home
    out[a] = prim_away
    out.loc[out[h].notna(), status_col] = f"{label}:primary"
    out.loc[out[a].notna() & (out[status_col] == ""), status_col] = f"{label}:primary"

    if secondary_cols:
        sec_home = _pull_side('h', secondary_cols)
        sec_away = _pull_side('a', secondary_cols)
        m = out[h].isna() & sec_home.notna()
        out.loc[m, h] = sec_home[m]
        out.loc[m, status_col] = f"{label}:secondary"
        m = out[a].isna() & sec_away.notna()
        out.loc[m, a] = sec_away[m]
        out.loc[m, status_col] = f"{label}:secondary"

    # sign-flip backfill when only one side is present
    m = out[a].isna() & out[h].notna()
    out.loc[m, a] = -out.loc[m, h]
    out.loc[m, status_col] = out.loc[m, status_col].replace("", f"{label}:flip")
    m = out[h].isna() & out[a].notna()
    out.loc[m, h] = -out.loc[m, a]
    out.loc[m, status_col] = out.loc[m, status_col].replace("", f"{label}:flip")

    # final fallback: set to 0.0 so UI doesn’t break; mark status
    m = out[h].isna()
    out.loc[m, h] = 0.0
    out.loc[m, status_col] = f"{label}:fallback0"
    m = out[a].isna()
    out.loc[m, a] = 0.0
    out.loc[m, status_col] = f"{label}:fallback0"

    # optional: print one matchup row to verify where it pulled from
    if debug_matchup is not None and "matchup" in out.columns:
        dbg = out[out["matchup"].astype(str).str.contains(debug_matchup, case=False, na=False)]
        if not dbg.empty:
            print(f"[DEBUG] {label} fallback for {debug_matchup} →\n", dbg[["matchup", h, a, status_col]].to_string(index=False))

def first_col(df: pd.DataFrame, options: list[str], default: str | None = None) -> str | None:
    for c in options:
        if c in df.columns:
            return c
    return default


def ensure_cols(df: pd.DataFrame, cols: list[str], fill_val=np.nan) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = fill_val
    return out


def load_dvoa_normalized(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        print(f"⚠️ DVOA file not found: {path}")
        return None
    dvoa = pd.read_csv(path)
    # normalize headers
    dvoa.columns = (
        dvoa.columns.astype(str)
        .str.strip().str.lower()
        .str.replace(r"%", "", regex=True)
        .str.replace(r"\s+", "_", regex=True)
    )
    # map team col
    if "team" not in dvoa.columns:
        cand = [c for c in dvoa.columns if c.lower() == "team"]
        if cand:
            dvoa = dvoa.rename(columns={cand[0]: "team"})
        else:
            raise SystemExit("DVOA file missing a 'Team' column.")

    # try variants
    variants = [
        {"total": "total_dvoa", "off": "off_dvoa", "def": "def_dvoa", "st": "st_dvoa"},
        {"total": "total_dvoa_proj", "off": "off_dvoa_proj", "def": "def_dvoa_proj", "st": "st_dvoa_proj"},
    ]

    def coerce_pct(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace("%", "")
        try:
            return float(s)
        except Exception:
            return np.nan

    for v in variants:
        if all(col in dvoa.columns for col in v.values()):
            use = dvoa[["team", v["total"], v["off"], v["def"], v["st"]]].copy()
            use = use.rename(columns={
                v["total"]: "total_dvoa",
                v["off"]: "off_dvoa",
                v["def"]: "def_dvoa",
                v["st"]: "st_dvoa",
            })
            for c in ["total_dvoa", "off_dvoa", "def_dvoa", "st_dvoa"]:
                use[c] = use[c].map(coerce_pct)
            return use

    needed = {"team", "total_dvoa", "off_dvoa", "def_dvoa", "st_dvoa"}
    if needed.issubset(dvoa.columns):
        use = dvoa[list(needed)].copy()
        for c in ["total_dvoa", "off_dvoa", "def_dvoa", "st_dvoa"]:
            use[c] = use[c].map(coerce_pct)
        return use

    raise SystemExit(f"Could not find DVOA columns in {path}. Columns were: {list(dvoa.columns)}")


# ---------------- core builder ----------------

def build_game_view(src: pd.DataFrame, dvoa_mode: str | None = None) -> pd.DataFrame:
    src = src.copy()
    print("SRC shape:", src.shape)

    # 1) Base games from schedule keys (guaranteed)
    need = {"week", "hometm", "vistm"}
    if not need.issubset(src.columns):
        raise SystemExit(f"Input missing required columns {need - set(src.columns)}; re-run survivor generator.")

    base = (
        src[["week", "hometm", "vistm"]]
        .drop_duplicates()
        .rename(columns={"hometm": "home_team", "vistm": "away_team"})
        .sort_values(["week", "home_team", "away_team"]).reset_index(drop=True)
    )
    print("BASE shape:", base.shape)

    # 2) Join DVOA onto the BASE immediately (so we have clean keys)
    dvoa = None
    if dvoa_mode == "projection":
        dvoa = load_dvoa_normalized(DATA_DIR / "2025_dvoa_projections.csv")
    elif dvoa_mode == "season":
        dvoa = load_dvoa_normalized(DATA_DIR / "2025_dvoa_season.csv")
        # Align team codes with schedule (e.g., DVOA uses WAS, schedule uses WSH)
        alias_to_schedule = {
            "WAS": "WSH",  # Washington
            # add more if needed, e.g. "JAC": "JAX", "NOR": "NO"
        }
        if "team" in dvoa.columns:
            dvoa["team"] = dvoa["team"].replace(alias_to_schedule)

    if dvoa is not None:
        dvoa_home = dvoa.rename(columns={
            "team": "home_team",
            "total_dvoa": "total_dvoa_home",
            "off_dvoa": "off_dvoa_home",
            "def_dvoa": "def_dvoa_home",
            "st_dvoa": "st_dvoa_home",
        })
        dvoa_away = dvoa.rename(columns={
            "team": "away_team",
            "total_dvoa": "total_dvoa_away",
            "off_dvoa": "off_dvoa_away",
            "def_dvoa": "def_dvoa_away",
            "st_dvoa": "st_dvoa_away",
        })
        base = base.merge(dvoa_home, on="home_team", how="left")
        base = base.merge(dvoa_away, on="away_team", how="left")

    # 3) Attach detailed team rows (for spreads, rest, etc.)
    src["hoa_norm"] = src.get("home_or_away", "").astype(str).str.strip().str.lower()
    home_rows = src[src["hoa_norm"].eq("home")].copy()
    if home_rows.empty:
        home_rows = src.copy()
    home_rows = home_rows.rename(columns=lambda c: f"{c}_h")

    away_rows = src[src["hoa_norm"].eq("away")].copy()
    if away_rows.empty:
        away_rows = src.copy()
    away_rows = away_rows.rename(columns=lambda c: f"{c}_a")

    games = base.merge(
        home_rows,
        left_on=["week", "home_team"],
        right_on=["week_h", "team_h"],
        how="left",
    ).merge(
        away_rows,
        left_on=["week", "away_team"],
        right_on=["week_a", "team_a"],
        how="left",
    )
    print("GAMES shape:", games.shape)

    # 4) Build output
    out = pd.DataFrame()
    out["week"] = games["week"]
    out["home_team"] = games["home_team"]
    out["away_team"] = games["away_team"]
    out["matchup"] = games["away_team"] + " @ " + games["home_team"]

    # kickoff: prefer schedule time, fallback to date
    kko_h = first_col(games, ["kickoff_local_h", "time_sch_h", "time_h"], None)
    date_h = first_col(games, ["date_h", "date_sch_h"], None)
    out["kickoff_local"] = games[kko_h] if kko_h else (games[date_h] if date_h else "")
    out["venue"] = games.get("venue_h", "")

    # spreads with sign-flip backfill
    def map_spreads(label: str, src_home: str | None, src_away: str | None):
        h = f"{label}_spread_home"; a = f"{label}_spread_away"
        out[h] = games[f"{src_home}_h"] if src_home else np.nan
        out[a] = games[f"{src_away}_a"] if src_away else np.nan
        if src_home or src_away:
            m = out[a].isna() & out[h].notna(); out.loc[m, a] = -out.loc[m, h]
            m = out[h].isna() & out[a].notna(); out.loc[m, h] = -out.loc[m, a]

    open_c  = first_col(src, ["open_spread", "opening_spread", "open_line"], None)
    curr_c  = first_col(src, ["current_spread", "curr_spread", "line", "spread"], None)
    close_c = first_col(src, ["closing_spread", "close_spread", "final_spread"], None)
    circa_c = first_col(src, ["circa_spread", "circa_line"], None)
    circa_px= first_col(src, ["circa_spread_price", "circa_vig"], None)

    map_spreads("open", open_c, open_c)
    # Robust fill for CURRENT spreads (with debug for NYG @ WSH)
    _fill_spreads_with_fallbacks(
        out,
        games,
        label="current",
        primary_cols=["current_spread", "line", "spread"],
        secondary_cols=["consensus_spread"],
        debug_matchup="NYG @ WSH",  # optional; remove later
    )
    # Fallback: if CURRENT spreads are missing, try consensus_spread (per side), then sign-flip
    if out["current_spread_home"].isna().any() or out["current_spread_away"].isna().any():
        if "consensus_spread_h" in games.columns:
            m = out["current_spread_home"].isna() & games["consensus_spread_h"].notna()
            out.loc[m, "current_spread_home"] = games.loc[m, "consensus_spread_h"]
        if "consensus_spread_a" in games.columns:
            m = out["current_spread_away"].isna() & games["consensus_spread_a"].notna()
            out.loc[m, "current_spread_away"] = games.loc[m, "consensus_spread_a"]

        # backfill the opposite side by sign if only one was filled
        m = out["current_spread_away"].isna() & out["current_spread_home"].notna()
        out.loc[m, "current_spread_away"] = -out.loc[m, "current_spread_home"]
        m = out["current_spread_home"].isna() & out["current_spread_away"].notna()
        out.loc[m, "current_spread_home"] = -out.loc[m, "current_spread_away"]

    map_spreads("closing", close_c, close_c)
    map_spreads("circa", circa_c, circa_c)

    out["circa_spread_price_home"] = games.get(f"{circa_px}_h") if circa_px else np.nan
    out["circa_spread_price_away"] = games.get(f"{circa_px}_a") if circa_px else np.nan

    # line values (member view uses CURRENT)
    out["line_value_current_home"] = out["current_spread_home"] - out["circa_spread_home"]
    out["line_value_current_away"] = out["current_spread_away"] - out["circa_spread_away"]
    out["line_value_closing_home"] = out["closing_spread_home"] - out["circa_spread_home"]
    out["line_value_closing_away"] = out["closing_spread_away"] - out["circa_spread_away"]
    out["line_value_home"] = out["line_value_current_home"]
    out["line_value_away"] = out["line_value_current_away"]

    # DVOA columns (already merged onto base)
    for col in [
        "total_dvoa_home","off_dvoa_home","def_dvoa_home","st_dvoa_home",
        "total_dvoa_away","off_dvoa_away","def_dvoa_away","st_dvoa_away",
    ]:
        if col not in out.columns and col in games.columns:
            out[col] = games[col]

    # EPA placeholders
    for c in [
        "off_epa_per_play_home", "off_epa_per_play_away",
        "def_epa_per_play_home", "def_epa_per_play_away",
        "epa_diff_home", "epa_diff_away",
    ]:
        out[c] = np.nan

    # rest days & diff
    out["rest_days_home"] = pd.to_numeric(games.get("rest_days_h"), errors="coerce")
    out["rest_days_away"] = pd.to_numeric(games.get("rest_days_a"), errors="coerce")
    out["rest_days_diff"] = out["rest_days_home"] - out["rest_days_away"]

    # injuries & weather
    out["injuries_key_home"] = games.get("injuries_key_h", "")
    out["injuries_key_away"] = games.get("injuries_key_a", "")
    out["weather_notes"] = games.get("weather_notes_h", "")

    # final order
    col_order = [
        "week", "game_num", "matchup", "kickoff_local", "venue", "home_team", "away_team",
        "open_spread_home", "open_spread_away",
        "circa_spread_home", "circa_spread_away",
        "current_spread_home", "current_spread_away",
        "closing_spread_home", "closing_spread_away",
        "line_value_current_home", "line_value_current_away",
        "line_value_closing_home", "line_value_closing_away",
        "line_value_home", "line_value_away",
        "off_epa_per_play_home", "off_epa_per_play_away",
        "def_epa_per_play_home", "def_epa_per_play_away",
        "total_dvoa_home", "total_dvoa_away",
        "off_dvoa_home", "off_dvoa_away",
        "def_dvoa_home", "def_dvoa_away",
        "st_dvoa_home", "st_dvoa_away",
        "rest_days_home", "rest_days_away", "rest_days_diff",
        "injuries_key_home", "injuries_key_away", "weather_notes",
        "circa_spread_price_home", "circa_spread_price_away",
    ]
    out = ensure_cols(out, col_order)

    # sort & number
    sort_keys = ["week", "kickoff_local", "matchup"] if "kickoff_local" in out.columns else ["week", "matchup"]
    out = out.sort_values(sort_keys, kind="mergesort").reset_index(drop=True)
    out["game_num"] = out.groupby("week").cumcount() + 1
    out = out[col_order]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week", type=int, help="Filter to a single week (also selects DVOA source)")
    args = ap.parse_args()

    if not SURV_ROADMAP.exists():
        raise SystemExit(f"Missing input file: {SURV_ROADMAP}. Run generate_survivor_roadmap.py first.")

    MIL_DIR.mkdir(parents=True, exist_ok=True)
    src = pd.read_csv(SURV_ROADMAP)

    dvoa_mode = None
    if args.week is not None:
        dvoa_mode = "projection" if int(args.week) == 1 else "season"

    game_df = build_game_view(src, dvoa_mode=dvoa_mode)
    game_df.to_csv(OUT_ALL, index=False)
    print(f"✅ wrote: {OUT_ALL.relative_to(ROOT)}  (games: {len(game_df)})")

    if args.week is not None:
        wk = int(args.week)
        wk_df = game_df[game_df["week"] == wk].copy()
        wk_df.to_csv(OUT_WEEK, index=False)
        print(f"✅ wrote: {OUT_WEEK.relative_to(ROOT)}  (week {wk}: {len(wk_df)} games)")


if __name__ == "__main__":
    main()
