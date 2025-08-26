# scripts/millions_export_member_html.py
# ------------------------------------------------------------
# NFL25 Agent — Member Dashboard Exporter (v1.17)
# What's new
# - **Header spacing fix**: matchup header now has its own padded section
#   with a bottom border so it no longer blends into the first row.
# - **DVOA indicators restored**: Top 5 / Bottom 5 pills per team next to
#   the matchup (auto-detected from rank-like columns; fallback to
#   thresholds via env vars DVOA_TOP_THRESH / DVOA_BOTTOM_THRESH).
# - Keeps v1.16 Total column layout, v1.15 permissive DVOA auto-detect,
#   and v1.14 sorting/overlay improvements.
# ------------------------------------------------------------

from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import os
import pandas as pd

SORT_KEYS = {"kickoff": "_kickoff_sort_key", "none": None}
WEEKDAY_ORDER = {"THU": 0, "FRI": 1, "SAT": 2, "SUN": 3, "MON": 4}

# ==========================
# Formatting helpers
# ==========================

def fmt_pct(x, nd: int = 1, signed: bool = True) -> str:
    if x is None or (hasattr(pd, "isna") and pd.isna(x)):
        return "—"
    try:
        v = float(x)
    except Exception:
        return "—"
    if abs(v) > 1.5:  # values like 11.0 mean 11%
        v = v / 100.0
    v *= 100.0
    if abs(v) < 0.05:
        v = 0.0
    out = f"{v:.{nd}f}%"
    if signed and v > 0:
        out = "+" + out
    return out


def fmt_spread(x) -> str:
    if x is None or (hasattr(pd, "isna") and pd.isna(x)):
        return "TBD"
    try:
        v = float(x)
    except Exception:
        return "TBD"
    if abs(v) < 0.05:
        return "PK"
    s = f"{v:.1f}"
    if not s.startswith("-"):
        s = "+" + s
    return s


def fmt_total(x) -> str:
    if x is None or (hasattr(pd, "isna") and pd.isna(x)):
        return "TBD"
    try:
        return f"{float(x):.1f}"
    except Exception:
        return "TBD"

# ==========================
# Kickoff helpers
# ==========================

def _weekday_rank_from_text(txt: str | None) -> int:
    if not isinstance(txt, str):
        return 99
    up = txt.upper()
    for k, v in WEEKDAY_ORDER.items():
        if k in up:
            return v
    return 99


def parse_kickoff_to_pt(s: str | float | int) -> tuple[str, float, int]:
    if not isinstance(s, str) or not s.strip():
        return ("", float("inf"), 99)
    txt = s.strip()
    wd_rank = _weekday_rank_from_text(txt)
    fmts = ["%a %I:%M %p", "%Y-%m-%d %I:%M %p", "%Y/%m/%d %I:%M %p", "%m/%d/%Y %I:%M %p", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%I:%M %p"]
    dt = None
    for f in fmts:
        try:
            dt = datetime.strptime(txt, f)
            break
        except Exception:
            pass
    if dt is None:
        try:
            t = datetime.strptime(txt, "%I:%M %p")
            minutes = (t.hour - 3) * 60 + t.minute
            disp = t.strftime("%I:%M %p").lstrip("0") + " PT"
            return (disp, float(minutes), wd_rank)
        except Exception:
            return (txt, float("inf"), wd_rank)
    map7 = {3: 0, 4: 1, 5: 2, 6: 3, 0: 4, 1: 99, 2: 99}
    wd_rank = map7.get(dt.weekday(), 99)
    dt_pt = dt - timedelta(hours=3)
    disp = dt_pt.strftime("%I:%M %p").lstrip("0") + " PT"
    minutes = dt_pt.hour * 60 + dt_pt.minute
    return (disp, float(minutes), wd_rank)

# ==========================
# Merge helpers
# ==========================

def make_key(a: str | None, b: str | None) -> str:
    return "|".join(sorted([(a or "").upper(), (b or "").upper()]))


def attach_display_helpers(df: pd.DataFrame) -> pd.DataFrame:
    def col(name: str) -> pd.Series:
        return df[name] if name in df.columns else pd.Series([None] * len(df), index=df.index)
    hoa = col("home_or_away").astype(str).str.strip().str.upper()
    team = col("team").astype(str).str.strip().str.upper()
    opp = col("opponent").astype(str).str.strip().str.upper()
    df["_home"] = team.where(hoa == "HOME", opp)
    df["_away"] = opp.where(hoa == "HOME", team)
    df["__game_key"] = [make_key(t, o) for t, o in zip(team, opp)]

    if "_kickoff_sort_key" in df.columns and df["_kickoff_sort_key"].notna().any():
        df["_kickoff_sort_key"] = pd.to_numeric(df["_kickoff_sort_key"], errors="coerce")
        if "_kickoff_pt" not in df.columns or df["_kickoff_pt"].isna().all():
            if "kickoff_pt" in df.columns:
                df["_kickoff_pt"] = df["kickoff_pt"]
            else:
                disp = []
                for v in col("kickoff_local"):
                    d, _tm, _wd = parse_kickoff_to_pt(v)
                    disp.append(d)
                df["_kickoff_pt"] = disp
        return df
    disp, time_keys, day_keys = [], [], []
    for v in col("kickoff_local"):
        d, tm, wd = parse_kickoff_to_pt(v)
        disp.append(d)
        time_keys.append(tm)
        day_keys.append(wd)
    df["_kickoff_pt"] = disp
    df["_kickoff_sort_key"] = [wd * 1440 + tm for wd, tm in zip(day_keys, time_keys)]
    return df


def _collapse_series(df: pd.DataFrame, name: str):
    matches = [i for i, c in enumerate(df.columns) if c == name]
    if not matches:
        return None
    s = df.iloc[:, matches[0]].copy()
    for j in matches[1:]:
        s = s.where(s.notna(), df.iloc[:, j])
    for j in sorted(matches[1:], reverse=True):
        try:
            df.drop(df.columns[j], axis=1, inplace=True)
        except Exception:
            pass
    return s


def overlay_from_odds(df: pd.DataFrame, odds_csv: Path | None) -> pd.DataFrame:
    """Overlay spreads/totals from an odds CSV.
    Latest CSV values now *overwrite* planner values so the export always
    reflects the most recent pull, and we keep the planner's row shape.
    """
    if not odds_csv:
        return df
    if not Path(odds_csv).exists():
        print(f"[warn] odds_csv not found: {odds_csv}")
        return df

    dfo = pd.read_csv(odds_csv)
    dfo["__game_key"] = [make_key(h, a) for h, a in zip(dfo.get("home_team"), dfo.get("away_team"))]

    use = [
        "__game_key",
        "current_spread_home", "current_spread_away", "current_total",
        "circa_spread_home", "circa_spread_away", "circa_total",
        "open_spread_home", "open_spread_away", "open_total",
        "line_delta_home",
    ]
    have = [c for c in use if c in dfo.columns]
    dfo = dfo[have]

    # season_all.csv is append-only. Keep the *last* row per game key so we
    # use the most recent snapshot for each matchup.
    if "__game_key" in dfo.columns:
        dfo = dfo.drop_duplicates(subset=["__game_key"], keep="last")

    merged = df.merge(dfo, on="__game_key", how="left", suffixes=("", "_odds"))

    def prefer_odds(dst: str):
        src = dst + "_odds"
        s_dst = _collapse_series(merged, dst)
        s_src = _collapse_series(merged, src)
        if s_dst is None:
            s_dst = pd.Series([None] * len(merged), index=merged.index)
        if s_src is None:
            s_src = pd.Series([None] * len(merged), index=merged.index)
        # Prefer odds CSV (latest) over any existing planner value
        merged[dst] = s_src.combine_first(s_dst)

    for col in (
        "current_spread_home", "current_spread_away", "current_total",
        "circa_spread_home", "circa_spread_away", "circa_total",
        "open_spread_home", "open_spread_away", "open_total",
        "line_delta_home",
    ):
        prefer_odds(col)

    if "home_or_away" in merged.columns:
        mask_home = merged["home_or_away"].astype(str).str.upper().eq("HOME")
        if "circa_line" not in merged.columns:
            merged["circa_line"] = None
        if "circa_spread_home" in merged.columns:
            merged.loc[mask_home, "circa_line"] = merged.loc[mask_home, "circa_spread_home"]
        if "circa_spread_away" in merged.columns:
            merged.loc[~mask_home, "circa_line"] = merged.loc[~mask_home, "circa_spread_away"]
        if "open_line" not in merged.columns:
            merged["open_line"] = None
        if "open_spread_home" in merged.columns:
            merged.loc[mask_home, "open_line"] = merged.loc[mask_home, "open_spread_home"]
        if "open_spread_away" in merged.columns:
            merged.loc[~mask_home, "open_line"] = merged.loc[~mask_home, "open_spread_away"]

    # Keep the planner's rows as-is; just drop helper columns from the merge
    merged = merged.loc[:, ~merged.columns.str.endswith("_odds")]
    return merged

# ==========================
# DVOA lookup (permissive) + badges
# ==========================

TEAM_BASES = ["team", "home", "tm"]
OPP_BASES  = ["opponent", "opp", "away"]

KIND_FRAGS = {
    "total": [["dvoa", "total"], ["total", "dvoa"], ["overall", "dvoa"], ["dvoa_overall"]],
    "off":   [["off", "dvoa"], ["dvoa", "off"], ["offense", "dvoa"], ["dvoa_off"]],
    "def":   [["def", "dvoa"], ["dvoa", "def"], ["defense", "dvoa"], ["dvoa_def"]],
}


def _match_by_fragments(row: pd.Series, base_opts: list[str], kind: str):
    cols = list(row.index)
    lower = [c.lower() for c in cols]
    for b in base_opts:
        b = b.lower()
        for fragset in KIND_FRAGS[kind]:
            for i, name in enumerate(lower):
                ok = b in name and all(f in name for f in [f.lower() for f in fragset])
                if ok:
                    val = row[cols[i]]
                    if val is not None and not pd.isna(val):
                        return val
    return None


def dvoa_value(row: pd.Series, side: str, kind: str) -> float | None:
    hoa = (row.get("home_or_away") or "").upper()
    side_is_team = (side == 'home' and hoa == 'HOME') or (side == 'away' and hoa != 'HOME')
    base_opts = TEAM_BASES if side_is_team else OPP_BASES
    v = _match_by_fragments(row, base_opts, kind)
    if v is None:
        return None
    try:
        v = float(v)
    except Exception:
        return None
    if abs(v) > 1.5:
        v = v / 100.0
    return v


def _rank_like(row: pd.Series, base_opts: list[str]):
    """Try to find a numeric 'rank' column (1 best .. 32 worst) for a given base."""
    cols = list(row.index)
    lower = [c.lower() for c in cols]
    for b in base_opts:
        b = b.lower()
        for i, name in enumerate(lower):
            if b in name and 'rank' in name and 'dvoa' in name:
                try:
                    v = float(row[cols[i]])
                    return v
                except Exception:
                    pass
    return None


def badge_html(row: pd.Series, side: str) -> str:
    # Try rank first
    hoa = (row.get("home_or_away") or "").upper()
    base_opts = TEAM_BASES if ((side == 'home' and hoa == 'HOME') or (side == 'away' and hoa != 'HOME')) else OPP_BASES
    rank = _rank_like(row, base_opts)
    if rank is not None:
        if rank <= 5:
            return "<span class='pill green'>Top 5 DVOA</span>"
        if rank >= 28:
            return "<span class='pill red'>Bottom 5 DVOA</span>"
        return ""
    # Fallback to thresholds on total DVOA
    top_t = float(os.getenv('DVOA_TOP_THRESH', '0.15'))  # 15%
    bot_t = float(os.getenv('DVOA_BOTTOM_THRESH', '-0.12'))  # -12%
    v = dvoa_value(row, side, 'total')
    if v is None:
        return ""
    if v >= top_t:
        return "<span class='pill green'>Top 5 DVOA</span>"
    if v <= bot_t:
        return "<span class='pill red'>Bottom 5 DVOA</span>"
    return ""

# ==========================
# Lines helpers
# ==========================

def favor_from_pair(row, prefix: str) -> tuple[str | None, float | None]:
    h_col, a_col = f"{prefix}_spread_home", f"{prefix}_spread_away"
    h, a = row.get(h_col), row.get(a_col)
    h = float(h) if h is not None and not pd.isna(h) else None
    a = float(a) if a is not None and not pd.isna(a) else None
    if h is None and a is None:
        return (None, None)
    if h is not None and a is not None and abs(h) < 0.05 and abs(a) < 0.05:
        return (None, 0.0)
    if h is not None and (a is None or h < a):
        if h < 0:
            return (row.get("_home"), h)
    if a is not None and (h is None or a < h):
        if a < 0:
            return (row.get("_away"), a)
    values = [(row.get("_home"), h), (row.get("_away"), a)]
    values = [(t, v) for t, v in values if v is not None]
    if not values:
        return (None, None)
    team, v = min(values, key=lambda tv: tv[1])
    return (team if v < 0 else None, v)

# ==========================
# Load, sort, build
# ==========================

def load_week(planner: Path, season: int, week: int) -> pd.DataFrame:
    df = pd.read_csv(planner)
    if "season" in df.columns:
        df = df[df["season"] == season]
    if "week" in df.columns:
        df = df[df["week"] == week]
    for c in ("team", "opponent", "home_or_away"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()
    df = attach_display_helpers(df)
    # ✅ Deduplicate by matchup so a corrupted planner with repeated rows
    # doesn't render the same game over and over in the HTML export.
    if "__game_key" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["__game_key"], keep="first")
        after = len(df)
        if after != before:
            print(f"[info] de-duplicated planner rows by game key: {before} → {after}")
    return df.reset_index(drop=True)


def sort_week(df: pd.DataFrame, sort_by: str) -> pd.DataFrame:
    key_col = SORT_KEYS.get(sort_by, None)
    if not key_col or key_col not in df.columns:
        return df.reset_index(drop=True)
    key = pd.to_numeric(df[key_col], errors="coerce")
    df2 = df.copy()
    df2["__tmp_sort"] = key
    df2 = df2.sort_values("__tmp_sort", ascending=True, kind="mergesort").drop(columns=["__tmp_sort"])  # stable
    return df2.reset_index(drop=True)


def _pickem_text(row, prefix: str) -> str:
    h, a = row.get(f"{prefix}_spread_home"), row.get(f"{prefix}_spread_away")
    try:
        if (h is not None and not pd.isna(h) and abs(float(h)) < 0.05) or \
           (a is not None and not pd.isna(a) and abs(float(a)) < 0.05):
            return "PK"
    except Exception:
        pass
    return "TBD"


def build_html(df: pd.DataFrame, season: int, week: int, sort_label: str, show_rest: bool) -> str:
    thresh_move = float(os.getenv("LINE_MOVE_THRESH", "2.5"))
    cards_html = []
    for _, r in df.iterrows():
        # Header with per-team DVOA badges
        away = r.get('_away') or ''
        home = r.get('_home') or ''
        header = f"{away} {badge_html(r, 'away')} @ {home} {badge_html(r, 'home')}"

        fav_open_team, fav_open_spread = favor_from_pair(r, 'open')
        fav_curr_team, fav_curr_spread = favor_from_pair(r, 'current')

        open_txt = (
            f"{fav_open_team} {fmt_spread(fav_open_spread)}" if fav_open_team else _pickem_text(r, 'open')
        )
        curr_txt = (
            f"{fav_curr_team} {fmt_spread(fav_curr_spread)}" if fav_curr_team else _pickem_text(r, 'current')
        )

        if "circa_line" in df.columns:
            circa_txt = fmt_spread(r.get("circa_line"))
        else:
            if (r.get("home_or_away", "").upper() == "HOME"):
                circa_txt = fmt_spread(r.get("circa_spread_home"))
            else:
                circa_txt = fmt_spread(r.get("circa_spread_away"))

        # Total value
        total_val = None
        for name in ("current_total", "circa_total", "open_total"):
            if name in r.index:
                v = r.get(name)
                if v is not None and not pd.isna(v):
                    total_val = v
                    break
        total_display = fmt_total(total_val) if total_val is not None else "TBD"

        # DVOA rows (both teams)
        dvoa_rows = []
        home_nm = home or "HOME"
        away_nm = away or "AWAY"
        dvoa_rows.append((f"{home_nm} DVOA (total)", fmt_pct(dvoa_value(r, 'home', 'total'))))
        dvoa_rows.append((f"{away_nm} DVOA (total)", fmt_pct(dvoa_value(r, 'away', 'total'))))
        dvoa_rows.append((f"{home_nm} Off DVOA", fmt_pct(dvoa_value(r, 'home', 'off'))))
        dvoa_rows.append((f"{home_nm} Def DVOA", fmt_pct(dvoa_value(r, 'home', 'def'))))
        dvoa_rows.append((f"{away_nm} Off DVOA", fmt_pct(dvoa_value(r, 'away', 'off'))))
        dvoa_rows.append((f"{away_nm} Def DVOA", fmt_pct(dvoa_value(r, 'away', 'def'))))

        # Chips (line move, injuries, weather)
        chips = []
        try:
            ld = r.get('line_delta_home')
            if ld is not None and not pd.isna(ld) and abs(float(ld)) >= thresh_move:
                arrow = '▲' if float(ld) < 0 else '▼'
                chips.append(f"<div class='chip'>Move {arrow} {abs(float(ld)):.1f} pts</div>")
        except Exception:
            pass
        for c in ('injuries_key_home', 'injuries_key_away'):
            val = r.get(c)
            if isinstance(val, str) and val.strip():
                title = val.replace("'", "&#39;")[:400]
                chips.append(f"<div class='chip' title='{title}'>Injuries</div>")
        val = r.get('weather_notes')
        if isinstance(val, str) and val.strip():
            title = val.replace("'", "&#39;")[:400]
            chips.append(f"<div class='chip' title='{title}'>Weather</div>")
        chips_html = ''.join(chips)

        # Build card
        body_rows = []
        body_rows.append(("Open", open_txt))
        body_rows.append(("Favorite (current)", curr_txt))
        body_rows.append(("Circa Line", circa_txt))
        body_rows.append(("Total", total_display))
        for lbl, val in dvoa_rows:
            body_rows.append((lbl, val))
        body_rows.append(("Auto-Notes", chips_html))
        body_rows.append(("Kickoff", r.get("_kickoff_pt", "")))

        rows_html = "\n".join([f"<div class='row'><span>{lbl}</span><span>{val}</span></div>" for lbl, val in body_rows])

        cards_html.append(f"""
        <div class='card'>
          <div class='header'><div class='matchup'>{header}</div></div>
          <div class='body'>
            {rows_html}
          </div>
        </div>
        """)

    cards = "\n".join(cards_html)
    html = f"""
<!doctype html><html lang='en'><head>
<meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
<title>NFL25 Millions — Member Dashboard</title>
<style>
  body {{ background:#0b0f14; color:#e2e8f0; font:16px/1.5 ui-sans-serif, system-ui; margin:0; }}
  .wrap {{ max-width:1280px; margin:24px auto; padding:0 16px; }}
  .grid {{ display:grid; grid-template-columns: repeat(auto-fill, minmax(360px,1fr)); gap:16px; }}
  .card {{ background:#141a22; border:1px solid #1f2937; border-radius:16px; padding:14px 16px; }}
  .header {{ display:flex; align-items:center; padding:10px 0 6px; border-bottom:1px solid rgba(255,255,255,.08); margin-bottom:6px; }}
  .matchup {{ font-weight:700; letter-spacing:.2px; font-size:18px; }}
  .row {{ display:flex; justify-content:space-between; padding:4px 0; border-bottom:1px dashed rgba(255,255,255,.06); }}
  .row.chips span:first-child {{ color:#94a3b8; }}
  .chip{{display:inline-block;padding:2px 6px;margin:0 4px 0 0;border:1px solid #ddd;border-radius:10px;font-size:12px}}
  .pill{{display:inline-block;font-size:12px;padding:2px 6px;margin-left:6px;border-radius:999px;border:1px solid;vertical-align:middle}}
  .pill.green{{border-color:#16a34a;color:#86efac}}
  .pill.red{{border-color:#dc2626;color:#fca5a5}}
</style></head><body>
<div class='wrap'>
<h1>NFL25 Millions — Member Dashboard</h1>
<div class='meta'>Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}. Sorted by Kickoff (PT).</div>
<div class='grid'>{cards}</div>
</div></body></html>
"""
    return html

# ==========================
# CLI
# ==========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--week", type=int, required=True)
    ap.add_argument("--planner", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--sort_by", choices=list(SORT_KEYS.keys()), default="kickoff")
    ap.add_argument("--show_rest", type=int, default=0)
    ap.add_argument("--odds_csv", default=None, help="Optional odds CSV to overlay (e.g., data/odds/season_all.csv)")
    args = ap.parse_args()

    df = load_week(Path(args.planner), args.season, args.week)
    df = overlay_from_odds(df, Path(args.odds_csv) if args.odds_csv else None)
    df = sort_week(df, args.sort_by)
    html = build_html(df, args.season, args.week, "Kickoff (PT)" if args.sort_by == "kickoff" else "None", bool(args.show_rest))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(html, encoding="utf-8")
    print(f"Exported member HTML → {args.out}")

if __name__ == "__main__":
    main()
