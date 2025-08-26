import pandas as pd
from pathlib import Path

"""
Purpose
- Join Sean's likes matrix to the master schedule.
- Fixes the TypeError you hit (float vs str) by coercing to strings and
  safely handling missing opponents.
- Uses week numbers that match your schedule (TG->12, CH->16).

Inputs (adjust paths):
- LIKES_CSV: wide matrix with columns Team, W1..W16, TG, CH
  Cell format: "OPP (H)" or "OPP (A)" or blank
- SCHEDULE_CSV: columns week, date, time, vistm, hometm (plus optional venue)

Output:
- OUT_CSV: per-like row with schedule info attached
"""

# ---- Paths (edit as needed) ----
LIKES_CSV = Path("data/survivor_bg_likes.csv")
SCHEDULE_CSV = Path("data/2025_nfl_schedule_cleaned.csv")
OUT_CSV = Path("data/survivor_bg_likes_with_sched.csv")

# ---- Load ----
likes = pd.read_csv(LIKES_CSV, dtype=str).fillna("")
sched = pd.read_csv(SCHEDULE_CSV, dtype=str).fillna("")

# ---- Normalize codes (common aliases) ----
code_fix = {
    "WAS": "WSH", "ARZ": "ARI", "LA": "LAR", "STL": "LAR",
    "SD": "LAC", "OAK": "LV", "JAC": "JAX"
}

if "Team" in likes.columns:
    likes["Team"] = likes["Team"].str.upper().str.strip().replace(code_fix)

# Any week columns present
WEEK_COLS = [c for c in likes.columns if c.startswith("W") or c in ("TG", "CH")]

# ---- Melt likes to long ----
like_long = likes.melt(id_vars=["Team"], value_vars=WEEK_COLS,
                       var_name="week", value_name="opp_cell")
like_long = like_long[like_long["opp_cell"].astype(str).str.strip() != ""].copy()

# Parse opponent + home/away
like_long["opponent"] = like_long["opp_cell"].str.extract(r"([A-Z]{2,3})").fillna("").replace(code_fix)
like_long["home_or_away"] = like_long["opp_cell"].str.extract(r"\((H|A)\)").fillna("")

# Map TG/CH to schedule weeks (12, 16). Adjust if your season map differs.
week_map = {f"W{i}": i for i in range(1, 17)}
week_map.update({"TG": 12, "CH": 16})
like_long["week_num"] = like_long["week"].map(week_map)

# ---- Prepare schedule ----
# Normalize codes
for col in ("vistm", "hometm"):
    sched[col] = sched[col].str.upper().str.strip().replace(code_fix)

# Ensure schedule week is numeric to match week_num
sched["week_num"] = sched["week"].astype(str).str.extract(r"(\d+)")
sched["week_num"] = sched["week_num"].astype(int)

# Build a team-pair key (orderless) so we don't depend on H/A to match
sched["_key"] = sched.apply(lambda r: "::".join(sorted([r.vistm, r.hometm])), axis=1)
like_long["_key"] = like_long.apply(lambda r: "::".join(sorted([str(r.Team), str(r.opponent)])), axis=1)

# ---- Merge ----
like_sched = like_long.merge(
    sched,
    on=["_key", "week_num"],
    how="left",
    suffixes=("", "_sched")
)

# Helpful columns first
ordered = [
    "week", "week_num", "Team", "opponent", "home_or_away",
    "date", "time", "vistm", "hometm"
] + [c for c in like_sched.columns if c not in {"week","week_num","Team","opponent","home_or_away","date","time","vistm","hometm","_key","opp_cell"}]
like_sched = like_sched[ordered]

# ---- Diagnostics: flag rows that didn't match a schedule row ----
like_sched["schedule_match"] = (~like_sched["vistm"].isna()) & (like_sched["vistm"] != "")

# Save
like_sched.to_csv(OUT_CSV, index=False)
print(f"Saved → {OUT_CSV}")

# Print quick summary for sanity
total = len(like_sched)
matched = int(like_sched["schedule_match"].sum())
print(f"Matched {matched}/{total} likes to schedule ({matched/total:.1%}).")
