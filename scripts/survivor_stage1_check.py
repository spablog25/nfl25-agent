import pandas as pd
import os

# === Paths ===
base_dir = os.path.dirname(os.path.dirname(__file__))  # .. from /scripts
roadmap_file = os.path.join(base_dir, 'picks', 'survivor', 'survivor_schedule_roadmap.csv')
output_file = os.path.join(base_dir, 'picks', 'survivor', 'survivor_schedule_roadmap_stage1.csv')

# === Load ===
df = pd.read_csv(roadmap_file)
df.columns = [c.strip() for c in df.columns]  # trim any stray spaces

# === 1) Column + sample preview ===
expected = [
    'week','team','opponent','home_or_away','reserved','is_locked_out','expected_avail',
    'spot_value','projected_win_prob','notes_future','spot_quality','preferred','must_use',
    'save_for_later','notes'
]

print("\n[Survivor Roadmap Columns]:")
print(df.columns.tolist())

missing = [c for c in expected if c not in df.columns]
if missing:
    print(f"\n⚠️ Missing expected columns: {missing}")
else:
    print("\n✅ All expected columns present.")

print("\n[First 10 Rows Preview]:")
print(df.head(10))

# === 2) Duplicate (week,team) guard ===
dups = df[df.duplicated(subset=['week','team'], keep=False)]
if not dups.empty:
    print("\n⚠️ Duplicate (week, team) rows found:")
    print(dups.sort_values(['team','week']))
else:
    print("\n✅ No duplicate (week, team) rows.")

# === 3) Weeks-per-team sanity check ===
weeks_per_team = df.groupby('team')['week'].nunique().sort_values()
print("\n[Weeks per team (unique weeks seen)]:")
print(weeks_per_team)

# Note: This is informational. Teams typically have 18 games + 1 bye (and your dataset keeps BYE rows).
# We won't hard-fail if it's not exactly a specific number; you'll review if anything looks off.

# === 4) Flag Thanksgiving & Christmas (2025) ===
# Thanksgiving = Week 13
#   GB @ DET, KC @ DAL, CIN @ BAL (and corresponding home/away inverses in your per-team rows)
# Christmas = Week 17
#   DET @ MIN, DAL @ WSH, DEN @ KC (and inverses)
thanksgiving_pairs = [
    ("GB", 13, "DET"), ("DET", 13, "GB"),
    ("KC", 13, "DAL"), ("DAL", 13, "KC"),
    ("CIN", 13, "BAL"), ("BAL", 13, "CIN"),
]
christmas_pairs = [
    ("DET", 17, "MIN"), ("MIN", 17, "DET"),
    ("DAL", 17, "WSH"), ("WSH", 17, "DAL"),
    ("DEN", 17, "KC"),  ("KC", 17, "DEN"),
]

df['holiday_flag'] = ""

# Apply flags
df.loc[df['week'].eq(13) & df['team'].isin([t for t,_,_ in thanksgiving_pairs]), 'holiday_flag'] = "Thanksgiving"
df.loc[df['week'].eq(17) & df['team'].isin([t for t,_,_ in christmas_pairs]),   'holiday_flag'] = "Christmas"

# === 5) Verify holiday rows exist and match expected opponents ===
def check_pairs(pairs, label):
    missing_rows = []
    opponent_mismatches = []
    for team, wk, opp in pairs:
        mask = (df['team'] == team) & (df['week'] == wk)
        sub = df.loc[mask]
        if sub.empty:
            missing_rows.append((team, wk, opp))
            continue
        # If row exists, confirm opponent matches what we expect
        seen_opps = set(sub['opponent'].astype(str).str.strip().tolist())
        if opp not in seen_opps:
            opponent_mismatches.append((team, wk, opp, list(seen_opps)))
    if missing_rows:
        print(f"\n⚠️ {label}: Missing rows for (team, week, expected_opp):")
        for row in missing_rows:
            print("   ", row)
    else:
        print(f"\n✅ {label}: All teams present.")
    if opponent_mismatches:
        print(f"\n⚠️ {label}: Opponent mismatches found:")
        for t, w, exp, seen in opponent_mismatches:
            print(f"   Team {t} Week {w}: expected opp '{exp}', saw {seen}")
    else:
        print(f"✅ {label}: All opponents match expected.")

check_pairs(thanksgiving_pairs, "Thanksgiving")
check_pairs(christmas_pairs,   "Christmas")

# === 6) Show holiday rows for quick visual check ===
print("\n[Thanksgiving rows detected]:")
print(df[(df['holiday_flag'] == "Thanksgiving")][['week','team','opponent','home_or_away','holiday_flag']].sort_values(['week','team']))

print("\n[Christmas rows detected]:")
print(df[(df['holiday_flag'] == "Christmas")][['week','team','opponent','home_or_away','holiday_flag']].sort_values(['week','team']))

# === 7) Save updated CSV ===
df.to_csv(output_file, index=False)
print(f"\n✅ Stage 1 complete. Updated file saved to:\n{output_file}")