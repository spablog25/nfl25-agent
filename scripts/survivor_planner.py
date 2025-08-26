import pandas as pd
import os
import numpy as np

# === Paths ===
base_dir = os.path.dirname(os.path.dirname(__file__))
src_file = os.path.join(base_dir, 'picks', 'survivor', 'survivor_roadmap_expanded.csv')

# === Config ===
WEEK_TO_VIEW = 4
THANKSGIVING_WEEK = 13
CHRISTMAS_WEEK = 17
THANKSGIVING_TEAMS = {"GB","DET","KC","DAL","CIN","BAL"}
CHRISTMAS_TEAMS = {"DET","MIN","DAL","WSH","DEN","KC"}

def value_from_prob(p):
    if pd.isna(p): return 'Low'
    try:
        p = float(p)
    except:
        return 'Low'
    if p >= 0.75: return 'High'
    if p >= 0.60: return 'Medium'
    return 'Low'

def downgrade(v):
    return 'Medium' if v == 'High' else ('Low' if v == 'Medium' else 'Low')

# === Load ===
df = pd.read_csv(src_file)
for c in ['team','opponent','home_or_away','holiday_flag','must_use','save_for_later','is_locked_out','reserved']:
    if c in df.columns:
        df[c] = df[c].astype(str).str.strip()

df['projected_win_prob'] = pd.to_numeric(df.get('projected_win_prob'), errors='coerce')

# === Filter to week ===
week_df = df[df['week'] == WEEK_TO_VIEW].copy()

# === Compute v1 spot value ===
vals = []
for _, r in week_df.iterrows():
    team = r['team']
    base = value_from_prob(r.get('projected_win_prob'))
    # Save-before-holiday rule
    if team in THANKSGIVING_TEAMS and WEEK_TO_VIEW < THANKSGIVING_WEEK:
        base = 'Low'
    if team in CHRISTMAS_TEAMS and WEEK_TO_VIEW < CHRISTMAS_WEEK:
        base = 'Low'
    # Overrides
    if str(r.get('must_use','')).lower() in ('yes','y','true','1'):
        base = 'High'
    elif str(r.get('save_for_later','')).lower() in ('yes','y','true','1'):
        base = downgrade(base)
    vals.append(base)

week_df['spot_value_v1'] = vals

# === Exclude locked/reserved teams if applicable ===
mask_locked = week_df['is_locked_out'].str.lower().isin(['yes','y','true','1'])
mask_reserved = week_df['reserved'].str.lower().isin(['yes','y','true','1'])
eligible = week_df[~mask_locked & ~mask_reserved].copy()

print(f"\n=== Survivor Planner - Week {WEEK_TO_VIEW} ===")
if eligible.empty:
    print("No eligible teams found for this week with current filters.")
else:
    cols = ['team','opponent','home_or_away','projected_win_prob','holiday_flag','spot_value_v1','must_use','save_for_later']
    present_cols = [c for c in cols if c in eligible.columns]
    print(eligible[present_cols].sort_values(['spot_value_v1','projected_win_prob'], ascending=[True, False]).reset_index(drop=True))

print("\n🏈 Survivor planning view ready")