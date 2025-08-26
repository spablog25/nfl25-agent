import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SURV_DIR = os.path.join(BASE_DIR, "picks", "survivor")

ROADMAP = os.path.join(SURV_DIR, "survivor_roadmap_expanded.csv")
PICKS = os.path.join(SURV_DIR, "survivor_weekly_picks.csv")

TG_WEEK = 13
XMAS_WEEK = 17

def norm(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def main():
    roadmap = norm(pd.read_csv(ROADMAP))
    picks = norm(pd.read_csv(PICKS))

    issues = []

    # Map (week, team) -> opponent, holiday_flag
    sched = {(int(r.week), str(r.team).upper()): 
             {"opp": str(r.opponent).upper() if pd.notna(r.opponent) else "", 
              "holiday": (str(r.holiday_flag).title() if pd.notna(r.holiday_flag) else "")}
             for _, r in roadmap.iterrows()}

    # 1) no team picked more than once
    multi = picks.groupby(picks['team'].str.upper())['week'].nunique()
    dup_teams = multi[multi > 1].index.tolist()
    if dup_teams:
        issues.append(f"❌ Team picked more than once: {', '.join(dup_teams)}")

    # 2) BYE week picks
    for _, r in picks.iterrows():
        wk = int(r.week); tm = str(r.team).upper()
        sk = sched.get((wk, tm))
        if not sk:
            issues.append(f"❌ {tm} picked in week {wk}, but no schedule found.")
            continue
        if sk["opp"] == "BYE":
            issues.append(f"❌ {tm} picked in week {wk}, but team is on BYE.")

    # 3) Picking a holiday team before its holiday (optional warning)
    # If a team has a TG/Xmas game, warn if it’s picked earlier than that holiday week.
    holiday_weeks = {}
    for (wk, tm), v in sched.items():
        if v["holiday"] in ("Thanksgiving", "Christmas"):
            holiday_weeks.setdefault(tm, []).append(wk)

    for _, r in picks.iterrows():
        wk = int(r.week); tm = str(r.team).upper()
        hweeks = holiday_weeks.get(tm, [])
        if hweeks:
            if any(wk < hw for hw in hweeks):
                issues.append(f"⚠️ {tm} picked week {wk} but has holiday slot(s) at week(s) {hweeks}.")

    # Results
    if issues:
        print("\n".join(issues))
    else:
        print("✅ Survivor state looks consistent.")

if __name__ == "__main__":
    main()
