# scripts/utils_dvoa.py
from __future__ import annotations
import pandas as pd
from pathlib import Path
import sys

# path bootstrap
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from scripts.utils_read import read_csv_safe
except Exception:
    read_csv_safe = None

def load_dvoa(path: str | Path = None) -> pd.DataFrame:
    """
    Load and normalize DVOA to: team, off_dvoa, def_dvoa, total_dvoa
    - Uppercases team codes/names
    - Accepts common alternate column names
    """
    if path is None:
        path = ROOT / "data" / "dvoa_data.csv"
    path = Path(path)

    df = read_csv_safe(path) if read_csv_safe else pd.read_csv(path)

    lower_map = {c.lower(): c for c in df.columns}
    def pick(*cands):
        for c in cands:
            if c in lower_map:
                return lower_map[c]
        return None

    team_col = pick("team", "abbr", "team_code", "tm", "teamname")
    off_col  = pick("off_dvoa", "offense_dvoa", "offdvoa", "off")
    def_col  = pick("def_dvoa", "defense_dvoa", "defdvoa", "def")
    tot_col  = pick("total_dvoa", "dvoa_total", "dvoa", "total")

    if team_col is None:
        raise ValueError("DVOA file missing a team column (e.g., 'team' or 'abbr').")

    out = pd.DataFrame()
    out["team"] = df[team_col].astype(str).str.upper().str.strip()

    to_num = lambda s: pd.to_numeric(s, errors="coerce")
    out["off_dvoa"] = to_num(df[off_col]) if off_col else 0.0
    out["def_dvoa"] = to_num(df[def_col]) if def_col else 0.0
    if tot_col:
        out["total_dvoa"] = to_num(df[tot_col])
    else:
        out["total_dvoa"] = out["off_dvoa"] - out["def_dvoa"]

    return out[["team", "off_dvoa", "def_dvoa", "total_dvoa"]]
