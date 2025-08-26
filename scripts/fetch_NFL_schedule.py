#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

SCHEDULE_CSV = Path('data/2025_nfl_schedule_cleaned.csv')
RAW_HTML_OUT = Path('scripts/raw_schedule.html')  # optional local preview

def main():
    df = pd.read_csv(SCHEDULE_CSV, dtype=str).fillna('')
    # Quick sanity check
    assert {'week','vistm','hometm'}.issubset(df.columns), 'Schedule CSV missing required columns'
    # If you want a quick browser view while developing:
    RAW_HTML_OUT.write_text(df.head(100).to_html(index=False), encoding='utf-8')
    print('Loaded schedule rows:', len(df))

if __name__ == '__main__':
    main()