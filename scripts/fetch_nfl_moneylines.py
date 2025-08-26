#!/usr/bin/env python3
# --- path bootstrap: keep at very top ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# ---------------------------------------

"""Compatibility wrapper: delegates to scripts.fetch_nfl_odds
This keeps old commands working while the canonical logic lives in fetch_nfl_odds.py.
"""
from scripts.fetch_nfl_odds import main as _odds_main

if __name__ == "__main__":
    _odds_main()