# scripts/utils_read.py
import pandas as pd
from pathlib import Path

def read_csv_safe(path: str | Path, **kwargs) -> pd.DataFrame:
    """
    Load a CSV safely:
      - Ensures file exists
      - Returns DataFrame with no index issues
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV not found: {p}")
    try:
        return pd.read_csv(p, **kwargs)
    except Exception as e:
        raise RuntimeError(f"Error reading CSV {p}: {e}")
