# scripts/teams.py
from __future__ import annotations

ABBREV_SET = set(
    "ARI ATL BAL BUF CAR CHI CIN CLE DAL DEN DET GB HOU IND JAX KC LAC LAR LV MIA MIN NE NO NYG NYJ PHI PIT SEA SF TB TEN WAS".split()
)

ALIASES = {
    # Washington variants
    "WSH": "WAS",
    "WASHINGTON": "WAS",
    "WASHINGTON COMMANDERS": "WAS",
    # LA variants
    "LOS ANGELES RAMS": "LAR",
    "LOS ANGELES CHARGERS": "LAC",
    "LAR": "LA",
    # Vegas
    "LAS VEGAS RAIDERS": "LV",
    # Common alternates
    "JAC": "JAX",
    "TAM": "TB",
    "NO": "NOR",
    "LV": "OAK",
}

def norm_team(x: str) -> str:
    if not isinstance(x, str):
        return x
    t = x.strip().upper()
    t = ALIASES.get(t, t)
    return t