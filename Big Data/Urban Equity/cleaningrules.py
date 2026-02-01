from __future__ import annotations
import re
from typing import Optional
import pandas as pd

REGION_CANON = {
    "metropolitana":"Metropolitana",
    "valparaíso":"Valparaíso","valparaiso":"Valparaíso",
    "biobío":"Biobío","biobio":"Biobío",
    "o'higgins":"O'Higgins","ohiggins":"O'Higgins",
    "coquimbo":"Coquimbo","los lagos":"Los Lagos",
    "antofagasta":"Antofagasta","maule":"Maule",
    "la araucanía":"La Araucanía","la araucania":"La Araucanía",
    "atacama":"Atacama",
}

def normalize_region(value: str) -> str:
    if value is None: return ""
    v=str(value).strip(); key=v.lower()
    return REGION_CANON.get(key, v.title())

def parse_int_maybe(value) -> Optional[int]:
    if value is None: return None
    s=str(value).strip()
    if s=="": return None
    s=re.sub(r"[,_\s]","",s)
    try: return int(float(s))
    except ValueError: return None

def parse_float_maybe(value) -> Optional[float]:
    if value is None: return None
    s=str(value).strip()
    if s=="": return None
    s=s.replace(",","")
    try: return float(s)
    except ValueError: return None

def clamp(series: pd.Series, lo: float, hi: float) -> pd.Series:
    return series.clip(lower=lo, upper=hi)
