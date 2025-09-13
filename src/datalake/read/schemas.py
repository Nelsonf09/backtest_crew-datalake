from typing import List
import pandas as pd

CANONICAL_ORDER: List[str] = [
    "ts","open","high","low","close","volume",
    "source","market","timeframe","symbol",
    "exchange","what_to_show","vendor","tz"
]

NUMERIC = {"open","high","low","close","volume"}
TEXTUAL = {"source","market","timeframe","symbol","exchange","what_to_show","vendor","tz"}

DEFAULTS = {
    "source": "ibkr",
    "market": "crypto",
    "timeframe": "M1",
    "exchange": "PAXOS",
    "what_to_show": "AGGTRADES",
    "vendor": "ibkr",
    "tz": "UTC",
}

def enforce_schema(df: pd.DataFrame, timeframe: str = None, symbol: str = None) -> pd.DataFrame:
    d = df.copy()
    # ts datetime con tz UTC
    d["ts"] = pd.to_datetime(d["ts"], utc=True)
    # numéricos a float
    for c in NUMERIC:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
        else:
            d[c] = 0.0
    # textuales a string
    for c in TEXTUAL:
        if c not in d.columns:
            d[c] = DEFAULTS.get(c, "")
        d[c] = d[c].astype("string")
    if timeframe:
        d["timeframe"] = str(timeframe)
    if symbol:
        d["symbol"] = str(symbol)
    # ordenar columnas si existen
    cols = [c for c in CANONICAL_ORDER if c in d.columns]
    # agrega columnas extra al final (si hubiera)
    extra = [c for c in d.columns if c not in cols]
    d = d[cols + extra]
    return d
