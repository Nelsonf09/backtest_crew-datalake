import pandas as pd
from typing import Union

EXCHANGE_TZ = {
    'PAXOS': 'America/New_York',
}

def to_utc(s: Union[pd.Series, list], exchange: str) -> pd.Series:
    tz = EXCHANGE_TZ.get(exchange, 'UTC')
    dt = pd.to_datetime(s, errors="coerce", utc=False)
    # Si la serie es naive, localiza primero en la tz del exchange
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize(tz)
    else:
        dt = dt.dt.tz_convert(tz)
    # Luego convierte a UTC
    return dt.dt.tz_convert("UTC")
