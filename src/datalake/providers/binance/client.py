from __future__ import annotations
import time
import math
from typing import Literal, Optional
from datetime import datetime, timezone
import requests
import pandas as pd

UTC = timezone.utc

BASE_URLS = {
    'global': 'https://api.binance.com',
    'us': 'https://api.binance.us',
}

_INTERVALS = {
    'M1': '1m',
    'M5': '5m',
}

class BinanceHTTPError(Exception):
    pass

def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def _rate_limited_get(url: str, params: dict, max_retries: int = 5, timeout: int = 30) -> requests.Response:
    for i in range(max_retries):
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            sleep_s = min(2 ** i, 10)
            time.sleep(sleep_s)
            continue
        if 200 <= r.status_code < 300:
            return r
        # 5xx retry suave
        if 500 <= r.status_code < 600:
            time.sleep(1.5)
            continue
        # Errores duros
        raise BinanceHTTPError(f"HTTP {r.status_code}: {r.text}")
    raise BinanceHTTPError(f"Rate limit persistente: {r.status_code} {r.text}")

def fetch_klines(
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
    tf: Literal['M1','M5'] = 'M1',
    region: Literal['global','us'] = 'global'
) -> pd.DataFrame:
    if tf not in _INTERVALS:
        raise ValueError(f"Intervalo no soportado para Binance: {tf}")
    if start_dt.tzinfo is None or end_dt.tzinfo is None:
        raise ValueError("start_dt y end_dt deben ser tz-aware UTC")
    base_url = BASE_URLS[region]
    url = f"{base_url}/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': _INTERVALS[tf],
        'startTime': _to_ms(start_dt),
        'endTime': _to_ms(end_dt),
        'limit': 1000
    }
    r = _rate_limited_get(url, params)
    data = r.json()
    if not isinstance(data, list):
        raise BinanceHTTPError(f"Respuesta inesperada: {data}")
    cols = [
        'openTime','open','high','low','close','volume','closeTime',
        'qav','numTrades','takerBuyBase','takerBuyQuote','ignore'
    ]
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        return df
    df['ts'] = pd.to_datetime(df['openTime'], unit='ms', utc=True)
    for c in ('open','high','low','close','volume'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    # Devolver sólo columnas del timeseries
    df = df[['ts','open','high','low','close','volume']].sort_values('ts').reset_index(drop=True)
    # Clip de seguridad por límites de API
    df = df[(df['ts'] >= start_dt) & (df['ts'] <= end_dt)].reset_index(drop=True)
    return df
