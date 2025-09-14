from __future__ import annotations
import time
from typing import Literal, Optional
from datetime import datetime, timezone, timedelta
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
    if end_dt < start_dt:
        return pd.DataFrame(columns=['ts','open','high','low','close','volume'])

    step_minutes = 1 if tf == 'M1' else 5
    max_bars = 1000  # límite de Binance por request
    # Ventana máxima en minutos por request
    max_minutes_per_req = max_bars * step_minutes

    base_url = BASE_URLS[region]
    url = f"{base_url}/api/v3/klines"

    out = []
    cursor = start_dt
    # Protección anti-bucle: máximo número de requests razonable por día
    max_requests = 10
    requests_done = 0

    while cursor <= end_dt and requests_done < max_requests:
        requests_done += 1
        # Sub-ventana [cursor, win_end]
        win_end = min(end_dt, cursor + timedelta(minutes=max_minutes_per_req - step_minutes))
        # Número esperado de barras en esta sub-ventana
        expected = int(((win_end - cursor).total_seconds() // 60) / step_minutes) + 1
        params = {
            'symbol': symbol,
            'interval': _INTERVALS[tf],
            'startTime': _to_ms(cursor),
            'endTime': _to_ms(win_end),
            'limit': min(max_bars, expected)
        }
        r = _rate_limited_get(url, params)
        data = r.json()
        if not isinstance(data, list):
            raise BinanceHTTPError(f"Respuesta inesperada: {data}")

        if not data:
            # Si no hubo datos, avanzar la ventana para evitar estancarse
            cursor = win_end + timedelta(minutes=step_minutes)
            continue

        cols = [
            'openTime','open','high','low','close','volume','closeTime',
            'qav','numTrades','takerBuyBase','takerBuyQuote','ignore'
        ]
        df = pd.DataFrame(data, columns=cols)
        if df.empty:
            cursor = win_end + timedelta(minutes=step_minutes)
            continue

        df['ts'] = pd.to_datetime(df['openTime'], unit='ms', utc=True)
        for c in ('open','high','low','close','volume'):
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df[['ts','open','high','low','close','volume']]
        # Clip de seguridad (por si Binance devolvió de más)
        df = df[(df['ts'] >= cursor) & (df['ts'] <= win_end)]
        out.append(df)

        # Avanzar cursor al siguiente minuto (o 5 min) después de win_end
        cursor = win_end + timedelta(minutes=step_minutes)

    if not out:
        return pd.DataFrame(columns=['ts','open','high','low','close','volume'])

    res = pd.concat(out, ignore_index=True).drop_duplicates(subset=['ts']).sort_values('ts').reset_index(drop=True)
    # Clip final absoluto
    res = res[(res['ts'] >= start_dt) & (res['ts'] <= end_dt)].reset_index(drop=True)
    return res
