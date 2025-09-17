# tools/fetch_binance_tail_range.py
# Descarga klines 1m de Binance para [20:00–23:59 UTC] y reporta huecos.
# Por defecto usa Binance global (api.binance.com). Para Binance.US pasa --us.

import argparse, os, sys, time
from datetime import datetime, timezone
import requests
import pandas as pd

UTC = timezone.utc

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def missing_report(df: pd.DataFrame, start_dt: datetime, end_dt: datetime):
    exp = pd.date_range(start=start_dt, end=end_dt, freq="min", tz="UTC")
    got = set(df["ts"]) if not df.empty else set()
    miss = [t for t in exp if t not in got]
    ranges = []
    if miss:
        run_start = miss[0]; prev = miss[0]
        for t in miss[1:]:
            if (t - prev) == pd.Timedelta(minutes=1):
                prev = t
            else:
                ranges.append((run_start, prev))
                run_start = t; prev = t
        ranges.append((run_start, prev))
    return len(miss), ranges

def fetch_binance_klines(symbol: str, start_dt: datetime, end_dt: datetime, base_url: str):
    """
    GET /api/v3/klines?symbol=BTCUSDT&interval=1m&startTime=...&endTime=...
    Devuelve lista de velas: [openTime, open, high, low, close, volume, closeTime, ...]
    """
    url = f"{base_url}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": to_ms(start_dt),
        "endTime": to_ms(end_dt),
        "limit": 1000  # suficiente para 240 velas
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    cols = ["openTime","open","high","low","close","volume","closeTime",
            "qav","numTrades","takerBuyBase","takerBuyQuote","ignore"]
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        return df
    # Timestamps a UTC y columnas numéricas
    df["ts"] = pd.to_datetime(df["openTime"], unit="ms", utc=True)
    num_cols = ["open","high","low","close","volume"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # Filtrar EXACTAMENTE el rango minuto-cerrado [20:00–23:59]
    df = df[(df["ts"] >= start_dt) & (df["ts"] <= end_dt)].copy()
    df = df[["ts","open","high","low","close","volume"]].sort_values("ts").reset_index(drop=True)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Fecha UTC YYYY-MM-DD")
    ap.add_argument("--symbol", default="BTC-USD", help="Símbolo lógico (se mapea por defecto a BTCUSDT)")
    ap.add_argument("--binance-symbol", default=None, help="Símbolo Binance (ej. BTCUSDT). Si se pasa, ignora --symbol")
    ap.add_argument("--us", action="store_true", help="Usar Binance.US (api.binance.us)")
    ap.add_argument("--out", default=None, help="Ruta CSV de salida")
    args = ap.parse_args()

    # Resolver exchange base
    base_url = "https://api.binance.us" if args.us else "https://api.binance.com"

    # Mapear símbolo por defecto (BTC-USD -> BTCUSDT)
    if args.binance_symbol:
        b_symbol = args.binance_symbol.upper()
    else:
        if args.symbol.upper() in ("BTC-USD", "BTCUSD", "BTC_USD"):
            b_symbol = "BTCUSDT"  # par spot más líquido
        else:
            # Regla simple: quitar guión y asegurar mayúsculas (p.ej. ETH-USD -> ETHUSDT)
            b_symbol = args.symbol.replace("-", "").upper()
            if b_symbol.endswith("USD"):
                b_symbol += "T"  # convertir a USDT si terminó en USD

    # Construir rango UTC [20:00–23:59]
    D = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=UTC)
    start_dt = D.replace(hour=20, minute=0, second=0, microsecond=0)
    end_dt   = D.replace(hour=23, minute=59, second=0, microsecond=0)

    print(f"Binance base: {base_url}")
    print(f"Símbolo Binance: {b_symbol}")
    print(f"Rango objetivo: {start_dt.isoformat()} → {end_dt.isoformat()}")

    # Descarga
    try:
        df = fetch_binance_klines(b_symbol, start_dt, end_dt, base_url)
    except requests.HTTPError as e:
        print(f"HTTPError: {e} | cuerpo={getattr(e.response, 'text', '')}")
        sys.exit(2)
    except Exception as e:
        print(f"Error inesperado: {e}")
        sys.exit(2)

    rows = 0 if df is None else len(df)
    miss, ranges = missing_report(df if df is not None else pd.DataFrame({"ts":[]}), start_dt, end_dt)

    print(f"Velas obtenidas: {rows} (esperado 240) | missing_minutos={miss}")
    if ranges:
        print("missing_ranges:", [(a.isoformat(), b.isoformat()) for a,b in ranges])
    if not df.empty:
        print("Primeras 3 filas:\n", df.head(3).to_string(index=False))
        print("Últimas 3 filas:\n", df.tail(3).to_string(index=False))

    # Salida
    out_path = args.out or os.path.join(os.getcwd(), "tmp", f"{args.symbol}_{args.date}_2000_2359_UTC_binance.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"CSV guardado en: {out_path}")

    # Exit útil
    sys.exit(0 if (rows >= 240 and miss == 0) else 3)

if __name__ == "__main__":
    main()
