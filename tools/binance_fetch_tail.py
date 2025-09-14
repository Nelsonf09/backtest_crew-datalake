from __future__ import annotations
import argparse, os
from datetime import datetime, timezone
import pandas as pd
from datalake.providers.binance.client import fetch_klines
from datalake.utils.symbols.binance_map import to_binance_symbol

UTC = timezone.utc

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', required=True, help='YYYY-MM-DD (UTC)')
    ap.add_argument('--symbol', required=True, help='Símbolo lógico, p.ej. BTC-USD')
    ap.add_argument('--region', choices=['global','us'], default='global')
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    b_sym = to_binance_symbol(args.symbol)
    D = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=UTC)
    start = D.replace(hour=20, minute=0, second=0)
    end   = D.replace(hour=23, minute=59, second=0)

    df = fetch_klines(b_sym, start, end, tf='M1', region=args.region)
    print(f"filas={len(df)} range={df['ts'].min() if not df.empty else None} -> {df['ts'].max() if not df.empty else None}")

    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        df.to_csv(args.out, index=False)
        print(f"CSV: {args.out}")

if __name__ == '__main__':
    main()
