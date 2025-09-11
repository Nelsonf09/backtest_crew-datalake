from __future__ import annotations
import argparse
import sys
import pandas as pd
from rich import print
from datalake.config import LakeConfig
from datalake.ingestors.ibkr.historical_fetcher import fetch_crypto_m1_range
from datalake.ingestors.ibkr.writer import write_month


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='Ingesta M1 cripto (AGGTRADES) → Parquet particionado')
    ap.add_argument('--symbols', required=True, help='Lista separada por comas, p.ej. BTC-USD,ETH-USD')
    ap.add_argument('--from', dest='date_from', required=True, help='Inicio UTC, p.ej. 2025-07-01')
    ap.add_argument('--to', dest='date_to', required=True, help='Fin UTC, p.ej. 2025-08-31')
    args = ap.parse_args(argv)

    cfg = LakeConfig()
    symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]

    for sym in symbols:
        print(f"[bold]Ingestando[/bold] {sym} {args.date_from}→{args.date_to}")
        df = fetch_crypto_m1_range(sym, args.date_from + ' 00:00:00Z', args.date_to + ' 23:59:59Z')
        if df.empty:
            print(f"[yellow]Sin datos para {sym}[/yellow]")
            continue
        # escribir por meses
        df['year'] = pd.to_datetime(df['ts'], utc=True).dt.year
        df['month'] = pd.to_datetime(df['ts'], utc=True).dt.month
        for (y, m), chunk in df.groupby(['year','month']):
            path = write_month(chunk.drop(columns=['year','month']), symbol=sym, cfg=cfg)
            print(f"[green]OK[/green] {sym} → {path}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
