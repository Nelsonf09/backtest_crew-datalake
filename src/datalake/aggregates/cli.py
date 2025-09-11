from __future__ import annotations
import argparse
from rich import print
from datalake.config import LakeConfig
from datalake.aggregates.loader import load_m1_range
from datalake.aggregates.aggregate import aggregate_symbol


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='Generar agregados OHLCV (M5/M15/H1/D1) desde M1')
    ap.add_argument('--symbols', required=True, help='BTC-USD,ETH-USD,...')
    ap.add_argument('--from', dest='date_from', required=True, help='YYYY-MM-DD (UTC)')
    ap.add_argument('--to', dest='date_to', required=True, help='YYYY-MM-DD (UTC)')
    ap.add_argument('--tfs', default='M5,M15,H1,D1', help='Lista de TFs')
    args = ap.parse_args(argv)

    cfg = LakeConfig(); tfs = [t.strip().upper() for t in args.tfs.split(',') if t.strip()]
    for s in [x.strip() for x in args.symbols.split(',') if x.strip()]:
        print(f"[bold]Agregando[/bold] {s} {args.date_from}→{args.date_to} ({','.join(tfs)})")
        results = aggregate_symbol(s, args.date_from+' 00:00:00Z', args.date_to+' 23:59:59Z', tfs, load_m1_range, cfg)
        for tf, paths in results.items():
            for p in paths:
                print(f"[green]OK[/green] {s} {tf} → {p}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
