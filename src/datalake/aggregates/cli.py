from __future__ import annotations
import argparse
from rich import print
from datalake.config import LakeConfig
from datalake.aggregates.loader import load_m1_range
from datalake.aggregates.aggregate import aggregate_symbol
from datetime import datetime, timezone


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='Generar agregados OHLCV (M5/M15/H1/D1) desde M1')
    ap.add_argument('--symbols', required=True, help='BTC-USD,ETH-USD,...')
    ap.add_argument('--from', dest='date_from', required=True, help='YYYY-MM-DD (UTC)')
    ap.add_argument('--to', dest='date_to', required=True, help='YYYY-MM-DD (UTC)')
    ap.add_argument('--to-tf', default='M5,M15,H1,D1', help='Lista de TFs destino (M5,M15,H1,D1)')
    args = ap.parse_args(argv)

    cfg = LakeConfig(); tfs = [t.strip().upper() for t in args.to_tf.split(',') if t.strip()]
    allowed = {'M5':288, 'M15':96, 'H1':24, 'D1':1}
    tfs = [t for t in tfs if t in allowed]
    date_from = datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    date_to = datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days = (date_to - date_from).days + 1
    expected_m1 = days * 1440
    for s in [x.strip() for x in args.symbols.split(',') if x.strip()]:
        start = args.date_from+' 00:00:00Z'
        end = args.date_to+' 23:59:59Z'
        raw = load_m1_range(s, start, end, cfg)
        if len(raw) != expected_m1:
            print(f"[red]Falta M1[/red] {s} {args.date_from}→{args.date_to} (esperado {expected_m1}, obtenido {len(raw)})")
            return 1
        print(f"[bold]Agregando[/bold] {s} {args.date_from}→{args.date_to} ({','.join(tfs)})")
        for tf in tfs:
            print(f"[cyan]Esperadas[/cyan] {allowed[tf]*days} filas {tf}")
        results = aggregate_symbol(s, start, end, tfs, lambda *_: raw, cfg)
        for tf, paths in results.items():
            for p in paths:
                print(f"[green]OK[/green] {s} {tf} → {p}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
