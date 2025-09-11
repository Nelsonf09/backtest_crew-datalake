from __future__ import annotations
import argparse
from rich import print
from datalake.config import LakeConfig
from datalake.levels.or_levels import build_or_levels, write_year_levels


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='Niveles OR + Break & Retest (offline)')
    ap.add_argument('--symbols', required=True, help='BTC-USD,ETH-USD,...')
    ap.add_argument('--from', dest='date_from', required=True, help='YYYY-MM-DD (UTC)')
    ap.add_argument('--to', dest='date_to', required=True, help='YYYY-MM-DD (UTC)')
    ap.add_argument('--or-window', default='00:00-01:00', help='Ventana OR en hora local (HH:MM-HH:MM)')
    ap.add_argument('--tz', default='UTC', help='Zona horaria local para OR')
    args = ap.parse_args(argv)

    cfg = LakeConfig()
    for s in [x.strip() for x in args.symbols.split(',') if x.strip()]:
        print(f"[bold]Niveles[/bold] {s} {args.date_from}→{args.date_to} (OR={args.or_window} {args.tz})")
        df = build_or_levels(s, args.date_from+' 00:00:00Z', args.date_to+' 23:59:59Z', or_window=args.or_window, tz=args.tz, cfg=cfg)
        if df.empty:
            print(f"[yellow]Sin niveles para {s}[/yellow]"); continue
        p = write_year_levels(df, s, cfg)
        print(f"[green]OK[/green] {s} niveles → {p}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
