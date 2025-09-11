from __future__ import annotations
import argparse
from rich import print
from datalake.config import LakeConfig
from bridge.backtest_crew.provider import LakeProvider


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='Smoke test: proveedor offline para backtest_crew')
    ap.add_argument('--symbol', required=True)
    ap.add_argument('--from', dest='date_from', required=True)
    ap.add_argument('--to', dest='date_to', required=True)
    ap.add_argument('--exec-tf', default='1 min')
    ap.add_argument('--filter-tf', default='5 mins')
    args = ap.parse_args(argv)

    prov = LakeProvider(LakeConfig())
    de, df = prov.load_exec_and_filter(args.symbol, args.date_from+' 00:00:00Z', args.date_to+' 23:59:59Z', args.exec_tf, args.filter_tf)
    print(f"[bold]EXEC[/bold] rows={len(de)} ts=[{de.ts.min() if not de.empty else None} .. {de.ts.max() if not de.empty else None}]")
    print(f"[bold]FILTER[/bold] rows={len(df)} ts=[{df.ts.min() if not df.empty else None} .. {df.ts.max() if not df.empty else None}]")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
