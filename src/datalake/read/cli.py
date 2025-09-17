import argparse, os
from .api import read_range_df, join_mtf_exec_ctx

def _cmd_read(a):
    df = read_range_df(a.lake_root, market=a.market, tf=a.tf, symbol=a.symbol, date_from=a.date_from, date_to=a.date_to, source=a.source)
    if a.head:
        print(df.head(a.head))
    if a.out_csv:
        df.to_csv(a.out_csv, index=False)

def _cmd_join(a):
    ctx = [t.strip() for t in a.ctx_tf.split(',') if t.strip()]
    df = join_mtf_exec_ctx(a.lake_root, symbol=a.symbol, market=a.market, exec_tf=a.exec_tf, ctx_tfs=ctx, date_from=a.date_from, date_to=a.date_to, source=a.source, suffix_close_only=True)
    if a.head:
        print(df.head(a.head))
    if a.out_csv:
        df.to_csv(a.out_csv, index=False)

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    r = sub.add_parser('read')
    r.add_argument('--lake-root', required=True)
    r.add_argument('--market', required=True)
    r.add_argument('--tf', required=True)
    r.add_argument('--symbol', required=True)
    r.add_argument('--date-from', required=True)
    r.add_argument('--date-to', required=True)
    r.add_argument('--source', default='ibkr')
    r.add_argument('--head', type=int, default=0)
    r.add_argument('--out-csv')
    r.set_defaults(func=_cmd_read)

    j = sub.add_parser('join-mtf')
    j.add_argument('--lake-root', required=True)
    j.add_argument('--market', default='crypto')
    j.add_argument('--symbol', required=True)
    j.add_argument('--exec-tf', required=True)
    j.add_argument('--ctx-tf', required=True, help='coma-separado, ej: M5,M15,H1')
    j.add_argument('--date-from', required=True)
    j.add_argument('--date-to', required=True)
    j.add_argument('--source', default='ibkr')
    j.add_argument('--head', type=int, default=0)
    j.add_argument('--out-csv')
    j.set_defaults(func=_cmd_join)

    a = p.parse_args()
    a.func(a)

if __name__ == '__main__':
    main()
