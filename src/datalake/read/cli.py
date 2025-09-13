import argparse, os, sys
import pandas as pd
from .reader import read_range
from .mtf import load_and_align


def main():
    ap = argparse.ArgumentParser(prog="datalake-read")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("read", help="Lee un rango [from,to] por símbolo y TF")
    r.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", os.getcwd()))
    r.add_argument("--market", default="crypto")
    r.add_argument("--tf", required=True)
    r.add_argument("--symbol", required=True)
    r.add_argument("--from", dest="date_from", required=True)
    r.add_argument("--to", dest="date_to", required=True)
    r.add_argument("--head", type=int, default=0)
    r.add_argument("--out-csv", default="")

    j = sub.add_parser("join-mtf", help="Join asof entre TF de ejecución y contextos")
    j.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", os.getcwd()))
    j.add_argument("--symbol", required=True)
    j.add_argument("--exec-tf", required=True)
    j.add_argument("--from", dest="date_from", required=True)
    j.add_argument("--to", dest="date_to", required=True)
    j.add_argument("--ctx-tf", default="M5,M15,H1")
    j.add_argument("--out-csv", default="")

    args = ap.parse_args()

    if args.cmd == "read":
        df = read_range(args.lake_root, args.market, args.tf, args.symbol, args.date_from, args.date_to)
        if args.head:
            print(df.head(args.head))
        else:
            print(df.shape)
        if args.out_csv:
            df.to_csv(args.out_csv, index=False)
            print("CSV escrito en:", args.out_csv)
        return 0

    if args.cmd == "join-mtf":
        ctx_list = [t.strip() for t in args.ctx_tf.split(",") if t.strip()]
        _, _, joined = load_and_align(args.lake_root, args.symbol, args.exec_tf, args.date_from, args.date_to, ctx_list)
        print(joined.shape)
        if args.out_csv:
            joined.to_csv(args.out_csv, index=False)
            print("CSV escrito en:", args.out_csv)
        return 0

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
