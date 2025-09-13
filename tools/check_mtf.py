import argparse, glob, os, pandas as pd

EXPECTED = {"M1":1440, "M5":288, "M15":96, "H1":24}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", os.getcwd()))
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--tf", required=True)
    args = ap.parse_args()

    base = os.path.join(args.lake_root, "data", "source=ibkr", "market=crypto", f"timeframe={args.tf}", f"symbol={args.symbol}")
    yy, mm = args.date[:4], args.date[5:7]
    patt = os.path.join(base, f"year={yy}", f"month={mm}", "*.parquet")
    files = sorted(glob.glob(patt))
    if not files:
        print("No hay archivos para", args.tf, patt)
        return 1

    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    start = pd.Timestamp(args.date + " 00:00:00+00:00")
    end   = pd.Timestamp(args.date + " 23:59:00+00:00")
    d = df[(df["ts"] >= start) & (df["ts"] <= end)].sort_values("ts").copy()

    exp = EXPECTED.get(args.tf.upper())
    print("tf:", args.tf, "| rows:", len(d), "| range:", d["ts"].min(), "->", d["ts"].max())
    if exp:
        print("expected_rows:", exp, "| ok:", len(d) == exp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
