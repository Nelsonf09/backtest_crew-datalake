import argparse, glob, os
from types import SimpleNamespace
import pandas as pd
from datalake.ingestors.ibkr.writer import write_month

AGG_MAP = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}

TF_RULE = {"M5":"5min", "M15":"15min", "H1":"1h"}


def resample_df(df_m1: pd.DataFrame, rule: str) -> pd.DataFrame:
    d = df_m1.copy()
    d["ts"] = pd.to_datetime(d["ts"], utc=True)
    d = d.set_index("ts").sort_index()
    ohlcv = d[["open","high","low","close","volume"]].resample(rule, label="left", closed="left").agg(AGG_MAP)
    ohlcv = ohlcv.dropna(subset=["open","high","low","close"]).reset_index().rename(columns={"ts":"ts"})
    return ohlcv


def read_m1_range(lake_root: str, symbol: str, date_from: str, date_to: str) -> pd.DataFrame:
    base = os.path.join(lake_root, "data", "source=ibkr", "market=crypto", "timeframe=M1", f"symbol={symbol}")
    yy0, mm0 = date_from[:4], date_from[5:7]
    yy1, mm1 = date_to[:4], date_to[5:7]
    files = []
    for yy in range(int(yy0), int(yy1)+1):
        for mm in range(1,13):
            if (yy == int(yy0) and mm < int(mm0)) or (yy == int(yy1) and mm > int(mm1)):
                continue
            patt = os.path.join(base, f"year={yy}", f"month={mm:02d}", "*.parquet")
            files.extend(glob.glob(patt))
    if not files:
        raise SystemExit(f"No hay M1 para {symbol} en {base}")
    df = pd.concat((pd.read_parquet(f) for f in sorted(set(files))), ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    mask = (df["ts"] >= pd.Timestamp(date_from + " 00:00:00+00:00")) & (df["ts"] <= pd.Timestamp(date_to + " 23:59:00+00:00"))
    return df.loc[mask].copy()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", os.getcwd()))
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--from", dest="date_from", required=True)
    ap.add_argument("--to",   dest="date_to", required=True)
    ap.add_argument("--to-tf", default="M5,M15,H1", help="Lista destino: M5,M15,H1")
    args = ap.parse_args()

    df = read_m1_range(args.lake_root, args.symbol, args.date_from, args.date_to)
    if df.empty:
        raise SystemExit("No hay datos M1 en el rango solicitado.")

    targets = [t.strip().upper() for t in args.to_tf.split(",") if t.strip()]
    wrote = []
    for tf in targets:
        rule = TF_RULE.get(tf)
        if not rule:
            print("TF no soportado:", tf); continue
        out = resample_df(df, rule=rule)
        # metadatos
        out["source"]="ibkr"; out["market"]="crypto"; out["timeframe"]=tf; out["symbol"]=args.symbol
        out["exchange"]=df.get("exchange").iloc[0] if "exchange" in df.columns and len(df) else "PAXOS"
        out["what_to_show"]=df.get("what_to_show").iloc[0] if "what_to_show" in df.columns and len(df) else "AGGTRADES"
        out["vendor"]="ibkr"; out["tz"]="UTC"

        cfg = SimpleNamespace(
            data_root=args.lake_root, market="crypto", timeframe=tf,
            source="ibkr", vendor="ibkr",
            exchange=out["exchange"].iloc[0], what_to_show=out["what_to_show"].iloc[0], tz="UTC",
        )
        p = write_month(out, symbol=args.symbol, cfg=cfg)
        wrote.append((tf, len(out), p))

    for tf, n, p in wrote:
        print(f"OK {tf}: {n} filas → {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
