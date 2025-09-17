import argparse, os
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
from datalake.ingestors.ibkr.writer import write_month


def make_m1(symbol: str, day_from: str, day_to: str, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    d0 = datetime.fromisoformat(day_from).replace(tzinfo=timezone.utc)
    d1 = datetime.fromisoformat(day_to).replace(tzinfo=timezone.utc)

    day = d0
    frames = []
    price0 = 100_000.0
    while day <= d1:
        idx = pd.date_range(day.replace(hour=0, minute=0, second=0, microsecond=0),
                            day.replace(hour=23, minute=59, second=0, microsecond=0),
                            freq="1min", tz="UTC")
        steps = rng.normal(0, 10, len(idx))
        px = price0 + steps.cumsum()
        high = px + rng.uniform(0, 5, len(idx))
        low  = px - rng.uniform(0, 5, len(idx))
        open_ = px
        close = px + rng.normal(0, 2, len(idx))
        vol = rng.integers(0, 100, len(idx))

        df = pd.DataFrame({
            "ts": idx,
            "open": open_, "high": high, "low": low, "close": close,
            "volume": vol
        })
        frames.append(df)
        price0 = float(close[-1])
        day += timedelta(days=1)

    out = pd.concat(frames, ignore_index=True)
    out["source"]="ibkr"; out["market"]="crypto"; out["timeframe"]="M1"; out["symbol"]=symbol
    out["exchange"]=os.getenv("IB_EXCHANGE_CRYPTO","PAXOS")
    out["what_to_show"]=os.getenv("IB_WHAT_TO_SHOW","AGGTRADES")
    out["vendor"]="ibkr"; out["tz"]="UTC"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, help="BTC-USD, ETH-USD, etc.")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--to",   dest="date_to",   required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    df = make_m1(args.symbol, args.date_from, args.date_to, seed=args.seed)

    cfg = SimpleNamespace(
        data_root=os.getenv("LAKE_ROOT", os.getcwd()),
        market="crypto", timeframe="M1",
        source="ibkr", vendor="ibkr",
        exchange=os.getenv("IB_EXCHANGE_CRYPTO","PAXOS"),
        what_to_show=os.getenv("IB_WHAT_TO_SHOW","AGGTRADES"),
        tz="UTC",
    )

    path = write_month(df, symbol=args.symbol, cfg=cfg)
    print("M1 sintético escrito en:", path, "| rows:", len(df))


if __name__ == "__main__":
    raise SystemExit(main())
