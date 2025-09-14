import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
from ib_insync import IB

from datalake.config import LakeConfig
from datalake.ingestors.ibkr.ingest_cli import (
    BAR_SIZES,
    _find_missing_ranges_utc,
    _hourly_fetch,
)
from datalake.ingestors.ibkr.writer import write_month


logger = logging.getLogger("ibkr.repair_day")


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--tf", choices=list(BAR_SIZES.keys()), default="M1")
    ap.add_argument("--exchange", default=os.getenv("IB_EXCHANGE_CRYPTO", "PAXOS"))
    ap.add_argument(
        "--what-to-show",
        dest="what",
        default=os.getenv("IB_WHAT_TO_SHOW", "AGGTRADES"),
    )
    ap.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", os.getcwd()))
    return ap


def repair_day(args) -> str:
    date_utc = datetime.fromisoformat(args.date).replace(tzinfo=timezone.utc)
    tf = args.tf
    exchange = args.exchange
    what = args.what
    lake_root = args.lake_root

    cfg = LakeConfig()
    cfg.data_root = lake_root
    cfg.market = "crypto"
    cfg.timeframe = tf
    cfg.source = "ibkr"
    cfg.vendor = "ibkr"
    cfg.exchange = exchange
    cfg.what_to_show = what
    cfg.tz = "UTC"

    synth = os.getenv("DATALAKE_SYNTH") == "1"
    ib = None
    if not synth:
        host = os.getenv("IB_HOST", "127.0.0.1")
        port = int(os.getenv("IB_PORT", "7497"))
        client_id = int(os.getenv("IB_CLIENT_ID", "1"))
        ib = IB()
        ib.connect(host, port, clientId=client_id, timeout=15)

    year = date_utc.year
    month = date_utc.month
    day_start = date_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1) - timedelta(minutes=1)

    base = os.path.join(
        lake_root,
        "data",
        "source=ibkr",
        f"market={cfg.market}",
        f"timeframe={cfg.timeframe}",
        f"symbol={args.symbol}",
        f"year={year}",
        f"month={month:02d}",
    )
    part_file = os.path.join(base, f"part-{year}-{month:02d}.parquet")
    if os.path.exists(part_file):
        df_month = pd.read_parquet(part_file)
        df_month["ts"] = pd.to_datetime(df_month["ts"], utc=True)
        df_day = df_month[
            (df_month["ts"] >= day_start) & (df_month["ts"] <= day_end)
        ].copy()
    else:
        df_day = pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

    what_final = what
    if cfg.market == "crypto" and what_final.upper() == "TRADES":
        logger.warning("whatToShow TRADES incompatible with crypto; forcing AGGTRADES")
        what_final = "AGGTRADES"

    if len(df_day) == 1440 and tf == "M1":
        print("already complete")
        if ib is not None:
            ib.disconnect()
        return part_file

    if df_day.empty:
        missing_ranges: List[tuple[datetime, datetime]] = [(day_start, day_end)]
    else:
        missing_ranges = _find_missing_ranges_utc(df_day)

    new_dfs: List[pd.DataFrame] = []
    for start, end in missing_ranges:
        if synth:
            new_dfs.append(
                pd.DataFrame(
                    {
                        "ts": [start],
                        "open": [1.0],
                        "high": [1.0],
                        "low": [1.0],
                        "close": [1.0],
                        "volume": [1.0],
                    }
                )
            )
        else:
            dfh = _hourly_fetch(
                ib, args.symbol, start, end, cfg, tf, what_final, exchange, False
            )
            if not dfh.empty:
                new_dfs.append(dfh)

    if ib is not None:
        ib.disconnect()

    if new_dfs:
        df_new = pd.concat([df_day] + new_dfs, ignore_index=True)
    else:
        df_new = df_day

    df_new = df_new.drop_duplicates(subset=["ts"]).sort_values("ts")
    if len(df_new) == 1440 and tf == "M1":
        print("day healed")
    else:
        if tf == "M1":
            remaining = _find_missing_ranges_utc(df_new)
            print("remaining gaps:", [(s.isoformat(), e.isoformat()) for s, e in remaining])
    path = write_month(df_new, symbol=args.symbol, cfg=cfg)
    return path


def main(argv: List[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    repair_day(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

