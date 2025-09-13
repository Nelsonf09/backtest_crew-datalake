import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
from ib_insync import IB, Contract

from datalake.config import LakeConfig
from datalake.ingestors.ibkr.writer import write_month

# --- Helpers de contrato, chunking y fetch robusto (2h) ---
CHUNK_HOURS = 8
BACKFILL_SLICE_HOURS = 2

BAR_SIZES = {
    "M1": "1 min",
    "M5": "5 mins",
    "M15": "15 mins",
    "H1": "1 hour",
    "D1": "1 day",
}

BAR_SIZE_SECONDS = {
    "1 min": 60,
    "5 mins": 5 * 60,
    "15 mins": 15 * 60,
    "1 hour": 60 * 60,
    "1 day": 24 * 60 * 60,
}

RESAMPLE_FREQ = {
    "M1": "1min",
    "M5": "5min",
    "M15": "15min",
    "H1": "1H",
    "D1": "1D",
}


logger = logging.getLogger("ibkr.ingest")


def _crypto_contract(symbol: str, exchange: str = "PAXOS") -> Contract:
    base, quote = symbol.split("-")
    return Contract(secType="CRYPTO", symbol=base, currency=quote, exchange=exchange)


def _day_chunks_utc(day: datetime, hours: int = CHUNK_HOURS):
    start = day.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end = start + timedelta(days=1) - timedelta(minutes=1)  # 23:59 inclusivo
    chunks = []
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(hours=hours) - timedelta(minutes=1), end)
        chunks.append((cur, nxt))
        cur = nxt + timedelta(minutes=1)
    logging.info(
        "IBKR chunks UTC: %s",
        [(a.strftime("%H:%M"), b.strftime("%H:%M")) for a, b in chunks],
    )
    return chunks


def _fetch_window(
    ib: IB,
    contract: Contract,
    start_utc: datetime,
    end_utc: datetime,
    what_to_show: str,
    timeframe: str,
    use_rth: bool,
    single_day_fast: bool = False,
) -> pd.DataFrame:
    bar_size = BAR_SIZES.get(timeframe, "1 min")
    end_inclusive = end_utc + timedelta(minutes=1)
    seconds = int((end_inclusive - start_utc).total_seconds())
    duration_str = (
        "1 D" if single_day_fast and timeframe == "M1" else f"{seconds} S"
    )
    end_str = end_inclusive.strftime("%Y%m%d %H:%M:%S UTC")
    logger.debug(
        "reqHistoricalData duration=%s barSize=%s end=%s",
        duration_str,
        bar_size,
        end_str,
    )
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_str,
        durationStr=duration_str,
        barSizeSetting=bar_size,
        whatToShow=what_to_show,
        useRTH=int(use_rth),
        formatDate=2,
        keepUpToDate=False,
    )
    if not bars:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(b.__dict__ for b in bars)[
        ["date", "open", "high", "low", "close", "volume"]
    ]
    df["ts"] = pd.to_datetime(df["date"], utc=True)
    df = df.drop(columns=["date"]).sort_values("ts")
    df = df[(df["ts"] >= start_utc) & (df["ts"] <= end_utc)]
    return df


def _resample(pdf: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    freq = RESAMPLE_FREQ.get(timeframe, "1min")
    if timeframe == "M1":
        return pdf
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    out = (
        pdf.set_index("ts")
        .resample(freq)
        .agg(agg)
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    return out


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", required=True, help="Lista separada por comas. Ej: BTC-USD,ETH-USD")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--tf", choices=list(BAR_SIZES.keys()), default="M1")
    ap.add_argument(
        "--exchange",
        default=os.getenv("IB_EXCHANGE_CRYPTO", "PAXOS"),
        help="Exchange del contrato CRYPTO",
    )
    ap.add_argument(
        "--what-to-show",
        dest="what",
        default=os.getenv("IB_WHAT_TO_SHOW", "AGGTRADES"),
        help="Tipo de datos HMDS",
    )
    ap.add_argument("--rth", action="store_true", help="Usar Regular Trading Hours")
    return ap


def ingest(args, data_root: str | None = None) -> List[str]:
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    d0 = datetime.fromisoformat(args.date_from).replace(tzinfo=timezone.utc)
    d1 = datetime.fromisoformat(args.date_to).replace(tzinfo=timezone.utc)
    tf = args.tf
    exchange = args.exchange
    what = args.what
    rth = bool(args.rth)
    same_day = d0.date() == d1.date()

    lake_root = data_root or os.getenv("LAKE_ROOT", os.getcwd())

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

    written: List[str] = []
    for sym in symbols:
        logger.info("start %s %s→%s", sym, d0.date(), d1.date())
        cur = d0
        while cur <= d1:
            logger.info("day %s %s", sym, cur.date())
            if synth:
                day_df = pd.DataFrame(
                    {
                        "ts": [cur],
                        "open": [1.0],
                        "high": [1.0],
                        "low": [1.0],
                        "close": [1.0],
                        "volume": [1.0],
                    }
                )
            else:
                all_dfs: List[pd.DataFrame] = []
                fast = (
                    os.getenv("DATALAKE_IBKR_SINGLE_DAY_FASTPATH", "0") == "1"
                    and same_day
                    and tf == "M1"
                )
                if fast:
                    cont = _crypto_contract(sym, exchange=exchange)
                    dfw = _fetch_window(
                        ib,
                        cont,
                        cur,
                        cur + timedelta(days=1) - timedelta(minutes=1),
                        what_to_show=what,
                        timeframe=tf,
                        use_rth=rth,
                        single_day_fast=True,
                    )
                    if not dfw.empty:
                        all_dfs.append(dfw)
                else:
                    for start_utc, end_utc in _day_chunks_utc(cur, CHUNK_HOURS):
                        cont = _crypto_contract(sym, exchange=exchange)
                        dfw = _fetch_window(
                            ib,
                            cont,
                            start_utc,
                            end_utc,
                            what_to_show=what,
                            timeframe=tf,
                            use_rth=rth,
                        )
                        if not dfw.empty:
                            all_dfs.append(dfw)
                if not all_dfs:
                    logger.warning("no bars %s %s", sym, cur.date())
                    cur = (cur + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    continue
                day_df = (
                    pd.concat(all_dfs, ignore_index=True)
                    .drop_duplicates(subset=["ts"], keep="last")
                    .sort_values("ts")
                )

            day_df = _resample(day_df, tf)
            day_df["source"] = "ibkr"
            day_df["market"] = "crypto"
            day_df["timeframe"] = tf
            day_df["symbol"] = sym
            day_df["exchange"] = exchange
            day_df["what_to_show"] = what
            day_df["vendor"] = "ibkr"
            day_df["tz"] = "UTC"
            path = write_month(day_df, symbol=sym, cfg=cfg)
            written.append(path)
            logger.info("end %s %s -> %s", sym, cur.date(), path)
            cur = (cur + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        logger.info("done %s", sym)

    if ib is not None:
        ib.disconnect()
    return written


def main(argv: List[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    ingest(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

