import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
from ib_insync import IB, Contract

from datalake.config import LakeConfig
from datalake.ingestors.ibkr.writer import write_month
from datalake.ingestors.ibkr.downloader import download_window

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


def _day_chunks_exact_utc(day_utc: datetime) -> List[tuple[datetime, datetime]]:
    """Return three fixed UTC chunks covering the day without gaps."""
    start = day_utc.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    chunks: List[tuple[datetime, datetime]] = []
    cur = start
    for _ in range(3):
        end = cur + timedelta(hours=CHUNK_HOURS) - timedelta(minutes=1)
        chunks.append((cur, end))
        cur = end + timedelta(minutes=1)
    return chunks


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
                    end_fast = cur + timedelta(days=1) - timedelta(minutes=1)
                    end_inclusive = end_fast + timedelta(minutes=1)
                    seconds = int((end_inclusive - cur).total_seconds())
                    duration_str = "1 D" if tf == "M1" else f"{seconds} S"
                    end_str = end_inclusive.astimezone(timezone.utc).strftime(
                        "%Y%m%d %H:%M:%S UTC"
                    )
                    what_req = what
                    if cfg.market == "crypto" and what_req.upper() == "TRADES":
                        logger.warning(
                            "whatToShow TRADES incompatible with crypto; forcing AGGTRADES"
                        )
                        what_req = "AGGTRADES"
                    logger.info(
                        "REQ [1/1] sym=%s start=%s endDateTime=%s duration=%s rth=%s what=%s exch=%s",
                        sym,
                        cur.isoformat(),
                        end_str,
                        duration_str,
                        rth,
                        what_req,
                        exchange,
                    )
                    dfw = download_window(
                        ib,
                        cont,
                        end_date_time=end_str,
                        duration_str=duration_str,
                        bar_size=BAR_SIZES.get(tf, "1 min"),
                        what_to_show=what_req,
                        use_rth=rth,
                    )
                    if not dfw.empty:
                        dfw = dfw[(dfw["ts"] >= cur) & (dfw["ts"] <= end_fast)]
                        all_dfs.append(dfw)
                    what_final = what_req
                else:
                    what_final = what
                    if cfg.market == "crypto" and what_final.upper() == "TRADES":
                        logger.warning(
                            "whatToShow TRADES incompatible with crypto; forcing AGGTRADES"
                        )
                        what_final = "AGGTRADES"
                    for i, (start_utc, end_utc) in enumerate(
                        _day_chunks_exact_utc(cur), start=1
                    ):
                        cont = _crypto_contract(sym, exchange=exchange)
                        end_inclusive = end_utc + timedelta(minutes=1)
                        seconds = int((end_inclusive - start_utc).total_seconds())
                        duration_str = f"{seconds} S"
                        end_str = end_inclusive.astimezone(timezone.utc).strftime(
                            "%Y%m%d %H:%M:%S UTC"
                        )
                        logger.info(
                            "REQ [%s/3] sym=%s start=%s endDateTime=%s duration=%s rth=%s what=%s exch=%s",
                            i,
                            sym,
                            start_utc.isoformat(),
                            end_str,
                            duration_str,
                            rth,
                            what_final,
                            exchange,
                        )
                        dfw = download_window(
                            ib,
                            cont,
                            end_date_time=end_str,
                            duration_str=duration_str,
                            bar_size=BAR_SIZES.get(tf, "1 min"),
                            what_to_show=what_final,
                            use_rth=rth,
                        )
                        if not dfw.empty:
                            dfw = dfw[
                                (dfw["ts"] >= start_utc) & (dfw["ts"] <= end_utc)
                            ]
                            all_dfs.append(dfw)
                if not all_dfs:
                    logger.warning("no bars %s %s", sym, cur.date())
                    cur = (cur + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    continue
                day_df = (
                    pd.concat(all_dfs, ignore_index=True)
                    .drop_duplicates(subset=["ts"], keep="first")
                    .sort_values("ts")
                )
                if day_df["ts"].dt.tz is None:
                    day_df["ts"] = day_df["ts"].dt.tz_localize("UTC")
                else:
                    day_df["ts"] = day_df["ts"].dt.tz_convert("UTC")
                logger.info(
                    "summary rows=%d range=%s→%s",
                    len(day_df),
                    day_df["ts"].min(),
                    day_df["ts"].max(),
                )

            day_df = _resample(day_df, tf)
            day_df["source"] = "ibkr"
            day_df["market"] = "crypto"
            day_df["timeframe"] = tf
            day_df["symbol"] = sym
            day_df["exchange"] = exchange
            day_df["what_to_show"] = what_final if not synth else what
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

