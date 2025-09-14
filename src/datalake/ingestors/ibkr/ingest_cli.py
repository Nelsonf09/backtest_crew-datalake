import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
from ib_insync import IB, Contract

from datalake.config import LakeConfig
from datalake.ingestors.ibkr.writer import write_month
from datalake.ingestors.ibkr.downloader import download_window, fetch_hist_bars

# --- Helpers de contrato, chunking y fetch robusto (2h) ---
CHUNK_HOURS = 8

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


def _find_missing_ranges_utc(df_day: pd.DataFrame) -> List[tuple[datetime, datetime]]:
    """Given a day's dataframe, return missing minute ranges in UTC."""
    if df_day.empty:
        return []
    day_start = df_day["ts"].dt.floor("D").min().replace(tzinfo=timezone.utc)
    full = pd.date_range(
        day_start,
        day_start + timedelta(days=1) - timedelta(minutes=1),
        freq="1min",
        tz=timezone.utc,
    )
    missing = full.difference(pd.DatetimeIndex(df_day["ts"]))
    if missing.empty:
        return []
    ranges: List[tuple[datetime, datetime]] = []
    start = missing[0]
    prev = start
    for ts in missing[1:]:
        if ts - prev == timedelta(minutes=1):
            prev = ts
        else:
            ranges.append((start.to_pydatetime(), prev.to_pydatetime()))
            start = ts
            prev = ts
    ranges.append((start.to_pydatetime(), prev.to_pydatetime()))
    return ranges


def _synth_fill(df_day: pd.DataFrame, day_start: datetime) -> pd.DataFrame:
    """Fill missing minute bars with flat synthetic data."""
    full = pd.date_range(
        day_start,
        day_start + timedelta(days=1) - timedelta(minutes=1),
        freq="1min",
        tz=timezone.utc,
    )
    existing = pd.DatetimeIndex(df_day["ts"])
    missing = full.difference(existing)
    if missing.empty:
        return df_day
    synth_rows = []
    for ts in missing:
        prev = df_day[df_day["ts"] < ts]
        nxt = df_day[df_day["ts"] > ts]
        if not prev.empty:
            price = prev.iloc[-1]["close"]
        elif not nxt.empty:
            price = nxt.iloc[0]["open"]
        else:
            price = 0.0
        synth_rows.append(
            {
                "ts": ts,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0,
                "is_synth": True,
            }
        )
    df_synth = pd.DataFrame(synth_rows)
    df_day = pd.concat([df_day, df_synth], ignore_index=True)
    if "is_synth" not in df_day.columns:
        df_day["is_synth"] = df_day.get("is_synth", False)
    df_day["is_synth"] = df_day["is_synth"].fillna(False)
    return df_day.sort_values("ts")


def _hourly_fetch(
    ib: IB,
    symbol: str,
    start_utc: datetime,
    end_utc: datetime,
    cfg: LakeConfig,
    tf: str,
    what: str,
    exchange: str,
    rth: bool,
) -> pd.DataFrame:
    """Fetch missing data in 1h windows covering [start_utc, end_utc]."""
    cont = _crypto_contract(symbol, exchange=exchange)
    cur = start_utc.replace(minute=0, second=0, microsecond=0)
    end_hour = end_utc.replace(minute=0, second=0, microsecond=0)
    dfs: List[pd.DataFrame] = []
    while cur <= end_hour:
        end_inclusive = cur + timedelta(hours=1)
        end_str = end_inclusive.strftime("%Y%m%d %H:%M:%S UTC")
        duration_str = "3600 S"
        logger.debug(
            "REQ[H] sym=%s endDateTime=%s duration=%s", symbol, end_str, duration_str
        )
        dfh = download_window(
            ib,
            cont,
            end_date_time=end_str,
            duration_str=duration_str,
            bar_size=BAR_SIZES.get(tf, "1 min"),
            what_to_show=what,
            use_rth=rth,
        )
        if not dfh.empty:
            dfh = dfh[(dfh["ts"] >= cur) & (dfh["ts"] < end_inclusive)]
            dfs.append(dfh)
        cur = end_inclusive
    if not dfs:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    out = pd.concat(dfs, ignore_index=True).drop_duplicates("ts").sort_values("ts")
    if out["ts"].dt.tz is None:
        out["ts"] = out["ts"].dt.tz_localize("UTC")
    else:
        out["ts"] = out["ts"].dt.tz_convert("UTC")
    return out[(out["ts"] >= start_utc) & (out["ts"] <= end_utc)]


def _repair_range_with_fallback(
    symbol: str,
    start: datetime,
    end: datetime,
    params: dict,
) -> pd.DataFrame:
    """Attempt to repair a missing range using decreasing window sizes."""
    ib: IB = params["ib"]
    exchange: str = params["exchange"]
    what: str = params["what"]
    rth: bool = params["rth"]
    cont = _crypto_contract(symbol, exchange=exchange)
    remaining: List[tuple[datetime, datetime]] = [(start, end)]
    out = pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    for step in [3600, 1800, 600, 300]:
        new_remaining: List[tuple[datetime, datetime]] = []
        for rs, re in remaining:
            cur = rs
            while cur <= re:
                block_end = min(cur + timedelta(seconds=step) - timedelta(minutes=1), re)
                end_incl = block_end + timedelta(minutes=1)
                duration = int((end_incl - cur).total_seconds())
                df = fetch_hist_bars(
                    ib,
                    cont,
                    end_incl,
                    duration,
                    bar_size="1 min",
                    what=what,
                    rth=rth,
                )
                if not df.empty:
                    df = df[(df["ts"] >= cur) & (df["ts"] <= block_end)]
                    out = pd.concat([out, df], ignore_index=True)
                else:
                    new_remaining.append((cur, block_end))
                cur = block_end + timedelta(minutes=1)
        if not new_remaining:
            break
        remaining = new_remaining
    if out.empty:
        return out
    return out.drop_duplicates("ts").sort_values("ts")


def _fetch_with_fallback(
    ib: IB,
    symbol: str,
    start: datetime,
    end: datetime,
    cfg: LakeConfig,
    tf: str,
    what: str,
    exchange: str,
    rth: bool,
) -> pd.DataFrame:
    cont = _crypto_contract(symbol, exchange=exchange)
    end_inclusive = end + timedelta(minutes=1)
    duration = int((end_inclusive - start).total_seconds())
    end_str = end_inclusive.strftime("%Y%m%d %H:%M:%S UTC")
    duration_str = f"{duration} S"
    logger.info(
        "REQ[A] sym=%s start=%s endDateTime=%s duration=%s rth=%s what=%s exch=%s",
        symbol,
        start.isoformat(),
        end_str,
        duration_str,
        rth,
        what,
        exchange,
    )
    df = download_window(
        ib,
        cont,
        end_date_time=end_str,
        duration_str=duration_str,
        bar_size=BAR_SIZES.get(tf, "1 min"),
        what_to_show=what,
        use_rth=rth,
    )
    if df.empty:
        end_str_b = end.strftime("%Y%m%d 23:59:59 UTC")
        duration_str_b = "28800 S"
        logger.info(
            "REQ[B] sym=%s start=%s endDateTime=%s duration=%s rth=%s what=%s exch=%s",
            symbol,
            start.isoformat(),
            end_str_b,
            duration_str_b,
            rth,
            what,
            exchange,
        )
        df = download_window(
            ib,
            cont,
            end_date_time=end_str_b,
            duration_str=duration_str_b,
            bar_size=BAR_SIZES.get(tf, "1 min"),
            what_to_show=what,
            use_rth=rth,
        )
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
    ap.add_argument(
        "--allow-synth",
        action="store_true",
        help="Rellenar huecos con barras sintéticas si quedan faltantes",
    )
    return ap


def ingest(args, data_root: str | None = None) -> List[str]:
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    d0 = datetime.fromisoformat(args.date_from).replace(tzinfo=timezone.utc)
    d1 = datetime.fromisoformat(args.date_to).replace(tzinfo=timezone.utc)
    tf = args.tf
    exchange = args.exchange
    what = args.what
    rth = bool(args.rth)
    allow_synth = bool(getattr(args, "allow_synth", False)) or os.getenv(
        "ALLOW_SYNTH_FILL"
    ) == "1"

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
                what_final = what
                if cfg.market == "crypto" and what_final.upper() == "TRADES":
                    logger.warning(
                        "whatToShow TRADES incompatible with crypto; forcing AGGTRADES"
                    )
                    what_final = "AGGTRADES"
                for start_utc, end_utc in _day_chunks_exact_utc(cur):
                    dfw = _fetch_with_fallback(
                        ib,
                        sym,
                        start_utc,
                        end_utc,
                        cfg,
                        tf,
                        what_final,
                        exchange,
                        rth,
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
                missing_ranges: List[tuple[datetime, datetime]] = []
                if tf == "M1" and len(day_df) != 1440:
                    missing_ranges = _find_missing_ranges_utc(day_df)
                    common = {
                        "ib": ib,
                        "exchange": exchange,
                        "what": what_final,
                        "rth": rth,
                    }
                    for start_m, end_m in missing_ranges:
                        df_fix = _repair_range_with_fallback(
                            sym, start_m, end_m, common
                        )
                        if not df_fix.empty:
                            day_df = pd.concat([day_df, df_fix], ignore_index=True)
                    day_df = day_df.drop_duplicates(subset=["ts"], keep="first").sort_values("ts")
                    if tf == "M1" and len(day_df) != 1440 and allow_synth:
                        day_df = _synth_fill(day_df, cur)
                    day_df = day_df.drop_duplicates(subset=["ts"], keep="first").sort_values("ts")
                if day_df.empty:
                    logger.warning("no bars %s %s", sym, cur.isoformat())
                    cur = (cur + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    continue
                per_hour = (
                    day_df.set_index("ts")
                    .groupby(day_df["ts"].dt.hour)
                    .size()
                    .reindex(range(24), fill_value=0)
                )
                if tf == "M1" and len(day_df) != 1440:
                    remaining = _find_missing_ranges_utc(day_df)
                    range_str = (
                        "EMPTY"
                        if day_df.empty
                        else f"{day_df['ts'].min()}→{day_df['ts'].max()}"
                    )
                    logger.warning(
                        "incomplete day rows=%d range=%s per_hour=%s missing=%s",
                        len(day_df),
                        range_str,
                        per_hour.to_dict(),
                        [(s.isoformat(), e.isoformat()) for s, e in remaining],
                    )
                else:
                    if tf == "M1" and missing_ranges:
                        logger.info("day healed")
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

