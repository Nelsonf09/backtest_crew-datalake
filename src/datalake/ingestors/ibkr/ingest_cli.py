import argparse
import logging
import os
import re
import time
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

UTC = timezone.utc


def _dt(date_str: str) -> datetime:
    """'YYYY-MM-DD' -> datetime UTC (00:00:00)"""
    y, m, d = map(int, date_str.split("-"))
    return datetime(y, m, d, 0, 0, 0, tzinfo=UTC)


def _clip_df_to(df: pd.DataFrame | None, start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    return df[(df["ts"] >= start_ts) & (df["ts"] <= end_ts)]


def _concat_non_empty(out: pd.DataFrame | None, df: pd.DataFrame | None) -> pd.DataFrame:
    """Concatenate ``df`` into ``out`` skipping empties to silence pandas warnings."""
    if df is None or df.empty:
        return out if out is not None else pd.DataFrame()
    if out is None or out.empty:
        return df.copy()
    return pd.concat([out, df], ignore_index=True)


def to_dataframe(bars) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(b.__dict__ for b in bars)[
        ["date", "open", "high", "low", "close", "volume"]
    ]
    df["ts"] = pd.to_datetime(df["date"], utc=True)
    return df.drop(columns=["date"]).sort_values("ts")


def _req_historical_with_retry(
    ib,
    contract,
    *,
    end_dt,
    duration,
    bar_size,
    what_to_show,
    use_rth,
    fmt_date: int = 2,
):
    try:
        return ib.reqHistoricalData(
            contract,
            endDateTime=end_dt,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=fmt_date,
            keepUpToDate=False,
        )
    except Exception as e:
        msg = str(e)
        needs_agg = ("10299" in msg) and ("AGGTRADES" in msg.upper())
        if needs_agg and what_to_show.upper() != "AGGTRADES":
            logging.warning(
                "IB exige AGGTRADES (10299). Reintentando con whatToShow=AGGTRADES."
            )
            return ib.reqHistoricalData(
                contract,
                endDateTime=end_dt,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="AGGTRADES",
                useRTH=use_rth,
                formatDate=fmt_date,
                keepUpToDate=False,
            )
        raise


def _fetch_tail_cross_midnight(ib, contract, day_str: str, what_to_show: str, use_rth: int):
    """Fetch final chunk crossing midnight and clip to 20:00-23:59 UTC of day_str."""
    D0 = _dt(day_str)
    end_req = (D0 + timedelta(days=1)).replace(hour=3, minute=59, second=59)
    duration = "28800 S"  # 8 hours
    bars = _req_historical_with_retry(
        ib,
        contract,
        end_dt=end_req.strftime("%Y%m%d %H:%M:%S UTC"),
        duration=duration,
        bar_size="1 min",
        what_to_show=what_to_show,
        use_rth=use_rth,
    )
    df = to_dataframe(bars)
    start_clip = D0.replace(hour=20, minute=0, second=0)
    end_clip = D0.replace(hour=23, minute=59, second=0)
    df = _clip_df_to(df, start_clip, end_clip)
    logging.debug("tail cross-midnight fetched %d rows", 0 if df is None else len(df))
    return df


def _repair_tail_if_missing(
    ib,
    contract,
    day_str: str,
    what_to_show: str,
    use_rth: int,
) -> pd.DataFrame:
    """Try to recover missing 20:00-23:59 chunk by fetching crossing midnight."""
    D0 = _dt(day_str)
    end_req = (D0 + timedelta(days=1)).replace(hour=1, minute=10, second=0)
    duration = "18000 S"  # 5 hours
    bars = _req_historical_with_retry(
        ib,
        contract,
        end_dt=end_req.strftime("%Y%m%d %H:%M:%S UTC"),
        duration=duration,
        bar_size="1 min",
        what_to_show=what_to_show,
        use_rth=use_rth,
    )
    df = to_dataframe(bars)
    start_clip = D0.replace(hour=20, minute=0, second=0)
    end_clip = D0.replace(hour=23, minute=59, second=0)
    df = _clip_df_to(df, start_clip, end_clip)
    logging.debug("tail-repair fetched %d rows", 0 if df is None else len(df))
    return df


def _is_crypto(symbol: str, exchange: str | None) -> bool:
    ex = (exchange or "").upper()
    if ex == "PAXOS":
        return True
    return "-" in (symbol or "")


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


def _end_of_day_utc(date_str: str) -> str:
    y, m, d = date_str.split("-")
    return f"{y}{m}{d} 23:59:59 UTC"


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
    df_day = _concat_non_empty(df_day, df_synth)
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
        bar_sz = BAR_SIZES.get(tf, "1 min")
        logger.info(
            "REQ[H] sym=%s exch=%s what=%s useRTH=%s bar=%s end=%s dur=%s",
            symbol,
            exchange,
            what,
            rth,
            bar_sz,
            end_str,
            duration_str,
        )
        dfh = download_window(
            ib,
            cont,
            end_date_time=end_str,
            duration_str=duration_str,
            bar_size=bar_sz,
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
                    out = _concat_non_empty(out, df)
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
    end_str = end.replace(second=59).strftime("%Y%m%d %H:%M:%S UTC")
    duration = int((end - start).total_seconds()) + 60
    duration_str = f"{duration} S"
    attempts = [2, 5, 10]
    df = pd.DataFrame()
    for i, backoff in enumerate(attempts, start=1):
        bar_sz = BAR_SIZES.get(tf, "1 min")
        logger.info(
            "REQ[A] sym=%s exch=%s what=%s useRTH=%s bar=%s end=%s dur=%s attempt=%d",
            symbol,
            exchange,
            what,
            rth,
            bar_sz,
            end_str,
            duration_str,
            i,
        )
        df = download_window(
            ib,
            cont,
            end_date_time=end_str,
            duration_str=duration_str,
            bar_size=bar_sz,
            what_to_show=what,
            use_rth=rth,
        )
        if not df.empty and df["ts"].max() >= end:
            break
        logger.warning(
            "short chunk sym=%s last=%s expected_end=%s attempt=%d",
            symbol,
            None if df.empty else df["ts"].max(),
            end,
            i,
        )
        time.sleep(backoff)
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
        choices=["TRADES", "MIDPOINT", "BID", "ASK", "BID_ASK", "AGGTRADES"],
        help="Tipo de datos HMDS",
    )
    ap.add_argument(
        "--use-rth",
        dest="use_rth",
        choices=[0, 1],
        type=int,
        help="Usar Regular Trading Hours (0/1)",
    )
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
    allow_synth = bool(getattr(args, "allow_synth", False)) or os.getenv(
        "ALLOW_SYNTH_FILL"
    ) == "1"

    env_what = os.getenv("IB_WHAT_TO_SHOW")
    env_rth = os.getenv("IB_USE_RTH")

    symbol_first = symbols[0] if symbols else ""
    is_crypto_default = _is_crypto(symbol_first, exchange)

    user_forced_wts = args.what is not None and args.what != ""
    if is_crypto_default and not user_forced_wts:
        args.what = env_what or "AGGTRADES"

    if is_crypto_default and args.use_rth is None:
        if env_rth is not None and env_rth.strip() != "":
            args.use_rth = int(env_rth)
        else:
            args.use_rth = 0

    lake_root = data_root or os.getenv("LAKE_ROOT", os.getcwd())

    cfg = LakeConfig()
    cfg.data_root = lake_root
    cfg.market = "crypto"
    cfg.timeframe = tf
    cfg.source = "ibkr"
    cfg.vendor = "ibkr"
    cfg.exchange = exchange
    cfg.what_to_show = args.what or env_what or (
        "AGGTRADES" if is_crypto_default else "TRADES"
    )
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
            is_crypto = _is_crypto(sym, exchange)
            what_final = args.what or env_what or (
                "AGGTRADES" if is_crypto else "TRADES"
            )
            rth_final = (
                bool(args.use_rth)
                if args.use_rth is not None
                else (bool(int(env_rth)) if env_rth is not None else False)
            )
            if is_crypto and rth_final:
                logger.warning("useRTH=1 con cripto; continúa por petición del usuario")
            cfg.what_to_show = what_final
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
                contract = _crypto_contract(sym, exchange=exchange)
                all_df = pd.DataFrame()
                chunks = _day_chunks_exact_utc(cur)
                for start_utc, end_utc in chunks[:2]:
                    dfw = _fetch_with_fallback(
                        ib,
                        sym,
                        start_utc,
                        end_utc,
                        cfg,
                        tf,
                        what_final,
                        exchange,
                        rth_final,
                    )
                    if not dfw.empty:
                        dfw = dfw[
                            (dfw["ts"] >= start_utc) & (dfw["ts"] <= end_utc)
                        ]
                        if tf == "M1":
                            exp_rows = int(
                                (end_utc - start_utc).total_seconds() / 60
                            ) + 1
                            if len(dfw) < exp_rows:
                                miss = _find_missing_ranges_utc(dfw)
                                common = {
                                    "ib": ib,
                                    "exchange": exchange,
                                    "what": what_final,
                                    "rth": rth_final,
                                }
                                for s_m, e_m in miss:
                                    df_fix = _repair_range_with_fallback(
                                        sym, s_m, e_m, common
                                    )
                                    if not df_fix.empty:
                                        dfw = _concat_non_empty(dfw, df_fix)
                                dfw = dfw.drop_duplicates("ts").sort_values("ts")
                        logger.debug(
                            "chunk %s %s→%s rows=%d last=%s",
                            sym,
                            start_utc,
                            end_utc,
                            len(dfw),
                            dfw["ts"].max(),
                        )
                        all_df = _concat_non_empty(all_df, dfw)
                try:
                    df_tail = _fetch_tail_cross_midnight(
                        ib,
                        contract,
                        day_str=cur.strftime("%Y-%m-%d"),
                        what_to_show=what_final,
                        use_rth=int(rth_final),
                    )
                    all_df = _concat_non_empty(all_df, df_tail)
                except Exception as e:
                    logging.warning(f"tail cross-midnight fetch failed: {e}")
                if all_df is None or all_df.empty:
                    logger.warning("no bars %s %s", sym, cur.date())
                    cur = (cur + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    continue
                day_df = (
                    all_df.drop_duplicates(subset=["ts"], keep="first")
                    .sort_values("ts")
                )
                if day_df["ts"].dt.tz is None:
                    day_df["ts"] = day_df["ts"].dt.tz_localize("UTC")
                else:
                    day_df["ts"] = day_df["ts"].dt.tz_convert("UTC")
                missing_ranges: List[tuple[datetime, datetime]] = []
                if tf == "M1" and len(day_df) != 1440:
                    missing_ranges = _find_missing_ranges_utc(day_df)
                    D0 = cur
                    tail_start = D0.replace(hour=20, minute=0, second=0, microsecond=0)
                    tail_end = D0.replace(hour=23, minute=59, second=0, microsecond=0)
                    tail_missing = any(
                        (s <= tail_start and e >= tail_end) for s, e in missing_ranges
                    )
                    if tail_missing:
                        logging.warning(
                            "Falta tramo 20:00–23:59; intentando tail-repair..."
                        )
                        try:
                            df_tail_rep = _repair_tail_if_missing(
                                ib,
                                contract,
                                cur.strftime("%Y-%m-%d"),
                                what_final,
                                int(rth_final),
                            )
                            day_df = _concat_non_empty(day_df, df_tail_rep)
                            day_df = day_df.drop_duplicates(
                                subset=["ts"], keep="last"
                            ).sort_values("ts")
                        except Exception as e:
                            logging.warning(
                                f"tail-repair no pudo completar el tramo 20:00–23:59: {e}"
                            )
                        missing_ranges = _find_missing_ranges_utc(day_df)
                    common = {
                        "ib": ib,
                        "exchange": exchange,
                        "what": what_final,
                        "rth": rth_final,
                    }
                    for start_m, end_m in missing_ranges:
                        df_fix = _repair_range_with_fallback(
                            sym, start_m, end_m, common
                        )
                        if not df_fix.empty:
                            day_df = _concat_non_empty(day_df, df_fix)
                    day_df = day_df.drop_duplicates(subset=["ts"], keep="first").sort_values("ts")
                    if tf == "M1" and len(day_df) != 1440 and allow_synth:
                        day_df = _synth_fill(day_df, cur)
                    day_df = day_df.drop_duplicates(subset=["ts"], keep="first").sort_values("ts")
                    if tail_missing and len(day_df) == 1440:
                        logging.info("tail-repair completó el tramo 20:00–23:59.")
                    elif tail_missing and len(day_df) != 1440:
                        logging.warning("tail-repair no pudo completar el tramo 20:00–23:59.")
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
            day_df["what_to_show"] = what_final
            day_df["vendor"] = "ibkr"
            day_df["tz"] = "UTC"
            day_df = day_df.drop_duplicates(
                subset=["symbol", "timeframe", "ts", "source"], keep="last"
            )
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

