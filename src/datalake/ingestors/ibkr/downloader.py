import logging
import logging
import os
from types import SimpleNamespace
from typing import List

import pandas as pd
from ib_insync import IB, Contract

from .timeutil import to_utc

logger = logging.getLogger("ibkr.downloader")


def _req_historical_with_retry(
    ib: IB,
    contract: Contract,
    *,
    end_date_time: str,
    duration_str: str,
    bar_size: str,
    what_to_show: str,
    use_rth: bool,
    fmt_date: int = 2,
):
    try:
        return ib.reqHistoricalData(
            contract,
            endDateTime=end_date_time,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=int(use_rth),
            formatDate=fmt_date,
            keepUpToDate=False,
        )
    except Exception as e:
        msg = str(e)
        needs_agg = ("10299" in msg) and ("AGGTRADES" in msg.upper())
        if needs_agg and what_to_show.upper() != "AGGTRADES":
            logger.warning(
                "IB exige AGGTRADES (10299). Reintentando con whatToShow=AGGTRADES."
            )
            return ib.reqHistoricalData(
                contract,
                endDateTime=end_date_time,
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow="AGGTRADES",
                useRTH=int(use_rth),
                formatDate=fmt_date,
                keepUpToDate=False,
            )
        raise

def download_window(
    ib: IB,
    contract: Contract,
    *,
    end_date_time: str,
    duration_str: str,
    bar_size: str,
    what_to_show: str,
    use_rth: bool,
) -> pd.DataFrame:
    """Wrapper around ``IB.reqHistoricalData`` with debug logging.

    Both ``end_date_time`` and ``duration_str`` are passed verbatim to IB. The
    duration string **must** already be expressed in seconds using the
    ``"{N} S"`` format.
    """
    if not duration_str.endswith(" S"):
        raise ValueError("duration_str must be in seconds, e.g. '28800 S'")
    logger.info(
        "REQ[W] sym=%s exch=%s what=%s useRTH=%s bar=%s end=%s dur=%s",
        contract.symbol,
        contract.exchange,
        what_to_show,
        use_rth,
        bar_size,
        end_date_time,
        duration_str,
    )
    bars = _req_historical_with_retry(
        ib,
        contract,
        end_date_time=end_date_time,
        duration_str=duration_str,
        bar_size=bar_size,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )
    if not bars:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(b.__dict__ for b in bars)[
        ["date", "open", "high", "low", "close", "volume"]
    ]
    df["ts"] = pd.to_datetime(df["date"], utc=True)
    return df.drop(columns=["date"]).sort_values("ts")


def fetch_hist_bars(
    ib: IB,
    contract: Contract,
    end_dt_utc,
    duration_seconds: int,
    bar_size: str = "1 min",
    what: str = "AGGTRADES",
    rth: bool = False,
) -> pd.DataFrame:
    """Perform a HMDS request with explicit parameters.

    Parameters mirror IB's ``reqHistoricalData`` arguments but enforce seconds for
    ``duration_seconds`` and UTC for ``end_dt_utc``. Returns a dataframe with
    ``ts`` in UTC alongside OHLCV columns.
    """

    if isinstance(end_dt_utc, str):
        end_str = end_dt_utc
    else:
        end_str = end_dt_utc.strftime("%Y%m%d %H:%M:%S UTC")
    duration_str = f"{int(duration_seconds)} S"
    df = download_window(
        ib,
        contract,
        end_date_time=end_str,
        duration_str=duration_str,
        bar_size=bar_size,
        what_to_show=what,
        use_rth=rth,
    )
    logger.debug(
        "fetch_hist_bars endDateTime=%s durationStr=%s barSize=%s whatToShow=%s useRTH=%s rows=%d ts_min=%s ts_max=%s",
        end_str,
        duration_str,
        bar_size,
        what,
        rth,
        len(df),
        df["ts"].min() if not df.empty else None,
        df["ts"].max() if not df.empty else None,
    )
    return df


BAR_SIZES = {
    "M1": "1 min",
    "H1": "1 hour",
}


def bars_to_df(bars, exchange: str) -> pd.DataFrame:
    """Convierte lista de barras de ib_insync en DataFrame con ts en UTC."""
    if not bars:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(
        [
            {
                "date": getattr(b, "date", None),
                "open": float(getattr(b, "open", "nan")),
                "high": float(getattr(b, "high", "nan")),
                "low": float(getattr(b, "low", "nan")),
                "close": float(getattr(b, "close", "nan")),
                "volume": float(getattr(b, "volume", "nan")),
            }
            for b in bars
        ]
    )
    df["ts"] = to_utc(df["date"], exchange)
    df = df.drop(columns=["date"]).sort_values("ts").reset_index(drop=True)
    return df


def fetch_bars_range(
    symbol: str,
    exchange: str,
    end_dt_utc,
    duration_seconds: int,
    timeframe: str,
    what_to_show: str,
    use_rth: bool = False,
) -> List[SimpleNamespace]:
    """Descarga barras históricas para un rango arbitrario.

    Devuelve la lista de barras tal como ``ib_insync`` la proporciona. Si la
    variable de entorno ``DATALAKE_SYNTH`` es ``"1"`` se generan barras
    sintéticas para pruebas offline.
    """

    if os.getenv("DATALAKE_SYNTH") == "1":
        end = pd.to_datetime(end_dt_utc, utc=True)
        start = end - pd.Timedelta(seconds=int(duration_seconds))
        times = pd.date_range(start, end - pd.Timedelta(minutes=1), freq="1min", tz="UTC")
        return [
            SimpleNamespace(
                date=ts.to_pydatetime(),
                open=1.0,
                high=1.0,
                low=1.0,
                close=1.0,
                volume=1.0,
            )
            for ts in times
        ]

    from .contracts import make_crypto_contract

    host = os.getenv("IB_HOST", "127.0.0.1")
    port = int(os.getenv("IB_PORT", "7497"))
    client_id = int(os.getenv("IB_CLIENT_ID", "1"))
    ib = IB()
    ib.connect(host, port, clientId=client_id, timeout=15)
    try:
        contract = make_crypto_contract(symbol, exchange=exchange)
        end_str = end_dt_utc if isinstance(end_dt_utc, str) else end_dt_utc.strftime("%Y%m%d %H:%M:%S UTC")
        duration_str = f"{int(duration_seconds)} S"
        bar_size = BAR_SIZES.get(timeframe, timeframe)
        bars = _req_historical_with_retry(
            ib,
            contract,
            end_date_time=end_str,
            duration_str=duration_str,
            bar_size=bar_size,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        return bars
    finally:
        ib.disconnect()
