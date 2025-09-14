import logging

import pandas as pd
from ib_insync import IB, Contract

logger = logging.getLogger("ibkr.downloader")


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
    logger.debug(
        "reqHistoricalData endDateTime=%s durationStr=%s barSize=%s what=%s rth=%s exch=%s sym=%s",
        end_date_time,
        duration_str,
        bar_size,
        what_to_show,
        use_rth,
        contract.exchange,
        contract.symbol,
    )
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_date_time,
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
