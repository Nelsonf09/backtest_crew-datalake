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
    """Wrapper around IB.reqHistoricalData with debug logging.

    end_date_time must include the " UTC" suffix. duration_str is expressed in
    seconds, e.g. "28800 S".
    """
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
