from __future__ import annotations
import datetime as dt
from typing import Iterable, List
import pandas as pd
from ib_insync import util
from datalake.ingestors.ibkr.ib_client import IBClient, IBClientConfig
from datalake.ingestors.ibkr.contracts import make_crypto_contract
from datalake.ingestors.ibkr.normalize import to_bar_end_utc, enforce_m1_grid


def _date_range_days(start: pd.Timestamp, end: pd.Timestamp) -> Iterable[pd.Timestamp]:
    cur = start.normalize()
    end = end.normalize()
    while cur <= end:
        yield cur
        cur += pd.Timedelta(days=1)


def fetch_crypto_m1_range(symbol: str, start_utc: str, end_utc: str, client_cfg: IBClientConfig | None = None) -> pd.DataFrame:
    """Descarga histórico M1 (AGGTRADES) por ventanas diarias y concatena.
    Retorna DataFrame con columnas: ts, open, high, low, close, volume, source, market, symbol, exchange, what_to_show.
    """
    cfg = client_cfg or IBClientConfig()
    cli = IBClient(cfg)
    cli.connect()
    try:
        contract = make_crypto_contract(symbol)
        start = pd.Timestamp(start_utc, tz='UTC')
        end = pd.Timestamp(end_utc, tz='UTC')
        dfs: List[pd.DataFrame] = []
        for day in _date_range_days(start, end):
            cli._throttle()
            # endDateTime debe ser fin del día UTC
            end_dt = (day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).to_pydatetime()
            bars = cli.ib.reqHistoricalData(
                contract,
                endDateTime=end_dt,
                durationStr='1 D',
                barSizeSetting='1 min',
                whatToShow='AGGTRADES',
                useRTH=False,
                formatDate=1,
                keepUpToDate=False
            )
            df = util.df(bars)
            if df.empty:
                continue
            df = to_bar_end_utc(df, 'date')
            df = df.rename(columns={'open':'open','high':'high','low':'low','close':'close','volume':'volume'})
            df['source'] = 'ibkr'
            df['market'] = 'crypto'
            df['symbol'] = symbol
            df['exchange'] = contract.exchange
            df['what_to_show'] = 'AGGTRADES'
            dfs.append(df[['ts','open','high','low','close','volume','source','market','symbol','exchange','what_to_show']])
        if not dfs:
            return pd.DataFrame(columns=['ts','open','high','low','close','volume','source','market','symbol','exchange','what_to_show'])
        out = enforce_m1_grid(pd.concat(dfs, ignore_index=True))
        return out
    finally:
        cli.disconnect()
