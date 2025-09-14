from __future__ import annotations
import datetime as dt
from typing import Iterable, List
import pandas as pd
from ib_insync import util, Contract
from datalake.ingestors.ibkr.ib_client import IBClient, IBClientConfig
from datalake.ingestors.ibkr.contracts import make_crypto_contract
from datalake.ingestors.ibkr.normalize import to_bar_end_utc, enforce_m1_grid


def _date_range_days(start: pd.Timestamp, end: pd.Timestamp) -> Iterable[pd.Timestamp]:
    """
    Genera un rango de fechas día por día.
    """
    cur = start.normalize()
    end = end.normalize()
    while cur <= end:
        yield cur
        cur += pd.Timedelta(days=1)


def _fetch_bars_chunk(cli: IBClient, contract: Contract, end_dt: dt.datetime, duration_sec: int) -> pd.DataFrame | None:
    """
    Función auxiliar para solicitar un fragmento de datos históricos.
    """
    bars = cli.ib.reqHistoricalData(
        contract,
        endDateTime=end_dt,
        durationStr=f'{duration_sec} S',
        barSizeSetting='1 min',
        whatToShow='AGGTRADES',
        useRTH=False,
        formatDate=1,
        keepUpToDate=False
    )
    return util.df(bars)


def fetch_crypto_m1_range(symbol: str, start_utc: str, end_utc: str, client_cfg: IBClientConfig | None = None) -> pd.DataFrame:
    """Descarga histórico M1 (AGGTRADES) dividiendo cada día en dos fragmentos para evitar límites de la API.

    Retorna DataFrame con columnas: ts, open, high, low, close, volume, source, 
    market, symbol, exchange, what_to_show.
    """
    cfg = client_cfg or IBClientConfig()
    cli = IBClient(cfg)
    cli.connect()
    try:
        contract = make_crypto_contract(symbol)
        start = pd.Timestamp(start_utc, tz='UTC')
        end = pd.Timestamp(end_utc, tz='UTC')
        all_dfs: List[pd.DataFrame] = []

        # 43200 segundos = 12 horas
        chunk_duration_sec = 43200 

        for day in _date_range_days(start, end):
            day_dfs: List[pd.DataFrame] = []
            
            # Parte 1: Primeras 12 horas del día
            end_dt_part1 = (day + pd.Timedelta(hours=12)).to_pydatetime()
            cli._throttle()
            df1 = _fetch_bars_chunk(cli, contract, end_dt_part1, chunk_duration_sec)
            if df1 is not None and not df1.empty:
                day_dfs.append(df1)

            # Parte 2: Últimas 12 horas del día
            end_dt_part2 = (day + pd.Timedelta(hours=24)).to_pydatetime()
            cli._throttle()
            df2 = _fetch_bars_chunk(cli, contract, end_dt_part2, chunk_duration_sec)
            if df2 is not None and not df2.empty:
                day_dfs.append(df2)

            if not day_dfs:
                continue

            # Unir y procesar los fragmentos del día
            day_df = pd.concat(day_dfs, ignore_index=True)
            day_df = to_bar_end_utc(day_df, 'date')
            day_df = day_df.rename(columns={'open':'open','high':'high','low':'low','close':'close','volume':'volume'})
            day_df['source'] = 'ibkr'
            day_df['market'] = 'crypto'
            day_df['symbol'] = symbol
            day_df['exchange'] = contract.exchange
            day_df['what_to_show'] = 'AGGTRADES'
            
            all_dfs.append(day_df[['ts','open','high','low','close','volume','source','market','symbol','exchange','what_to_show']])

        if not all_dfs:
            return pd.DataFrame(columns=['ts','open','high','low','close','volume','source','market','symbol','exchange','what_to_show'])
        
        # Concatena todos los dataframes de todos los días y asegura la grilla M1
        out = enforce_m1_grid(pd.concat(all_dfs, ignore_index=True))
        # Filtrar para asegurar que solo tenemos datos dentro del rango solicitado
        final_out = out[(out['ts'] >= start) & (out['ts'] < (end + pd.Timedelta(days=1)))].copy()
        return final_out
        
    finally:
        cli.disconnect()
