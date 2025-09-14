from __future__ import annotations
import datetime as dt
from typing import Iterable, List
import pandas as pd
from rich import print
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


def _fetch_bars(cli: IBClient, contract: Contract, end_dt: dt.datetime, duration_str: str) -> pd.DataFrame | None:
    """
    Función auxiliar para solicitar un fragmento de datos históricos.
    """
    bars = cli.ib.reqHistoricalData(
        contract,
        endDateTime=end_dt,
        durationStr=duration_str,
        barSizeSetting='1 min',
        whatToShow='AGGTRADES',
        useRTH=False,
        formatDate=1,
        keepUpToDate=False
    )
    return util.df(bars)


def fetch_crypto_m1_range(symbol: str, start_utc: str, end_utc: str, client_cfg: IBClientConfig | None = None) -> pd.DataFrame:
    """Descarga histórico M1, manejando el truncamiento de datos de IBKR al final del día.

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

        for day in _date_range_days(start, end):
            day_dfs: List[pd.DataFrame] = []
            
            # --- Intento 1: Obtener el día completo ---
            day_end_dt = (day + pd.Timedelta(days=1)).to_pydatetime()
            cli._throttle()
            main_df = _fetch_bars(cli, contract, day_end_dt, '1 D')

            if main_df is None or main_df.empty:
                continue
            
            # Procesar el fragmento principal
            main_df = to_bar_end_utc(main_df, 'date')
            day_dfs.append(main_df)
            
            # --- Verificación y Recuperación del Final del Día ---
            last_ts = main_df['ts'].max()
            expected_day_end_ts = day.normalize() + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)

            # Si el último timestamp es anterior al esperado, faltan datos.
            if last_ts < expected_day_end_ts:
                missing_seconds = int((expected_day_end_ts - last_ts).total_seconds())
                
                # Pedir el fragmento faltante
                if missing_seconds > 60:
                    print(f"[yellow]Día incompleto para {symbol} en {day.date()}. Última vela: {last_ts}. Intentando recuperar {missing_seconds // 60} minutos...[/yellow]")
                    cli._throttle()
                    tail_df = _fetch_bars(cli, contract, day_end_dt, f'{missing_seconds} S')
                    
                    if tail_df is not None and not tail_df.empty:
                        tail_df = to_bar_end_utc(tail_df, 'date')
                        day_dfs.append(tail_df)

            # Unir y procesar los fragmentos del día
            full_day_df = pd.concat(day_dfs, ignore_index=True).drop_duplicates('ts').sort_values('ts')

            full_day_df['source'] = 'ibkr'
            full_day_df['market'] = 'crypto'
            full_day_df['symbol'] = symbol
            full_day_df['exchange'] = contract.exchange
            full_day_df['what_to_show'] = 'AGGTRADES'
            
            all_dfs.append(full_day_df[['ts','open','high','low','close','volume','source','market','symbol','exchange','what_to_show']])

        if not all_dfs:
            return pd.DataFrame()
        
        # Concatena todos los días y asegura la grilla M1
        out = enforce_m1_grid(pd.concat(all_dfs, ignore_index=True))
        
        # Filtrar para asegurar que solo tenemos datos dentro del rango original solicitado
        query_end_ts = end.normalize() + pd.Timedelta(days=1)
        final_out = out[(out['ts'] >= start) & (out['ts'] < query_end_ts)].copy()
        return final_out
        
    finally:
        cli.disconnect()
