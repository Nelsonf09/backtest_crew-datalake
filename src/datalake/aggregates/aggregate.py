from __future__ import annotations
import pandas as pd
from datalake.config import LakeConfig
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Reglas de resampleo por timeframe
# M5='5min', M15='15min', H1='1h', D1='1d'
_RULES = {'M5':'5min','M15':'15min','H1':'1h','D1':'1d'}


def _dest_path(cfg: LakeConfig, symbol: str, tf: str, year: int, month: int) -> Path:
    root = Path(cfg.root)
    return (root / f"aggregates/source=ibkr/market=crypto/timeframe={tf}/symbol={symbol}/year={year:04d}/month={month:02d}/part-{year:04d}-{month:02d}.parquet").resolve()


def _agg(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    res = resample_df(df, rule)
    res['source'] = 'ibkr'; res['market'] = 'crypto'
    if 'symbol' in df.columns and not df['symbol'].empty:
        res['symbol'] = df['symbol'].iloc[-1]
    if 'exchange' in df.columns and not df['exchange'].empty:
        res['exchange'] = df['exchange'].iloc[-1]
    return res[['ts','open','high','low','close','volume','source','market','symbol','exchange']]


def resample_df(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample minuto M1 OHLCV a otra frecuencia.

    - Localiza/convierte ``ts`` a UTC.
    - Ordena por índice y elimina duplicados.
    - Usa ``label='left', closed='left'`` para alinear a la izquierda.
    - Forward-fill de columnas OHLC para continuidad.
    """
    df = df.copy()
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df = (df.set_index('ts')
            .sort_index()
            .loc[lambda x: ~x.index.duplicated(keep='last')])
    res = (df.resample(rule, label='left', closed='left')
             .agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}))
    res[['open','high','low','close']] = res[['open','high','low','close']].ffill()
    res = res.dropna(subset=['open','high','low','close']).reset_index()
    return res


def write_month_aggregate(df: pd.DataFrame, symbol: str, tf: str, cfg: LakeConfig) -> Path:
    df = df.copy()
    df['year'] = pd.to_datetime(df['ts'], utc=True).dt.year
    df['month'] = pd.to_datetime(df['ts'], utc=True).dt.month
    out: Path | None = None
    for (y,m), chunk in df.groupby(['year','month']):
        dest = _dest_path(cfg, symbol, tf, int(y), int(m))
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            existing = pq.read_table(dest).to_pandas()
            merged = (pd.concat([existing, chunk], ignore_index=True)
                        .drop_duplicates('ts', keep='last')
                        .sort_values('ts').reset_index(drop=True))
        else:
            merged = chunk.sort_values('ts').reset_index(drop=True)
        table = pa.Table.from_pandas(merged.drop(columns=['year','month'], errors='ignore'), preserve_index=False)
        tmp = dest.with_suffix('.tmp.parquet'); pq.write_table(table, tmp, compression=LakeConfig().compression); tmp.replace(dest)
        out = dest
    return out if out else _dest_path(cfg, symbol, tf, int(df['year'].iloc[-1]), int(df['month'].iloc[-1]))


def aggregate_symbol(symbol: str, start_utc: str, end_utc: str, timeframes: list[str], loader_func, cfg: LakeConfig) -> dict[str, list[Path]]:
    raw = loader_func(symbol, start_utc, end_utc, cfg)
    if raw.empty:
        return {tf: [] for tf in timeframes}
    out: dict[str, list[Path]] = {}
    for tf in timeframes:
        rule = _RULES[tf]
        agg = _agg(raw, rule)
        agg['year'] = pd.to_datetime(agg['ts'], utc=True).dt.year
        agg['month'] = pd.to_datetime(agg['ts'], utc=True).dt.month
        paths = []
        for (y,m), chunk in agg.groupby(['year','month']):
            p = write_month_aggregate(chunk.drop(columns=['year','month']), symbol, tf, cfg)
            paths.append(p)
        out[tf] = paths
    return out
