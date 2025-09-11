from __future__ import annotations
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datalake.config import LakeConfig

PART_FMT = 'part-{year:04d}-{month:02d}.parquet'


def _dest_path(cfg: LakeConfig, symbol: str, year: int, month: int) -> Path:
    root = Path(cfg.root)
    return (root / f"data/source=ibkr/market=crypto/timeframe=M1/symbol={symbol}/year={year:04d}/month={month:02d}/" / PART_FMT.format(year=year, month=month)).resolve()


def write_month(df: pd.DataFrame, symbol: str, cfg: LakeConfig) -> Path:
    if df.empty:
        raise ValueError('DataFrame vacío')
    # Infer year/month from 'ts' (UTC)
    df = df.copy()
    df['year'] = df['ts'].dt.year
    df['month'] = df['ts'].dt.month
    out_paths = []
    for (y, m), chunk in df.groupby(['year', 'month']):
        dest = _dest_path(cfg, symbol, int(y), int(m))
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Si existe, leemos y fusionamos (sin append directo, para dedupe estable)
        if dest.exists():
            existing = pq.read_table(dest).to_pandas()
            merged = (pd.concat([existing, chunk], ignore_index=True)
                        .drop_duplicates('ts', keep='last')
                        .sort_values('ts')
                        .reset_index(drop=True))
        else:
            merged = chunk.sort_values('ts').reset_index(drop=True)
        table = pa.Table.from_pandas(merged.drop(columns=['year','month'], errors='ignore'), preserve_index=False)
        tmp = dest.with_suffix('.tmp.parquet')
        pq.write_table(table, tmp, compression=LakeConfig().compression)
        tmp.replace(dest)
        out_paths.append(dest)
    return out_paths[-1]
