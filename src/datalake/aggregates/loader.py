from __future__ import annotations
from pathlib import Path
import pandas as pd
from datalake.config import LakeConfig

def iter_month_paths(symbol: str, start: pd.Timestamp, end: pd.Timestamp, cfg: LakeConfig) -> list[Path]:
    paths: list[Path] = []
    root = Path(cfg.root)
    cur = pd.Timestamp(year=start.year, month=start.month, day=1, tz='UTC')
    endm = pd.Timestamp(year=end.year, month=end.month, day=1, tz='UTC')
    while cur <= endm:
        p = root / f"data/source=ibkr/market=crypto/timeframe=M1/symbol={symbol}/year={cur.year:04d}/month={cur.month:02d}/part-{cur.year:04d}-{cur.month:02d}.parquet"
        if p.exists():
            paths.append(p)
        cur = cur + pd.offsets.MonthBegin()
    return paths


def load_m1_range(symbol: str, start_utc: str, end_utc: str, cfg: LakeConfig) -> pd.DataFrame:
    start = pd.Timestamp(start_utc, tz='UTC')
    end = pd.Timestamp(end_utc, tz='UTC')
    dfs = [pd.read_parquet(p) for p in iter_month_paths(symbol, start, end, cfg)]
    if not dfs:
        return pd.DataFrame(columns=['ts','open','high','low','close','volume','source','market','symbol','exchange','what_to_show'])
    out = pd.concat(dfs, ignore_index=True)
    out['ts'] = pd.to_datetime(out['ts'], utc=True)
    out = out[(out['ts'] >= start) & (out['ts'] <= end)].sort_values('ts').reset_index(drop=True)
    return out
