import glob, os
from typing import List, Optional
import pandas as pd
from .paths import months_between, symbol_base
from .schemas import enforce_schema


def list_month_files(lake_root: str, market: str, timeframe: str, symbol: str,
                     date_from: str, date_to: str) -> List[str]:
    base = symbol_base(lake_root, market, timeframe, symbol)
    files: List[str] = []
    for yy, mm in months_between(date_from, date_to):
        patt = os.path.join(base, f"year={yy}", f"month={mm:02d}", "*.parquet")
        files.extend(glob.glob(patt))
    return sorted(set(files))


def read_range(lake_root: str, market: str, timeframe: str, symbol: str,
               date_from: str, date_to: str,
               columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Lee filas cuya ts ∈ [date_from 00:00:00, date_to 23:59:59] UTC.
    Devuelve DataFrame ordenado por ts y con schema normalizado.
    """
    files = list_month_files(lake_root, market, timeframe, symbol, date_from, date_to)
    if not files:
        return enforce_schema(pd.DataFrame(columns=["ts","open","high","low","close","volume"]), timeframe, symbol)
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f, columns=columns))
        except Exception:
            dfs.append(pd.read_parquet(f))
    df = pd.concat(dfs, ignore_index=True)
    df = enforce_schema(df, timeframe=timeframe, symbol=symbol)
    start = pd.Timestamp(date_from + " 00:00:00+00:00")
    end   = pd.Timestamp(date_to   + " 23:59:59+00:00")
    df = df[(df["ts"] >= start) & (df["ts"] <= end)].sort_values("ts").reset_index(drop=True)
    return df
