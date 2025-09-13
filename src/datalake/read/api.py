from __future__ import annotations
import os, glob
import pandas as pd
from typing import List, Optional

LAYOUT = "data/source={source}/market={market}/timeframe={tf}/symbol={symbol}/year=*/month=*/part-*.parquet"

def _resolve_paths(lake_root: str, source: str, market: str, tf: str, symbol: str) -> List[str]:
    pat = os.path.join(lake_root, LAYOUT.format(source=source, market=market, tf=tf, symbol=symbol))
    return sorted(glob.glob(pat))

def read_range_df(lake_root: str, *, market: str, tf: str, symbol: str, date_from: str, date_to: str, source: str = "ibkr") -> pd.DataFrame:
    files = _resolve_paths(lake_root, source, market, tf, symbol)
    if not files:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])  # vacío
    df = pd.concat((pd.read_parquet(p) for p in files), ignore_index=True)
    df = df.sort_values("ts")
    m = (df["ts"] >= pd.Timestamp(date_from, tz="UTC")) & (df["ts"] <= pd.Timestamp(date_to, tz="UTC") + pd.Timedelta(minutes=0))
    return df.loc[m].reset_index(drop=True)

def join_mtf_exec_ctx(lake_root: str, *, symbol: str, market: str, exec_tf: str, ctx_tfs: List[str], date_from: str, date_to: str, source: str = "ibkr", suffix_close_only: bool = True) -> pd.DataFrame:
    base = read_range_df(lake_root, market=market, tf=exec_tf, symbol=symbol, date_from=date_from, date_to=date_to, source=source)
    base = base.sort_values("ts")
    out = base.copy()
    for tf in ctx_tfs:
        ctx = read_range_df(lake_root, market=market, tf=tf, symbol=symbol, date_from=date_from, date_to=date_to, source=source)
        if ctx.empty:
            continue
        ctx = ctx.sort_values("ts")
        cols = ["ts","close"] if suffix_close_only else ["ts","open","high","low","close","volume"]
        ctx = ctx[cols].rename(columns={c: (f"{c}_{tf}" if c != "ts" else c) for c in cols})
        out = pd.merge_asof(out, ctx, on="ts", direction="backward")
    return out.reset_index(drop=True)
