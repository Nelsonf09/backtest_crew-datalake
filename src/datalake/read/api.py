from __future__ import annotations
import os, glob
import pandas as pd
from typing import List, Optional

LAYOUT = "data/source={source}/market={market}/timeframe={tf}/symbol={symbol}/year=*/month=*/part-*.parquet"

def _resolve_paths(lake_root: str, source: str, market: str, tf: str, symbol: str) -> List[str]:
    pat = os.path.join(lake_root, LAYOUT.format(source=source, market=market, tf=tf, symbol=symbol))
    return sorted(glob.glob(pat))

def read_range_df(lake_root: str, *, market: str, tf: str, symbol: str, date_from: str, date_to: str, source: str = "ibkr") -> pd.DataFrame:
    """
    Lee datos del lake y DEVUELVE por contrato global un DataFrame con:
      - Rango temporal half-open: [date_from, date_to) (fin EXCLUSIVO)
      - Columna ts como datetime64[ns, UTC]
      - Timestamps ordenados y SIN duplicados (drop_duplicates por 'ts')

    Nota: Si date_from/date_to son None, no se aplica el filtrado correspondiente.
    """

    files = _resolve_paths(lake_root, source, market, tf, symbol)
    if not files:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])  # vacío

    df = pd.concat((pd.read_parquet(p) for p in files), ignore_index=True)

    # --- Normalización y contrato global de salida ---
    if df is None or len(df) == 0:
        return df

    if "ts" not in df.columns:
        return df

    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    else:
        try:
            tz = getattr(df["ts"].dtype, "tz", None)
            if tz is None:
                df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            elif str(tz) != "UTC":
                df["ts"] = df["ts"].dt.tz_convert("UTC")
        except Exception:
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    df = df.dropna(subset=["ts"])

    def _to_utc(ts_like):
        if ts_like is None:
            return None
        ts = pd.Timestamp(ts_like)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC") if str(ts.tzinfo) != "UTC" else ts
        return ts

    _start = _to_utc(date_from) if date_from is not None else None
    _end = _to_utc(date_to) if date_to is not None else None

    if _start is not None:
        df = df.loc[df["ts"] >= _start]
    if _end is not None:
        df = df.loc[df["ts"] < _end]  # ⚠️ fin EXCLUSIVO por contrato global

    if "ts" in df.columns:
        df = (
            df.sort_values("ts")
              .drop_duplicates(subset=["ts"], keep="first")
              .reset_index(drop=True)
        )

    return df

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
