from __future__ import annotations

import glob
import os
from datetime import timedelta, timezone
from typing import List, Tuple

import pandas as pd


def find_missing_ranges_utc(
    symbol: str,
    date_utc: str,
    timeframe: str,
    exchange: str,
    what_to_show: str,
    cfg,
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Lee el parquet del día y retorna rangos faltantes en minutos UTC."""
    day = pd.Timestamp(date_utc).tz_localize("UTC")
    start = day
    end = day + pd.Timedelta(hours=23, minutes=59)

    data_root = getattr(cfg, "data_root", getattr(cfg, "root", "."))
    market = getattr(cfg, "market", "crypto")
    base = os.path.join(
        data_root,
        "data",
        "source=ibkr",
        f"market={market}",
        f"timeframe={timeframe}",
        f"symbol={symbol}",
    )
    yy = f"{day.year:04d}"
    mm = f"{day.month:02d}"
    patterns = [
        os.path.join(base, f"year={yy}", f"month={mm}", "*.parquet"),
        os.path.join(base, f"year={yy}", "month=*", "*.parquet"),
    ]
    files: List[str] = []
    for p in patterns:
        files.extend(glob.glob(p))
    if files:
        df = pd.concat((pd.read_parquet(f) for f in sorted(set(files))), ignore_index=True)
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df_day = df[(df["ts"] >= start) & (df["ts"] <= end)].sort_values("ts")
    else:
        df_day = pd.DataFrame(columns=["ts"])  # vacío

    if df_day.empty:
        return [(start, end)]

    full = pd.date_range(start, end, freq="1min", tz=timezone.utc)
    missing = full.difference(pd.DatetimeIndex(df_day["ts"]))
    if missing.empty:
        return []
    ranges: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    s = missing[0]
    prev = s
    for ts in missing[1:]:
        if ts - prev == pd.Timedelta(minutes=1):
            prev = ts
        else:
            ranges.append((s, prev))
            s = ts
            prev = ts
    ranges.append((s, prev))
    return ranges
