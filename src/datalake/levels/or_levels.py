from __future__ import annotations
import pandas as pd
from datalake.config import LakeConfig
from datalake.aggregates.loader import load_m1_range
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def _levels_dest(cfg: LakeConfig, symbol: str, year: int) -> Path:
    root = Path(cfg.root)
    return (root / f"levels/market=crypto/symbol={symbol}/year={year:04d}/part-{year:04d}.parquet").resolve()


def _parse_hhmm(hhmm: str) -> tuple[int,int]:
    hh, mm = hhmm.split(':'); return int(hh), int(mm)


def build_or_levels(symbol: str, start_utc: str, end_utc: str, *, or_window: str = '00:00-01:00', tz: str = 'UTC', cfg: LakeConfig | None = None) -> pd.DataFrame:
    cfg = cfg or LakeConfig()
    df = load_m1_range(symbol, start_utc, end_utc, cfg)
    if df.empty:
        return pd.DataFrame(columns=['session_date','tz','or_start','or_end','or_high','or_low','break_dir','break_ts','retest_ts','retest_price','symbol'])

    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    local = df['ts'].dt.tz_convert(tz)
    sh, sm = _parse_hhmm(or_window.split('-')[0]); eh, em = _parse_hhmm(or_window.split('-')[1])

    rows = []
    df['local_date'] = local.dt.tz_localize(None).dt.date
    for d, day_df in df.groupby('local_date', sort=True):
        day_local = local[df['local_date'] == d]
        if day_local.empty: continue
        start_local = pd.Timestamp(year=day_local.dt.year.iloc[0], month=day_local.dt.month.iloc[0], day=day_local.dt.day.iloc[0], hour=sh, minute=sm, tz=tz)
        end_local   = pd.Timestamp(year=day_local.dt.year.iloc[0], month=day_local.dt.month.iloc[0], day=day_local.dt.day.iloc[0], hour=eh, minute=em, tz=tz)
        mask_or = (day_local >= start_local) & (day_local < end_local)
        or_slice = df.loc[mask_or.values]
        if or_slice.empty: continue
        or_high = float(or_slice['high'].max()); or_low = float(or_slice['low'].min())
        mask_after = (day_local >= end_local); after = df.loc[mask_after.values]
        break_dir = 'NONE'; break_ts = pd.NaT; retest_ts = pd.NaT; retest_price = float('nan')
        if not after.empty:
            up = after[after['close'] > or_high]; dn = after[after['close'] < or_low]
            cand = []
            if not up.empty: cand.append(('UP', up.iloc[0]['ts']))
            if not dn.empty: cand.append(('DOWN', dn.iloc[0]['ts']))
            if cand:
                cand.sort(key=lambda x: x[1]); break_dir, break_ts = cand[0]
                if break_dir == 'UP':
                    rt = after[(after['low'] <= or_high)]
                else:
                    rt = after[(after['high'] >= or_low)]
                if not rt.empty:
                    retest_ts = rt.iloc[0]['ts']; retest_price = float(rt.iloc[0]['close'])
        rows.append({
            'session_date': pd.Timestamp(d), 'tz': tz,
            'or_start': start_local.tz_convert('UTC'), 'or_end': end_local.tz_convert('UTC'),
            'or_high': or_high, 'or_low': or_low,
            'break_dir': break_dir, 'break_ts': break_ts,
            'retest_ts': retest_ts, 'retest_price': retest_price,
            'symbol': symbol,
        })
    out = pd.DataFrame(rows)
    return out.sort_values('session_date').reset_index(drop=True) if not out.empty else out


def write_year_levels(df: pd.DataFrame, symbol: str, cfg: LakeConfig) -> Path:
    if df.empty: raise ValueError('Niveles vacío')
    df = df.copy(); df['year'] = pd.to_datetime(df['session_date']).dt.year
    out: Path | None = None
    for y, chunk in df.groupby('year'):
        dest = _levels_dest(cfg, symbol, int(y)); dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            existing = pq.read_table(dest).to_pandas()
            merged = (pd.concat([existing, chunk], ignore_index=True)
                        .drop_duplicates(['session_date','symbol'], keep='last')
                        .sort_values(['session_date','symbol']).reset_index(drop=True))
        else:
            merged = chunk.sort_values(['session_date','symbol']).reset_index(drop=True)
        table = pa.Table.from_pandas(merged.drop(columns=['year'], errors='ignore'), preserve_index=False)
        tmp = dest.with_suffix('.tmp.parquet'); pq.write_table(table, tmp, compression=LakeConfig().compression); tmp.replace(dest)
        out = dest
    return out
