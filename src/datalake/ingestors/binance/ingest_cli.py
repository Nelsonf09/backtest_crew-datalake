from __future__ import annotations
import os
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from datalake.providers.binance.client import fetch_klines
from datalake.utils.symbols.binance_map import to_binance_symbol

UTC = timezone.utc

TF_CHOICES = ['M1','M5','M15','M30']

def _dt_utc(d: str, h: int, m: int, s: int = 0) -> datetime:
    y, mo, da = map(int, d.split('-'))
    return datetime(y, mo, da, h, m, s, tzinfo=UTC)

def _days_iter(date_from: str, date_to: str):
    d0 = datetime.strptime(date_from, '%Y-%m-%d').date()
    d1 = datetime.strptime(date_to, '%Y-%m-%d').date()
    cur = d0
    while cur <= d1:
        yield cur.isoformat()
        cur = cur + timedelta(days=1)

def _expect_rows(tf: str) -> int:
    return {
        'M1': 1440,
        'M5': 288,
        'M15': 96,
        'M30': 48,
    }[tf]

def _add_control_cols(df: pd.DataFrame, symbol_logico: str, tf: str, region: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df['symbol'] = symbol_logico
    df['tf'] = tf
    df['source'] = 'binance'
    df['exchange'] = 'BINANCE.US' if region == 'us' else 'BINANCE'
    return df

def write_merge_dedupe(df: pd.DataFrame, *, root: str | None = None) -> str:
    if df is None or df.empty:
        return ""
    root = root or os.getenv("LAKE_ROOT", os.getcwd())
    symbol = df['symbol'].iloc[0]
    tf = df['tf'].iloc[0]
    year = int(df['ts'].dt.year.iloc[0])
    month = int(df['ts'].dt.month.iloc[0])
    base = (
        Path(root)
        / "data"
        / "source=binance"
        / "market=crypto"
        / f"timeframe={tf}"
        / f"symbol={symbol}"
        / f"year={year}"
        / f"month={month:02d}"
    )
    base.mkdir(parents=True, exist_ok=True)
    dest_file = base / f"part-{year}-{month:02d}.parquet"
    existing = pd.DataFrame()
    if dest_file.exists():
        try:
            existing = pq.read_table(dest_file).to_pandas()
        except Exception:
            existing = pd.read_parquet(dest_file)
    merged = pd.concat([existing, df], ignore_index=True)
    merged['ts'] = pd.to_datetime(merged['ts'], utc=True)
    merged = merged.drop_duplicates(
        subset=['symbol', 'tf', 'ts', 'source'], keep='last'
    ).sort_values('ts')
    table = pa.Table.from_pandas(merged, preserve_index=False)
    pq.write_table(table, dest_file, compression="zstd", version="2.6", use_dictionary=False)
    return str(dest_file)

def ingest(args: argparse.Namespace) -> None:
    symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]
    for sym in symbols:
        b_sym = to_binance_symbol(sym)
        for day in _days_iter(args.date_from, args.date_to):
            start = _dt_utc(day, 0, 0, 0)
            end   = _dt_utc(day, 23, 59, 0)

            # Llamada ya paginada
            df = fetch_klines(
                symbol=b_sym,
                start_dt=start,
                end_dt=end,
                tf=args.tf,
                region=args.binance_region
            )

            df = _add_control_cols(df, sym, args.tf, args.binance_region)

            if df is not None and not df.empty:
                write_merge_dedupe(df)

            # Validación
            exp = _expect_rows(args.tf)
            if exp is not None:
                rows = 0 if df is None else len(df)
                if rows != exp:
                    # Log claro de cobertura y primeras/últimas marcas
                    first_ts = None if (df is None or df.empty) else df['ts'].min()
                    last_ts  = None if (df is None or df.empty) else df['ts'].max()
                    print(f"[WARN] {sym} {day} tf={args.tf}: filas={rows} (esperado={exp}) range={first_ts}→{last_ts}")

def main() -> int:
    p = argparse.ArgumentParser(description='Ingesta Binance (source=binance) — Phase-4')
    p.add_argument('--symbols', required=True, help='Lista separada por comas, e.g. BTC-USD,ETH-USD')
    p.add_argument('--from', dest='date_from', required=True, help='YYYY-MM-DD (UTC)')
    p.add_argument('--to', dest='date_to', required=True, help='YYYY-MM-DD (UTC)')
    p.add_argument('--tf', choices=TF_CHOICES, default='M1')
    p.add_argument('--binance-region', choices=['global','us'], default=os.getenv('BINANCE_REGION','global'))
    args = p.parse_args()
    ingest(args)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
