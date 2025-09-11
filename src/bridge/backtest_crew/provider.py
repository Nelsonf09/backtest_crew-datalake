from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple
import pandas as pd

from datalake.config import LakeConfig
from datalake.aggregates.loader import load_m1_range
from datalake.aggregates.aggregate import _agg as agg_fn  # para fallback on-the-fly

# ------------------------------- Utils -------------------------------
_TF_RULE = {
    '1 min': 'M1', '1min': 'M1', 'M1': 'M1',
    '5 mins': 'M5', '5min': 'M5', 'M5': 'M5',
    '15 mins': 'M15', '15min': 'M15', 'M15': 'M15',
    '1 hour': 'H1', '60min': 'H1', 'H1': 'H1',
    '1 day': 'D1', 'D1': 'D1'
}
_RULE_TO_PANDAS = {'M1':'1min','M5':'5min','M15':'15min','H1':'60min','D1':'1D'}


def _norm_tf(tf: str) -> str:
    return _TF_RULE.get(tf.strip().lower().replace(' ', ''), tf.upper())


def _agg_on_the_fly(df_m1: pd.DataFrame, tf_norm: str) -> pd.DataFrame:
    rule = _RULE_TO_PANDAS[tf_norm]
    return agg_fn(df_m1, rule)


def _read_aggregate_parquet(cfg: LakeConfig, symbol: str, tf_norm: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    root = Path(cfg.root)
    dfs = []
    cur = pd.Timestamp(year=start.year, month=start.month, day=1, tz='UTC')
    endm = pd.Timestamp(year=end.year, month=end.month, day=1, tz='UTC')
    while cur <= endm:
        p = root / f"aggregates/source=ibkr/market=crypto/timeframe={tf_norm}/symbol={symbol}/year={cur.year:04d}/month={cur.month:02d}/part-{cur.year:04d}-{cur.month:02d}.parquet"
        if p.exists():
            dfs.append(pd.read_parquet(p))
        cur = cur + pd.offsets.MonthBegin()
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    out['ts'] = pd.to_datetime(out['ts'], utc=True)
    return out[(out['ts'] >= start) & (out['ts'] <= end)].sort_values('ts').reset_index(drop=True)

# ----------------------------- Provider -----------------------------
@dataclass
class LakeProvider:
    cfg: LakeConfig = LakeConfig()

    def load_exec_and_filter(self, symbol: str, start_utc: str, end_utc: str,
                             exec_tf: str = '1 min', filter_tf: str = '5 mins') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Devuelve (df_exec, df_filter) con columnas ts, open, high, low, close, volume.
        - Lee M1 del datalake; si el TF pedido no es M1, intenta leer agregados y si no existen,
          los calcula on-the-fly (sin escribir) respetando bar_end (label='right', closed='right').
        """
        start = pd.Timestamp(start_utc, tz='UTC'); end = pd.Timestamp(end_utc, tz='UTC')
        tf_exec = _norm_tf(exec_tf); tf_filter = _norm_tf(filter_tf)

        # Base M1 para ambos
        df_m1 = load_m1_range(symbol, start_utc, end_utc, self.cfg)
        if df_m1.empty:
            return df_m1, df_m1
        # Normalizar metadata mínima
        for c in ['source','market','symbol','exchange']:
            if c not in df_m1.columns:
                df_m1[c] = None
        
        def _make(tf_norm: str) -> pd.DataFrame:
            if tf_norm == 'M1':
                return df_m1.copy()
            # primero intentamos leer pre-agregado
            agg = _read_aggregate_parquet(self.cfg, symbol, tf_norm, start, end)
            if not agg.empty:
                return agg
            # fallback a on-the-fly
            return _agg_on_the_fly(df_m1, tf_norm)

        df_exec = _make(tf_exec)
        df_filter = _make(tf_filter)
        # Garantizar orden y tipos
        for d in (df_exec, df_filter):
            d['ts'] = pd.to_datetime(d['ts'], utc=True)
            d.sort_values('ts', inplace=True)
            d.reset_index(drop=True, inplace=True)
        return df_exec, df_filter
