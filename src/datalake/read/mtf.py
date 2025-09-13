from typing import Dict, Iterable, Tuple
import pandas as pd
from .reader import read_range

TF_ORDER = {"M1": 1, "M5": 5, "M15": 15, "H1": 60, "H4": 240}


def _rename_ctx(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    cols = {c: f"{c}_{tf}" for c in ["open","high","low","close","volume"] if c in df.columns}
    out = df.rename(columns=cols)
    keep = ["ts"] + list(cols.values())
    return out[keep]


def join_asof_multi(exec_df: pd.DataFrame, ctx_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Alinea múltiples contextos por merge_asof backward sobre 'ts'.
    exec_df debe tener columna 'ts' y estar ordenado por tiempo.
    """
    out = exec_df.sort_values("ts").copy()
    for tf, ctx in sorted(ctx_dfs.items(), key=lambda kv: TF_ORDER.get(kv[0], 999)):
        c = ctx.sort_values("ts")["ts"].is_monotonic_increasing
        if not c:
            ctx = ctx.sort_values("ts")
        out = pd.merge_asof(
            out, _rename_ctx(ctx, tf).sort_values("ts"), on="ts", direction="backward"
        )
    return out


def load_and_align(lake_root: str, symbol: str,
                   exec_tf: str, date_from: str, date_to: str,
                   ctx_tfs: Iterable[str]) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame], pd.DataFrame]:
    """Carga exec_tf y una lista de ctx_tfs desde el lake y devuelve (exec_df, ctx_map, joined)."""
    exec_df = read_range(lake_root, market="crypto", timeframe=exec_tf, symbol=symbol,
                         date_from=date_from, date_to=date_to)
    ctx_map = {}
    for tf in ctx_tfs:
        ctx_map[tf] = read_range(lake_root, market="crypto", timeframe=tf, symbol=symbol,
                                 date_from=date_from, date_to=date_to)
    joined = join_asof_multi(exec_df, ctx_map)
    return exec_df, ctx_map, joined
