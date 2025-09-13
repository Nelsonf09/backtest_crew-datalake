import pandas as pd
from datetime import datetime, timezone
from datalake.read.mtf import join_asof_multi


def _mk_df(freq: str, start="2025-08-01 00:00:00+00:00", end="2025-08-01 23:59:00+00:00"):
    idx = pd.date_range(start, end, freq=freq, tz="UTC")
    return pd.DataFrame({
        "ts": idx,
        "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1.0
    })


def test_join_mtf_shapes():
    exec_df = _mk_df("1min")
    m5  = _mk_df("5min")
    m15 = _mk_df("15min")
    h1  = _mk_df("1h")
    out = join_asof_multi(exec_df, {"M5": m5, "M15": m15, "H1": h1})
    # mismas filas que el TF de ejecución
    assert len(out) == len(exec_df)
    for tf in ("M5","M15","H1"):
        for c in ("open","high","low","close","volume"):
            assert f"{c}_{tf}" in out.columns
