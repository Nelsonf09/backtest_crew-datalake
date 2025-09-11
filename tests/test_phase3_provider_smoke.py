import pandas as pd
from bridge.backtest_crew.provider import LakeProvider, _norm_tf, _agg_on_the_fly
from datalake.config import LakeConfig


def test_norm_tf():
    assert _norm_tf('1 min') == 'M1'
    assert _norm_tf('5 mins') == 'M5'
    assert _norm_tf('1 day') == 'D1'


def test_agg_on_the_fly_barend():
    ts = pd.date_range('2025-08-01 00:01:00+00:00', periods=6, freq='min')
    df = pd.DataFrame({
        'ts': ts,
        'open': [1, 2, 3, 4, 5, 6],
        'high': [1, 2, 3, 4, 5, 6],
        'low': [1, 2, 3, 4, 5, 6],
        'close': [1, 2, 3, 4, 5, 6],
        'volume': [1] * 6,
        'symbol': ['BTC-USD'] * 6,
        'exchange': ['SIM'] * 6,
    })
    out = _agg_on_the_fly(df, 'M5')
    assert not out.empty and {'open','high','low','close'} <= set(out.columns)

# Nota: test de integración completo requeriría ficheros Parquet reales en data/. Aquí hacemos humo sobre helpers.
