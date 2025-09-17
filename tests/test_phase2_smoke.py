import pandas as pd
from datalake.aggregates.aggregate import _agg
from datalake.levels import or_levels as ol

def test_agg_barend():
    ts = pd.date_range('2025-08-01 00:01:00+00:00', periods=3, freq='min')
    df = pd.DataFrame({
        'ts': ts,
        'open': [1, 2, 3],
        'high': [1, 2, 3],
        'low': [1, 2, 3],
        'close': [1, 2, 3],
        'volume': [1, 1, 1],
        'symbol': ['BTC-USD'] * 3,
        'exchange': ['SIM'] * 3,
    })
    out = _agg(df, '5min')
    assert {'open','high','low','close','volume'} <= set(out.columns)

def test_levels_basic():
    ts = pd.date_range('2025-08-01 00:01:00+00:00', periods=6, freq='min')
    price = [10,11,12,13,12,11]
    df = pd.DataFrame({'ts':ts,'open':price,'high':[p+0.1 for p in price],'low':[p-0.1 for p in price],'close':price,'volume':[1]*6})
    def fake_loader(symbol, start, end, cfg): return df
    ol.load_m1_range = fake_loader  # monkeypatch
    out = ol.build_or_levels('BTC-USD','2025-08-01 00:00:00Z','2025-08-01 23:59:59Z', or_window='00:00-00:02', tz='UTC')
    assert set(['or_high','or_low','break_dir']).issubset(out.columns)
