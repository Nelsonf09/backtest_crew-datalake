import pandas as pd
from datetime import datetime, timezone
from datalake.aggregates.aggregate import resample_df


def test_resample_counts_and_ranges():
    # construye un día M1 completo
    day = datetime(2025, 8, 1, tzinfo=timezone.utc)
    idx = pd.date_range(day.replace(hour=0, minute=0, second=0, microsecond=0),
                        day.replace(hour=23, minute=59, second=0, microsecond=0),
                        freq="1min", tz="UTC")
    df = pd.DataFrame({
        "ts": idx,
        "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1
    })
    m5 = resample_df(df, "5min")
    m15 = resample_df(df, "15min")
    h1 = resample_df(df, "1h")
    d1 = resample_df(df, "1d")
    assert len(df) == 1440
    assert len(m5) == 288
    assert len(m15) == 96
    assert len(h1) == 24
    assert len(d1) == 1
    assert m5['ts'].iloc[0] == day.replace(hour=0, minute=0)
    assert m5['ts'].iloc[-1] == day.replace(hour=23, minute=55)
    assert m15['ts'].iloc[-1] == day.replace(hour=23, minute=45)
    assert h1['ts'].iloc[-1] == day.replace(hour=23, minute=0)
def test_resample_idempotent():
    day = datetime(2025, 8, 1, tzinfo=timezone.utc)
    idx = pd.date_range(day.replace(hour=0), day.replace(hour=23, minute=59), freq="1min", tz="UTC")
    df = pd.DataFrame({
        "ts": idx,
        "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1
    })
    once = resample_df(df, "5min")
    twice = resample_df(once, "5min")
    pd.testing.assert_frame_equal(once, twice)
