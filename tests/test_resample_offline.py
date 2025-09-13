import pandas as pd
from datetime import datetime, timezone
from datalake.aggregates.aggregate import resample_df


def test_resample_counts():
    # construye un día M1 completo
    day = datetime(2025,8,1,tzinfo=timezone.utc)
    idx = pd.date_range(day.replace(hour=0,minute=0,second=0,microsecond=0),
                        day.replace(hour=23,minute=59,second=0,microsecond=0),
                        freq="1min", tz="UTC")
    df = pd.DataFrame({
        "ts": idx,
        "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1
    })
    m5  = resample_df(df, "5min")
    m15 = resample_df(df, "15min")
    h1  = resample_df(df, "1H")
    assert len(df) == 1440
    assert len(m5) == 288
    assert len(m15) == 96
    assert len(h1) == 24
