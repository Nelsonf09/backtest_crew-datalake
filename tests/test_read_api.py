import os
from datalake.read.api import read_range_df, join_mtf_exec_ctx

def test_api_smoke(tmp_path):
    # smoke: no archivos => DF vacío
    df = read_range_df(str(tmp_path), market='crypto', tf='M1', symbol='BTC-USD', date_from='2025-08-01', date_to='2025-08-01')
    assert df.empty
