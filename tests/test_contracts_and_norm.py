from datalake.ingestors.ibkr.contracts import split_symbol, make_crypto_contract
from datalake.ingestors.ibkr.normalize import to_bar_end_utc
import pandas as pd

def test_split_symbol():
    assert split_symbol('BTC-USD') == ('BTC','USD')
    assert split_symbol('ETHUSD') == ('ETH','USD')

def test_to_bar_end():
    df = pd.DataFrame({'date':['2025-08-01T00:00:00Z','2025-08-01T00:01:00Z'], 'open':[1,2],'high':[1,2],'low':[1,2],'close':[1,2],'volume':[1,1]})
    out = to_bar_end_utc(df)
    assert str(out.loc[0,'ts']) == '2025-08-01 00:01:00+00:00'
