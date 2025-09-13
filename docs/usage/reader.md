# Reader API

### Python
```python
from datalake.read.api import read_range_df, join_mtf_exec_ctx

lake = r"C:/work/backtest_crew-datalake"
df = read_range_df(lake_root=lake, market='crypto', tf='M1', symbol='BTC-USD', date_from='2025-08-01', date_to='2025-08-03')
mtf = join_mtf_exec_ctx(lake, symbol='BTC-USD', market='crypto', exec_tf='M1', ctx_tfs=['M5','M15','H1'], date_from='2025-08-01', date_to='2025-08-01')
```

### CLI
```powershell
python -m datalake.read.cli read --lake-root $env:LAKE_ROOT --market crypto --tf M1 --symbol BTC-USD --date-from 2025-08-01 --date-to 2025-08-03 --head 5
python -m datalake.read.cli join-mtf --lake-root $env:LAKE_ROOT --symbol BTC-USD --exec-tf M1 --ctx-tf M5,M15,H1 --date-from 2025-08-01 --date-to 2025-08-01 --out-csv mtf.csv
```
