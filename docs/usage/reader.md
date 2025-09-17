# Reader (CLI y API)

## CLI — leer rango (head/tail)
```powershell
python -m datalake.read.cli read `
  --lake-root C:\work\backtest_crew-datalake `
  --market crypto --tf M1 --symbol BTC-USD `
  --date-from 2025-08-01 --date-to 2025-08-02 `
  --source binance --head 5
```
> **Importante**: `--date-to` es **exclusivo**. Para un día completo usa `date-to = día+1`.

## API — ejemplo en línea
```python
from datalake.read.api import read_range_df

df = read_range_df(
    lake_root=r"C:/work/backtest_crew-datalake",
    market='crypto', tf='M1', symbol='BTC-USD',
    date_from='2025-08-01', date_to='2025-08-02',
    source='binance'
)
print(len(df), df.ts.min(), df.ts.max())
```
