# Reader API

El lector permite cargar rangos de datos por **símbolo/TF** filtrando por `ts` en UTC.

## CLI

```powershell
# Variables
$env:LAKE_ROOT = "C:\\work\\backtest_crew-datalake"

# Leer M1 de 2025-08-01 a 2025-08-03
python -m datalake.read.cli read --lake-root $env:LAKE_ROOT --market crypto --tf M1 --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --head 5

# o con entrypoint instalado
# datalake-read read --lake-root $env:LAKE_ROOT --market crypto --tf M1 --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --head 5
```

## Python (API)

```python
from datalake.read.reader import read_range

df = read_range(
    lake_root=r"C:\\work\\backtest_crew-datalake",
    market="crypto",
    timeframe="M1",
    symbol="BTC-USD",
    date_from="2025-08-01",
    date_to="2025-08-03",
)
print(df.shape, df.ts.min(), df.ts.max())
```
