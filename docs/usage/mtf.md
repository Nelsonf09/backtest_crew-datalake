# Multi-Timeframe (asof join)

Alinea timeframes de contexto (M5/M15/H1) con el TF de ejecución (p. ej., M1) usando `merge_asof` con dirección **backward**.

## CLI

```powershell
$env:LAKE_ROOT = "C:\\work\\backtest_crew-datalake"

# Une M1 (exec) con M5,M15,H1 para el rango
python -m datalake.read.cli join-mtf --lake-root $env:LAKE_ROOT --symbol BTC-USD --exec-tf M1 --from 2025-08-01 --to 2025-08-01 --ctx-tf M5,M15,H1 --out-csv mtf_join.csv

# o con entrypoint instalado
# datalake-join-mtf join-mtf --lake-root $env:LAKE_ROOT --symbol BTC-USD --exec-tf M1 --from 2025-08-01 --to 2025-08-01 --ctx-tf M5,M15,H1 --out-csv mtf_join.csv
```

## Python (API)

```python
from datalake.read.mtf import load_and_align

exec_df, ctx_map, joined = load_and_align(
    lake_root=r"C:\\work\\backtest_crew-datalake",
    symbol="BTC-USD",
    exec_tf="M1",
    date_from="2025-08-01",
    date_to="2025-08-01",
    ctx_tfs=["M5","M15","H1"],
)
print(joined.columns)
```

### Notas
- Se renombran columnas de contexto como `close_M5`, `close_M15`, `close_H1`, etc.
- El join es **backward**: cada vela de M1 toma el último valor disponible del TF superior en o antes de su `ts`.
