# Fase 3 — Bridge offline para backtest_crew

Permite ejecutar el motor rápido B&R **sin IB**, leyendo del datalake.

## Uso rápido
```bash
# 1) Instala este repo en el entorno donde corre backtest_crew
pip install -e .

# 2) (Opcional) fija la raíz del lake
export LAKE_ROOT=/ruta/a/tu/datalake

# 3) Smoke test
python -m bridge.backtest_crew.cli \
  --symbol BTC-USD --from 2025-07-01 --to 2025-07-10 \
  --exec-tf "1 min" --filter-tf "5 mins"
```

## Integración en backtest_crew (sin tocar estrategia)
En el sitio donde hoy pides datos a IB, invoca el provider:
```python
from bridge.backtest_crew.provider import LakeProvider
from datalake.config import LakeConfig
prov = LakeProvider(LakeConfig())
df_exec, df_filter = prov.load_exec_and_filter(
    symbol='BTC-USD',
    start_utc='2025-07-01 00:00:00Z',
    end_utc='2025-07-31 23:59:59Z',
    exec_tf='1 min',
    filter_tf='5 mins',
)
```
- Columnas: `ts, open, high, low, close, volume` (más meta), ordenadas por `ts` (bar_end).
- Si no existen agregados precomputados, el bridge resamplea **on-the-fly**.

## Notas
- Resampling con `label='right', closed='right'` (anti *lookahead*).
- Recomendado tener agregados de **Fase 2** para velocidad.
- `LAKE_ROOT` controla dónde están los Parquet.
