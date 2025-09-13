# backtest_crew-datalake

**Fase 0 (Diseño)** — Estándares para un data lake local de backtesting (con foco en **Cripto**), independiente de IB en tiempo de ejecución.

## Objetivos Fase 0
- Definir **formato canónico**: Parquet (ZSTD), columnas OHLCV + `ts` UTC (semántica **bar_end**).
- Definir **particionado**: `source/market/timeframe/symbol/year/month/`.
- Establecer **datasets derivados**: agregados (M5/M15/H1/D1) y **niveles diarios** (estrechos) con OR/PDH/PDL.
- Estándares anti-look-ahead para estrategias MTF (B&R).
- Perfiles de **liquidez cripto** (OR windows por tz) separados de los datos.

## Layout
```
data/
  source=ibkr/
    market=crypto/
      timeframe=M1/
        symbol=BTC-USD/
          year=2025/
            month=08/
              part-2025-08.parquet
levels/
  market=crypto/
    symbol=BTC-USD/
      kind=daily/
        year=2025/
          month=08/
            btcusd_levels_2025-08.parquet
```

## Convenciones clave
- **UTC everywhere**. `ts` = fin de vela (**bar_end**).
- **M1 canónico** + agregados offline.
- Niveles diarios **estrechos**: `session_date`, `pdh/pdl/pdc` (D-1), `or_start_utc/or_end_utc`, `orh/orl`.
- **Crypto**: `what_to_show=AGGTRADES`, 24/7, sin RTH.

## Próximas fases
- Fase 1: Ingesta offline M1 (BTC-USD, ETH-USD) y validación.
- Fase 2: Agregados (M5/M15/H1/D1) y niveles diarios.
- Fase 3: Capa de acceso (DuckDB→pandas / Polars) y contratos de lectura.


## Reuso de código `backtest_crew`
Este repo **reusa** módulos de `backtest_crew` ubicados bajo `vendor/backtest_crew` (como submódulo o snapshot). Para importarlos desde el código del datalake:

```python
from datalake.ingestors.ibkr.submodule_bridge import ensure_submodule_on_syspath
ensure_submodule_on_syspath()
from config.crypto_symbols import CRYPTO_SYMBOLS
```



## Fase 1 · Ingesta de Cripto (M1, IBKR)
- Descarga de velas **M1** vía IBKR (`AGGTRADES`).
- Persistencia en **Parquet ZSTD** con particionado `source/market/timeframe/symbol/year/month`.
- **Timestamps** en UTC con semántica **bar_end**.

### Quickstart
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD --from 2025-07-01 --to 2025-07-31
```
Más detalles en: [`docs/usage/ingest_crypto_m1.md`](docs/usage/ingest_crypto_m1.md)



## Fase 2 · Agregados & Niveles (B&R)
Ver guía: [`docs/usage/phase2_aggregates_levels.md`](docs/usage/phase2_aggregates_levels.md)



## Fase 3 · Bridge Offline (backtest_crew)
- Proveedor `LakeProvider` que entrega `df_exec` y `df_filter` con el mismo layout que IB.
- Soporta TFs: M1/M5/M15/H1/D1 (usa agregados precomputados o *fallback* on-the-fly).

```bash
python -m bridge.backtest_crew.cli --symbol BTC-USD --from 2025-07-01 --to 2025-07-10 --exec-tf "1 min" --filter-tf "5 mins"
```
Más info: [`docs/usage/phase3_offline_bridge.md`](docs/usage/phase3_offline_bridge.md)

## Uso detallado

### Fase 2 — Resample desde M1 (offline con datos sintéticos)

> Objetivo: validar pipeline de resampling M1→M5/M15/H1 sin depender de IB.

**Preparación**
```powershell
# PowerShell
$env:LAKE_ROOT = "C:\\work\\backtest_crew-datalake"
$env:IB_EXCHANGE_CRYPTO = "PAXOS"
$env:IB_WHAT_TO_SHOW    = "AGGTRADES"
```

**Generar M1 sintético (ej. 3 días)**
```powershell
python .\\tools\\synth_gen.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03
```

**Validar M1 por día**
```powershell
python .\\tools\\check_day.py --symbol BTC-USD --date 2025-08-01 --lake-root C:\\work\\backtest_crew-datalake
python .\\tools\\check_day.py --symbol BTC-USD --date 2025-08-02 --lake-root C:\\work\\backtest_crew-datalake
python .\\tools\\check_day.py --symbol BTC-USD --date 2025-08-03 --lake-root C:\\work\\backtest_crew-datalake
```

**Resample M1 → M5,M15,H1**
```powershell
python .\\tools\\resample_from_m1.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --to-tf M5,M15,H1
```

**Checks por TF**
```powershell
python .\\tools\\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf M5  --lake-root C:\\work\\backtest_crew-datalake
python .\\tools\\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf M15 --lake-root C:\\work\\backtest_crew-datalake
python .\\tools\\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf H1  --lake-root C:\\work\\backtest_crew-datalake
```

**Demo offline (todo en un paso)**
```powershell
powershell -ExecutionPolicy Bypass -File .\\tools\\offline_demo.ps1
```

### Fase 3 — Ingesta IB (chunking) y validación

> Objetivo: cubrir 24h/día M1 con IB (cuando el servidor esté disponible), en 3 ventanas de 8h (00–08, 08–16, 16–24) con subtramos robustos.

**Ingesta con chunking**
```powershell
$env:LAKE_ROOT="C:\\work\\backtest_crew-datalake"
$env:IB_HOST="127.0.0.1"; $env:IB_PORT="7497"; $env:IB_CLIENT_ID="1"
$env:IB_EXCHANGE_CRYPTO="PAXOS"; $env:IB_WHAT_TO_SHOW="AGGTRADES"
python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD --from 2025-08-01 --to 2025-08-01
```

**Validación de día completo**
```powershell
python .\\tools\\check_day.py --symbol BTC-USD --date 2025-08-01 --lake-root C:\\work\\backtest_crew-datalake
```

**Notas**
- Reingestar el mismo rango es idempotente (deduplicación por `ts`).
- Si usaste datos sintéticos para rellenar, puedes borrar el `part-YYYY-MM.parquet` antes de reingestar o configurar el writer para preferir `keep='last'`.

## Fase 4 — Reader + Multi-TF join

**Objetivo**: consumir datos del datalake sin IB y alinear contextos (M5/M15/H1) con el TF de ejecución.

### Uso rápido (PowerShell)
```powershell
$env:LAKE_ROOT = "C:\\work\\backtest_crew-datalake"

# Leer rango
python -m datalake.read.cli read --lake-root $env:LAKE_ROOT --market crypto --tf M1 --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --head 5

# Join multi-TF
python -m datalake.read.cli join-mtf --lake-root $env:LAKE_ROOT --symbol BTC-USD --exec-tf M1 --from 2025-08-01 --to 2025-08-01 --ctx-tf M5,M15,H1 --out-csv mtf_join.csv
```

### Documentación
- `docs/usage/layout.md`
- `docs/usage/reader.md`
- `docs/usage/mtf.md`
- `docs/usage/troubleshooting.md`
