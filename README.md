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

