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

## Fase 1 — Ingesta M1 de Cripto (IBKR)
La Fase 1 implementa la ingesta offline de velas M1 desde Interactive Brokers.
Sigue la [guía de uso](docs/usage/ingest_crypto_m1.md) para descargar datos de **BTC-USD**, **ETH-USD** u otros símbolos y
guardarlos como Parquet particionado.

## Próximas fases
- Fase 2: Agregados (M5/M15/H1/D1) y niveles diarios.
- Fase 3: Capa de acceso (DuckDB→pandas / Polars) y contratos de lectura.


## Reuso de código `backtest_crew`
Este repo **reusa** módulos de `backtest_crew` ubicados bajo `vendor/backtest_crew` (como submódulo o snapshot). Para importarlos desde el código del datalake:

```python
from datalake.ingestors.ibkr.submodule_bridge import ensure_submodule_on_syspath
ensure_submodule_on_syspath()
from config.crypto_symbols import CRYPTO_SYMBOLS
```

