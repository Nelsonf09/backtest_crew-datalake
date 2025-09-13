# Visión general

**backtest_crew-datalake** provee:

- **Ingesta (Fase 1/3)**: descarga de velas desde IBKR (Crypto con `AGGTRADES` en `PAXOS`), escritura **idempotente** (Parquet mensual por símbolo/TF), normalización de esquema y deduplicación.
- **Datos sintéticos (Fase 2)**: generación M1 offline + **resample** a M5/M15/H1 + validadores.
- **Lectura (Fase 4)**: API para leer rangos por símbolo/TF y **join multi-TF (asof)** para estrategias que usan contexto superior (ej. EMAs).

## Arquitectura (resumen)
- **Layout particionado**: `data/source=ibkr/market=crypto/timeframe=TF/symbol=SYM/year=YYYY/month=MM/part-YYYY-MM.parquet`.
- **Esquema canónico**: `ts, open, high, low, close, volume, source, market, timeframe, symbol, exchange, what_to_show, vendor, tz`.
- **Idempotencia**: al re-ingestar o completar huecos, se lee el Parquet del mes, se mergea por `ts` (último gana) y se re-escribe.
- **Multi-TF**: `merge_asof` (backward) sobre `ts` del TF de ejecución contra TFs de contexto.

