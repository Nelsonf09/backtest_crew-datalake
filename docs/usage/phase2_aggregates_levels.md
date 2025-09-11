# Fase 2 — Agregados (M5/M15/H1/D1) y Niveles OR (B&R)

## Agregados
```bash
python -m datalake.aggregates.cli --symbols BTC-USD,ETH-USD --from 2025-07-01 --to 2025-07-31 --tfs M5,M15,H1,D1
```
Salida: `aggregates/source=ibkr/market=crypto/timeframe=<TF>/symbol=<SYM>/year=YYYY/month=MM/part-YYYY-MM.parquet`

## Niveles OR
```bash
python -m datalake.levels.cli --symbols BTC-USD --from 2025-07-01 --to 2025-07-31 --or-window 00:00-01:00 --tz UTC
```
Salida: `levels/market=crypto/symbol=<SYM>/year=YYYY/part-YYYY.parquet`
