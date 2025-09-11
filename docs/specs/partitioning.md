# Particionado del Data Lake

Ruta base (ejemplo cripto IBKR):
```
data/source=ibkr/market=crypto/timeframe=<TF>/symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-<YYYY>-<MM>.parquet
```
- **TF**: M1 (canónico), M5, M15, H1, D1 (agregados offline).
- **SYMBOL**: `BASE-QUOTE` (p. ej., `BTC-USD`).
- **Archivo mensual** con **row groups ~diarios**. Evita “archivo por día” salvo casos especiales.
- **Compresión**: ZSTD (preferida) o Snappy.
- **Semántica**: `ts` en UTC, **bar_end**.
