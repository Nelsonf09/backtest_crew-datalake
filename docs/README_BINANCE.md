# Source: Binance (Phase-4)

Ingesta alternativa a IB para klines spot 1m/5m (UTC), sin credenciales.

## Símbolos
- Lógico del proyecto: `BTC-USD` (etc.)
- Binance Spot: `BTCUSDT` (mapping automático)

## CLI
```bash
python -m datalake.ingestors.binance.ingest_cli \
  --symbols BTC-USD,ETH-USD \
  --from 2025-08-01 --to 2025-08-03 \
  --tf M1 \
  --binance-region global
```

## Tail focalizado (20:00–23:59 UTC)
```bash
python tools/binance_fetch_tail.py \
  --date 2025-08-01 \
  --symbol BTC-USD \
  --region global \
  --out ./tmp/BTC-USD_2025-08-01_tail_binance.csv
```

## Notas
- Timestamps tz-aware en UTC.
- Dedupe/merge idéntico al de IB (clave: symbol, tf, ts, source).
- Para Binance.US usar `--binance-region us`.
