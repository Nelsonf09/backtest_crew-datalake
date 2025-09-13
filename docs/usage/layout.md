# Layout del datalake

```
{LAKE_ROOT}/
  data/
    source=ibkr/
      market=crypto/
        timeframe=TF/              # M1, M5, M15, H1, ...
          symbol=SYMBOL/           # p.ej., BTC-USD
            year=YYYY/
              month=MM/
                part-YYYY-MM.parquet
```

- **Schema canónico**: `ts, open, high, low, close, volume, source, market, timeframe, symbol, exchange, what_to_show, vendor, tz`.
- **Particionado** mensual/año para escrituras idempotentes y lecturas eficientes.
