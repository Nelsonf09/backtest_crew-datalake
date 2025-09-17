# Layout y convenciones

- Parquet comprimido (ZSTD) por **mes** y **símbolo/TF**.
- `ts` en UTC y representa **fin de vela** (bar_end).
- Lecturas con semántica **[from, to)** para conteo exacto.

```
repo_root/
  data/
    source=binance/
      market=crypto/
        timeframe=M1|M5|M15|M30/
          symbol=SYMBOL/
            year=YYYY/
              month=MM/
                part-YYYY-MM.parquet
```
