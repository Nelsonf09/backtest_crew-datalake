# Fase 1 — Ingesta IBKR (Crypto)

### Variables
- `IB_HOST`, `IB_PORT`, `IB_CLIENT_ID`
- `IB_EXCHANGE_CRYPTO` = `PAXOS`
- `IB_WHAT_TO_SHOW` = `AGGTRADES` (Crypto **requiere** `AGGTRADES`).

### Ejecución (PowerShell)
```powershell
$env:LAKE_ROOT = "C:\\work\\backtest_crew-datalake"
$env:IB_HOST = "127.0.0.1"; $env:IB_PORT = "7497"; $env:IB_CLIENT_ID = "1"
$env:IB_EXCHANGE_CRYPTO = "PAXOS"; $env:IB_WHAT_TO_SHOW = "AGGTRADES"

python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD \
  --from 2025-08-01 --to 2025-08-01
```

### Salida
- Escribe/actualiza `part-YYYY-MM.parquet` de **M1**.
- Re-ejecuciones son **seguras**: mergean por `ts` sin duplicados.

