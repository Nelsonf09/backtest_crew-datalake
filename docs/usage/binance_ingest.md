# Ingesta con Binance

## Por día
```powershell
python -m datalake.ingestors.binance.ingest_cli `
  --symbols BTC-USD `
  --from 2025-08-01 --to 2025-08-01 `
  --tf M1 `
  --binance-region global
```
Soporta TF: **M1, M5, M15, M30** (uno por corrida). Cambia `--tf` según necesidad.

## Por mes (orquestación)
```powershell
python tools\fill_binance_month.py `
  --symbols BTC-USD `
  --month 2025-08 `
  --tfs M1,M5,M15,M30 `
  --region global
```
Parámetros útiles adicionales (si el script los expone): `--sleep-per-call`, `--max-weight-per-minute`.
