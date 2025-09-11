# Fase 1 — Ingesta M1 de Cripto (IBKR → Parquet)

Esta guía explica cómo descargar velas **M1** de criptomonedas desde **IBKR** (usando `AGGTRADES`) y guardarlas en el *data lake* como **Parquet ZSTD**, particionado por `source/market/timeframe/symbol/year/month`.

## Prerrequisitos
- **IB Gateway/TWS** ejecutándose (Paper o Live), mismo host por defecto `127.0.0.1:7497`.
- Suscripción/permiso de *market data* de criptomonedas en IB.
- Python 3.10+ y `pip install -r requirements.txt` (incluye `ib_insync`).
- Código de reuso disponible bajo `vendor/backtest_crew` (submódulo o copia). Si usas submódulo:
  ```bash
  git submodule update --init --recursive
  ```

## Semántica de timestamps
- Se usa **bar_end** en UTC: el `ts` de cada barra corresponde al **fin** del minuto.
- Cripto es 24/7: días completos ≈ **1440** filas.

## Quickstart
```bash
# 1) Crear entorno
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt

# 2) (Opcional) Fijar raíz del data lake si quieres escribir fuera del repo
# Linux/Mac
export DATA_LAKE_ROOT=/mnt/datalake
# Windows PowerShell
# setx DATA_LAKE_ROOT "D:\\datalake"  ; reinicia la terminal para que aplique

# 3) Ejecutar ingesta (ejemplos)
python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD --from 2025-07-01 --to 2025-07-31
python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD,ETH-USD --from 2025-07-01 --to 2025-08-31
```
