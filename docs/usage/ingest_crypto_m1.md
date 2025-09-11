# Fase 1 — Ingesta M1 de Cripto (IBKR → Parquet)

Esta guía explica cómo descargar velas **M1** de criptomonedas desde **IBKR** (usando `AGGTRADES`) y guardarlas en el *data lake* como **Parquet ZSTD**, particionado por fuente/mercado/timeframe/símbolo/año/mes.

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
export LAKE_ROOT=/mnt/datalake
# Windows PowerShell
# setx LAKE_ROOT "D:\\datalake"  ; reinicia la terminal para que aplique

# 3) Ejecutar ingesta (ejemplos)
python -m datalake.ingestors.ibkr.ingest_cli \
  --symbols BTC-USD,ETH-USD \
  --from 2025-07-01 --to 2025-07-31
```

## ¿Qué se genera?
```
<LAKE_ROOT || repo>/data/source=ibkr/market=crypto/timeframe=M1/
└── symbol=BTC-USD/
    └── year=2025/
        └── month=07/
            └── part-2025-07.parquet
```
Columnas: `ts` (UTC bar_end), `open`, `high`, `low`, `close`, `volume`, `source`, `market`, `symbol`, `exchange`, `what_to_show`.

## Verificación rápida
```bash
python - << 'PY'
import pandas as pd
p = 'data/source=ibkr/market=crypto/timeframe=M1/symbol=BTC-USD/year=2025/month=07/part-2025-07.parquet'
df = pd.read_parquet(p)
print(df.ts.min(), df.ts.max(), len(df))
print(df.head())
PY
```

## Opciones del CLI
```
python -m datalake.ingestors.ibkr.ingest_cli --help
  --symbols    Lista separada por comas (BTC-USD,ETH-USD,...)
  --from       Inicio (UTC, YYYY-MM-DD)
  --to         Fin (UTC, YYYY-MM-DD)
```
> El *rate limit* por defecto es ~0.7 req/s y se gestiona internamente para respetar el pacing de IB.

## Problemas comunes (troubleshooting)
- **Error 10299 (AGGTRADES)**: asegúrate de no usar `TRADES`. El ingestor usa `whatToShow='AGGTRADES'`.
- **Timeout/Cancelación en connect**: comprueba IB Gateway/TWS en `127.0.0.1:7497` y que el `clientId` no esté en uso.
- **Sin datos**: revisa suscripción de mercado cripto, rango de fechas y símbolo (`BTC-USD`, `ETH-USD`).
- **Archivos no aparecen**: valida `LAKE_ROOT` (si no está, se usa la raíz del repo).

## Rendimiento y diseño
- Escritura **mensual** por símbolo (ZSTD), idempotente: si reingestas, se fusiona y deduplica por `ts`.
- Layout particionado facilita cargas en motores analíticos y *pruning* por año/mes.
- Para ingestiones grandes, paraleliza **por símbolo** (cada proceso usa su propio `clientId`).

## Próximos pasos (Fase 2)
- Construcción de agregados (M5/M15/H1/D1).
- Derivados de niveles/Break & Retest para orquestación B&R.
