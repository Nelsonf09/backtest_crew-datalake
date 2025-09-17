# backtest_crew-datalake — Fase 4 (Reader + Binance)

> **Objetivo**: data lake local para backtesting con cripto OHLCV en UTC y semántica `bar_end`, ingestas reproducibles (Binance) y lecturas sin IB. Esta versión documenta **Fase 4** y asume la rama `phase-4`.

## Índice
- [Arquitectura y layout](#arquitectura-y-layout)
- [Requisitos](#requisitos)
- [Setup rápido (Windows PowerShell)](#setup-rápido-windows-powershell)
- [Seleccionar/actualizar `phase-4`](#seleccionaractualizar-phase-4)
- [Ingesta por día (Binance)](#ingesta-por-día-binance)
- [Ingesta por mes (orquestador)](#ingesta-por-mes-orquestador)
- [Lectura y validaciones (QC)](#lectura-y-validaciones-qc)
- [Ejemplos por timeframe](#ejemplos-por-timeframe)
- [Join Multi-TF para estrategias](#join-multi-tf-para-estrategias)
- [Resolución de problemas](#resolución-de-problemas)

---

## Arquitectura y layout
**Convenciones**
- Formato canónico: **Parquet (ZSTD)** con columnas: `ts` (UTC, fin de vela), `open`, `high`, `low`, `close`, `volume`, `symbol`, `tf`, `source`, `exchange`.
- Particionado mensual por símbolo y TF:
```
repo_root/
  data/
    source=binance/
      market=crypto/
        timeframe=M1|
                  M5|
                 M15|
                 M30/
          symbol=BTC-USD/
            year=YYYY/
              month=MM/
                part-YYYY-MM.parquet
```
- Semántica de lectura: rangos **[from, to)** (fin exclusivo) para obtener el conteo exacto de velas por día.

## Requisitos
- Python 3.10+ (recomendado)
- Windows PowerShell (ejemplos usan PowerShell, pero hay equivalentes POSIX en `docs/`)

## Setup rápido (Windows PowerShell)
```powershell
# 1) Clonar y entrar al repo
git clone https://github.com/Nelsonf09/backtest_crew-datalake.git
cd backtest_crew-datalake

# 2) Crear/activar venv e instalar deps
python -m venv .venv
..\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3) Apuntar al lake (root del repo)
$env:LAKE_ROOT = "C:\work\backtest_crew-datalake"
```

## Seleccionar/actualizar `phase-4`
```powershell
git fetch origin
git checkout phase-4
git pull --ff-only
```

## Ingesta por día (Binance)
> Fuente alternativa a IB, sin credenciales (se usa API pública) y con manejo de paginación.

Ejemplo para **BTC-USD** en **M1** el **2025-08-01**:
```powershell
python -m datalake.ingestors.binance.ingest_cli `
  --symbols BTC-USD `
  --from 2025-08-01 --to 2025-08-01 `
  --tf M1 `
  --binance-region global
```
Cambia `--tf` por **M5**, **M15** o **M30** para otros timeframes:
```powershell
# M5
python -m datalake.ingestors.binance.ingest_cli --symbols BTC-USD --from 2025-08-01 --to 2025-08-01 --tf M5 --binance-region global
# M15
python -m datalake.ingestors.binance.ingest_cli --symbols BTC-USD --from 2025-08-01 --to 2025-08-01 --tf M15 --binance-region global
# M30
python -m datalake.ingestors.binance.ingest_cli --symbols BTC-USD --from 2025-08-01 --to 2025-08-01 --tf M30 --binance-region global
```

## Ingesta por mes (orquestador)
> Script de orquestación que recorre los días del mes y TFs solicitados respetando límites de solicitudes de Binance.
```powershell
python tools\fill_binance_month.py `
  --symbols BTC-USD `
  --month 2025-08 `
  --tfs M1,M5,M15,M30 `
  --region global
```
Parámetros:
- `--symbols`: uno o varios símbolos separados por coma (ej: `BTC-USD,ETH-USD`).
- `--month`: `YYYY-MM`.
- `--tfs`: lista de TFs (M1,M5,M15,M30).
- `--region`: `global` o `us`.

## Lectura y validaciones (QC)
**Principio**: rangos **[from, to)** → día completo sin vela extra del día siguiente.

Leer 2025-08-01 completo en **M1**:
```powershell
python -m datalake.read.cli read `
  --lake-root C:\work\backtest_crew-datalake `
  --market crypto --tf M1 --symbol BTC-USD `
  --date-from 2025-08-01 --date-to 2025-08-02 `
  --source binance --head 3
```
**QC mensual automático** (comprueba que M1=1440, M5=288, M15=96, M30=48 por día):
```powershell
$py = @'
from datalake.read.api import read_range_df
import pandas as pd
from datetime import datetime, timedelta, timezone

LAKE   = r"C:/work/backtest_crew-datalake"
SYMBOL = "BTC-USD"; SOURCE = "binance"
YEAR, MONTH = 2025, 8
UTC = timezone.utc
start = datetime(YEAR, MONTH, 1, tzinfo=UTC)
end_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
ndays = (end_month - start).days
expected = {"M1":1440, "M5":288, "M15":96, "M30":48}

issues = []
for tf, exp in expected.items():
    for i in range(ndays):
        d0 = (start + timedelta(days=i)).date().isoformat()
        d1 = (start + timedelta(days=i+1)).date().isoformat()
        df = read_range_df(LAKE, 'crypto', tf, SYMBOL, d0, d1, SOURCE)
        if len(df) != exp:
            issues.append((tf, d0, len(df), exp))

print('QC terminado.')
if issues:
    print('Días con conteo inesperado:')
    for x in issues: print('-', x)
else:
    print('¡Todo completo para el mes en todos los TFs!')
'@
$py | Set-Content .\tmp_qc_mes.py -Encoding UTF8
python .\tmp_qc_mes.py
```

## Ejemplos por timeframe
- **M1**: 1440 velas por día.
- **M5**: 288 velas por día.
- **M15**: 96 velas por día.
- **M30**: 48 velas por día.

Lectura de un **subrango intradía** (ej: 14:30–16:30 UTC):
```powershell
python -m datalake.read.cli read `
  --lake-root C:\work\backtest_crew-datalake `
  --market crypto --tf M1 --symbol BTC-USD `
  --date-from 2025-08-01T14:30:00Z --date-to 2025-08-01T16:30:00Z `
  --source binance --head 5
```

## Join Multi-TF para estrategias
```powershell
python -m datalake.read.cli join-mtf `
  --lake-root C:\work\backtest_crew-datalake `
  --symbol BTC-USD `
  --exec-tf M1 `
  --from 2025-08-01 --to 2025-08-02 `
  --ctx-tf M5,M15,M30 `
  --out-csv mtf_join.csv
```

## Resolución de problemas
- **DF vacío al leer**: verifica `--lake-root` (debe apuntar al **repo root**) y que la ingesta haya creado `data/source=binance/.../part-YYYY-MM.parquet`.
- **Más/menos filas de lo esperado**: asegúrate de usar `date-to` **exclusivo** (día+1). Reingesta es idempotente (se deduplican `ts`).
- **Binance rate limits**: el orquestador pacea solicitudes. Si ves 429, reintenta con `--sleep-per-call` mayor (ver docs) o reduce TFs/símbolos por corrida.
- **Regiones**: si un símbolo no existe en `us`, usa `--region global`.

---

**Licencia**: MIT
