$ErrorActionPreference = 'Stop'
$env:LAKE_ROOT = "$PSScriptRoot\.." | Resolve-Path
$env:IB_EXCHANGE_CRYPTO = "PAXOS"
$env:IB_WHAT_TO_SHOW    = "AGGTRADES"

# 1) Generar M1 sintético (3 días)
python .\tools\synth_gen.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03

# 2) Checks M1 por día
python .\tools\check_day.py --symbol BTC-USD --date 2025-08-01 --lake-root $env:LAKE_ROOT
python .\tools\check_day.py --symbol BTC-USD --date 2025-08-02 --lake-root $env:LAKE_ROOT
python .\tools\check_day.py --symbol BTC-USD --date 2025-08-03 --lake-root $env:LAKE_ROOT

# 3) Resample M1 -> M5,M15,H1 para el rango
python .\tools\resample_from_m1.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --to-tf M5,M15,H1

# 4) Checks por TF (día 1)
python .\tools\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf M5 --lake-root $env:LAKE_ROOT
python .\tools\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf M15 --lake-root $env:LAKE_ROOT
python .\tools\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf H1 --lake-root $env:LAKE_ROOT
