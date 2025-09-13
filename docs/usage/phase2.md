# Fase 2 — Sintético + Resample + Checks

### Sintético M1
```powershell
python .\\tools\\synth_gen.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03
```

### Resample (desde M1)
```powershell
# Nota: usar '1h' en vez de '1H' para evitar FutureWarning
python .\\tools\\resample_from_m1.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --to-tf M5,M15,H1
```
Cada día debe producir exactamente:
- M5 → **288** velas
- M15 → **96** velas
- H1 → **24** velas

### Validadores de integridad
```powershell
python .\\tools\\check_day.py  --symbol BTC-USD --date 2025-08-01 --lake-root $env:LAKE_ROOT  # espera 1440 filas
python .\\tools\\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf M5 --lake-root $env:LAKE_ROOT
python .\\tools\\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf M15 --lake-root $env:LAKE_ROOT
python .\\tools\\check_mtf.py --symbol BTC-USD --date 2025-08-01 --tf H1 --lake-root $env:LAKE_ROOT
```

### Conteos esperados por día
- M1: **1440**
- M5: **288**
- M15: **96**
- H1: **24**

