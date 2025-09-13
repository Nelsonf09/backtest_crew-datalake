# Fase 4 — Reader + Multi-TF join (asof)

### Lectura de rangos
```powershell
python -m datalake.read.cli read --lake-root $env:LAKE_ROOT \
  --market crypto --tf M1 --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --head 5
```

### Join Multi-TF
```powershell
python -m datalake.read.cli join-mtf --lake-root $env:LAKE_ROOT \
  --symbol BTC-USD --exec-tf M1 --from 2025-08-01 --to 2025-08-01 \
  --ctx-tf M5,M15,H1 --out-csv mtf_join.csv
```

- El DataFrame resultante conserva las filas del **TF de ejecución** y agrega columnas sufijadas (`close_M5`, `close_M15`, ...).
- Join **backward**: cada vela de M1 recibe el último valor disponible del TF superior **≤ ts**.

