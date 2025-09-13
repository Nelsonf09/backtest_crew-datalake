# Fase 4 — CLI endurecida y recetas end-to-end

## Cambios clave
- CLI con argumentos completos, validaciones y logs más verbosos.
- Agregador reforzado contra gaps y duplicados.

## Receta end-to-end
```powershell
# Ingesta real
python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD --from 2025-08-01 --to 2025-08-01
# o modo sintético
$env:DATALAKE_SYNTH = "1"
python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD --from 2025-08-01 --to 2025-08-01
# Agregados M5/M15/H1
python .\\tools\\resample_from_m1.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-01 --to-tf M5,M15,H1
# Validaciones
python .\\tools\\check_day.py  --symbol BTC-USD --date 2025-08-01 --lake-root $env:LAKE_ROOT
python .\\tools\\check_mtf.py  --symbol BTC-USD --date 2025-08-01 --tf M5 --lake-root $env:LAKE_ROOT
```

## Buenas prácticas
- Reintentos por día completo, nunca parciales.
- Verificar huecos con `check_day.py` antes de resample.
- Las operaciones son idempotentes: reingestión y resample no generan duplicados.

