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

### IB Error 321
Si IB devuelve `Error 321` por `duration format`, revisa la versión del ingestor.
Las solicitudes ahora expresan la duración en segundos (`"{N} S"`) o usan
`"1 D"` para ventanas diarias de M1, evitando la unidad `H`.

## CRYPTO M1
Las descargas M1 de criptomonedas se dividen en **tres solicitudes de 8 horas exactas**:

- 00:00→07:59 (endDateTime=08:00:00 UTC, duration=28800 S)
- 08:00→15:59 (endDateTime=16:00:00 UTC, duration=28800 S)
- 16:00→23:59 (endDateTime=00:00:00 UTC del día siguiente, duration=28800 S)

El `endDateTime` se construye como `end + 1min` y la duración se expresa en
segundos, evitando perder el último minuto del tramo.

### Troubleshooting
Si falta el bloque 20:00→23:59, revisar los logs `REQ [3/3]` y confirmar
`endDateTime` y `duration`.

