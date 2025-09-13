# Fase 3 — Escritura idempotente y normalización

- **Normalización de esquema** al escribir/leer: tipos consistentes y columnas canónicas.
- **Idempotencia**: si el archivo mensual existe, `write_month` lo **lee**, mergea el nuevo bloque por `ts` y lo re-escribe sin duplicados.
- **Corrección de límites**: ingesta por **trozos intra-día** (para IB) + merge mensual ⇒ evita huecos y solapes.

### Modo sintético (sin IB)
```powershell
$env:DATALAKE_SYNTH = "1"
python -m datalake.ingestors.ibkr.ingest_cli --symbols BTC-USD --from 2025-08-01 --to 2025-08-01
```
Cuando IB está caído, el ingestor genera velas sintéticas para no interrumpir el pipeline.

### Re-ingesta del mismo día
- Seguro: actualiza o completa huecos.
- En caso de conflicto `ts`, **último gana**.
- Reintentar siempre por día completo para conservar la idempotencia.

