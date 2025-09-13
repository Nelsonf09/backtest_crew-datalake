# Troubleshooting

### Warnings y alias
- **FutureWarning ('H' deprecado)**: usar `1h` en vez de `1H`.

### Ingesta IBKR
- **Error 10299 / AGGTRADES**: Crypto requiere `AGGTRADES`. Ajusta `what_to_show` a `AGGTRADES` y `exchange` a `PAXOS`.
- **Timeout al conectar**: valida TWS/IBG, `IB_HOST/PORT/CLIENT_ID` y que el login esté activo.

### Lectura/escritura Parquet
- **ArrowTypeError (tipos incompatibles)**: mezcla de tipos (p. ej. `source` dict vs string). Solución: normaliza esquema (Fase 3) o re-escribe el mes con esquema canónico.
- **Faltan columnas (`timeframe`)**: asegúrate de incluir metadatos (`source, market, timeframe, symbol, exchange, what_to_show, vendor, tz`).

### MTF / Alineación
- **Columnas sufijadas faltantes**: confirma `ctx-tf` y que existan `part-YYYY-MM.parquet` para esos TF en el rango.
- **Join vacío**: revisa filtros de fechas (UTC) y que `ts` esté en UTC con tz (no naive).

