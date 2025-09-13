# Troubleshooting

- **FutureWarning ('H' deprecado)**: usa `1h` en vez de `1H` en reglas de resample.
- **DataFrames vacíos**: verifica rutas (`LAKE_ROOT`) y que existan `part-YYYY-MM.parquet` en el rango.
- **Tipos Arrow al leer**: el lector normaliza esquema con `enforce_schema` para evitar conflictos.
- **Timezones**: `ts` se maneja en UTC; al filtrar usa siempre UTC.
