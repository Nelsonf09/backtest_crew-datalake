# Particionado del datalake

El lake sigue la semántica `bar_end` en UTC y contratos de lectura en rangos **[from, to)**. Cada archivo Parquet respeta un
particionado jerárquico que agrupa por **fuente**, **mercado**, **timeframe**, **símbolo** y fecha calendario.

```
data/
  source={source}/
    market=crypto/
      timeframe={tf}/
        symbol={symbol}/
          year={YYYY}/
            month={MM}/
              part-{YYYY}-{MM}.parquet
```

## Convenciones
- `tf` utiliza códigos tipo `M1`, `M5`, `M15`, `M30`, etc.
- Los timestamps (`ts`) representan el final de la vela (`bar_end`) y están en UTC.
- La lectura siempre debe pedirse con rangos half-open: `from <= ts < to`.
- Las particiones no se solapan: cada archivo contiene valores de `ts` estrictamente crecientes.
- Cualquier dataset derivado (por ejemplo niveles diarios) reutiliza el mismo árbol añadiendo particiones específicas, como `level={level}`.
