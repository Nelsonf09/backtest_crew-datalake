# Fase 3 — Escritura idempotente y normalización

- **Normalización de esquema** al escribir/leer: tipos consistentes y columnas canónicas.
- **Idempotencia**: si el archivo mensual existe, se **lee**, se mergea el nuevo bloque (por `ts`) y se re-escribe.
- **Corrección de límites**: ingesta por **trozos intra-día** (para IB) + merge mensual ⇒ evita huecos y solapes.

### Re-ingesta del mismo día
- Seguro: actualiza o completa huecos.
- En caso de conflicto `ts`, **último gana**.

