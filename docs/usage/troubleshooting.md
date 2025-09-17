# Troubleshooting

- **DF vacío**: revisa que `--lake-root` apunte al **root del repo** (el reader añade `/data` internamente) y que exista `data/source=binance/.../part-YYYY-MM.parquet`.
- **Un día con +1 vela**: asegúrate de leer con `date-to = día+1` (fin exclusivo). Si concatenas días, quita duplicados por `ts`.
- **Rate limit**: ejecuta menos TFs/símbolos por corrida o aumenta `sleep`/baja `page size` si está disponible.
- **Símbolo/Región**: si en `us` no existe el par, usa `--region global`.
