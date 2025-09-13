# Herramientas (tools)

- `tools/synth_gen.py`: genera M1 sintético (OHLCV) en el layout del lake.
- `tools/resample_from_m1.py`: produce M5/M15/H1 desde M1 (usa `5min`, `15min`, `1h`).
- `tools/check_day.py`: verifica cobertura completa de un día (M1 = 1440 filas).
- `tools/check_mtf.py`: verifica conteos por día en M5/M15/H1.

## Ejemplos (Bash)
```bash
export LAKE_ROOT="/work/backtest_crew-datalake"
python tools/synth_gen.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03
python tools/resample_from_m1.py --symbol BTC-USD --from 2025-08-01 --to 2025-08-03 --to-tf M5,M15,H1
python tools/check_day.py --symbol BTC-USD --date 2025-08-01 --lake-root $LAKE_ROOT
```

