# Perfiles de liquidez (Crypto)

Perfiles recomendados para construir **OR** en cripto:

- `daily_open_utc`: tz=`UTC`, start=`00:00`, minutes=5 — coherente 24/7.
- `us_equity_open`: tz=`America/New_York`, start=`09:30`, minutes=5 — captura flujo USA.
- `asia_open`: tz=`Asia/Tokyo`, start=`09:00`, minutes=5.

## Reglas anti-look-ahead
- `ORH/ORL` del día D **solo** se usan **después** de `or_end_utc`.
- `PDH/PDL/PDC` del día D-1 se aplican a todo el día D.
- `ts` es UTC con semántica **bar_end** (evita ambigüedad de pertenencia a OR).
