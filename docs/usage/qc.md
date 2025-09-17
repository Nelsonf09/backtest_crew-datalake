# QC — Validaciones de plenitud

## Conteo esperado por día
- M1: **1440**
- M5: **288**
- M15: **96**
- M30: **48**

## Script de QC mensual
```powershell
$py = @'
from datalake.read.api import read_range_df
import pandas as pd
from datetime import datetime, timedelta, timezone

LAKE   = r"C:/work/backtest_crew-datalake"
SYMBOL = "BTC-USD"; SOURCE = "binance"
YEAR, MONTH = 2025, 8
UTC = timezone.utc
start = datetime(YEAR, MONTH, 1, tzinfo=UTC)
end_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
ndays = (end_month - start).days
expected = {"M1":1440, "M5":288, "M15":96, "M30":48}

issues = []
for tf, exp in expected.items():
    for i in range(ndays):
        d0 = (start + timedelta(days=i)).date().isoformat()
        d1 = (start + timedelta(days=i+1)).date().isoformat()
        df = read_range_df(LAKE, 'crypto', tf, SYMBOL, d0, d1, SOURCE)
        if len(df) != exp:
            issues.append((tf, d0, len(df), exp))

print('QC terminado.')
if issues:
    print('Días con conteo inesperado:')
    for x in issues: print('-', x)
else:
    print('¡Todo completo!')
'@
$py | Set-Content .\tmp_qc_mes.py -Encoding UTF8
python .\tmp_qc_mes.py
```
