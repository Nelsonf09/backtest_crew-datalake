# Quickstart (Windows PowerShell)

```powershell
git clone https://github.com/Nelsonf09/backtest_crew-datalake.git
cd backtest_crew-datalake

git fetch origin
git checkout phase-4
python -m venv .venv
..\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

$env:LAKE_ROOT = "C:\work\backtest_crew-datalake"
```

> En Linux/Mac usa `source .venv/bin/activate` y variables de entorno POSIX.
