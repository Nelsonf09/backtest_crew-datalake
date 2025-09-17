#!/usr/bin/env python3
"""
Llenado de granja de datos (source=binance) para un mes completo y múltiples TFs.
- Itera días del mes YYYY-MM.
- Reusa el ingestor interno de Binance (que ya pagina y escribe con dedupe).
- Respeta límites con pacing conservador por minuto.

Uso básico (PowerShell):

  python tools/fill_binance_month.py \
    --symbols BTC-USD \
    --month 2025-08 \
    --tfs M1,M5,M15,M30 \
    --region global

Puedes ajustar:
  --sleep-per-call 0.2   (segundos entre "unidades planificadas" de request)
  --max-weight-per-minute 5000  (budget conservador; Spot ~6000/min en binance.com)
  --dry-run

Nota: El ingestor escribe bajo ./data/source=binance/...
"""
from __future__ import annotations
import argparse
import calendar
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from types import SimpleNamespace

# Importamos el orquestador de escritura que ya existe en la repo
try:
    from datalake.ingestors.binance.ingest_cli import ingest  # type: ignore
except Exception as e:
    print("ERROR: No se pudo importar datalake.ingestors.binance.ingest_cli.ingest", e, file=sys.stderr)
    sys.exit(2)

UTC = timezone.utc

TF_EXPECTED = {
    "M1": 1440,
    "M5": 288,
    "M15": 96,
    "M30": 48,
}

# Aproximación de requests por día según TF (Spot /api/v3/klines limit=1000)
# Esto es para pacing de rate limit a nivel orquestador (el ingestor ya pagina internamente)
TF_REQS_PER_DAY = {
    "M1": 2,   # 1440/1000 => 2 requests
    "M5": 1,   # 288 <= 1000
    "M15": 1,  # 96 <= 1000
    "M30": 1,  # 48 <= 1000
}

@dataclass
class Budget:
    max_weight_per_minute: int = 5000  # conservador (binance.com suele ser 6000)
    used_weight: int = 0
    window_start: float = time.time()

    def maybe_wait(self, planned_reqs: int, weight_per_req: int = 2, sleep_per_call: float = 0.2):
        """Control de budget por minuto (ventana deslizante). Si vamos a exceder,
        dormimos hasta que reinicie la ventana. Además paceamos con pequeños sleeps por request planificada.
        """
        now = time.time()
        # Reinicia ventana si pasó 1 minuto
        if now - self.window_start >= 60:
            self.window_start = now
            self.used_weight = 0
        planned_weight = planned_reqs * weight_per_req
        if self.used_weight + planned_weight > self.max_weight_per_minute:
            # Esperar al comienzo de la próxima ventana
            wait_s = 60 - (now - self.window_start)
            if wait_s > 0:
                print(f"[RATE] Esperando {wait_s:.1f}s para no exceder budget/min...")
                time.sleep(wait_s)
            # Reinicia
            self.window_start = time.time()
            self.used_weight = 0
        # Pacing suave por cada req planificada
        for _ in range(planned_reqs):
            time.sleep(max(0.0, sleep_per_call))
            self.used_weight += weight_per_req


def iter_days_of_month(year: int, month: int):
    ndays = calendar.monthrange(year, month)[1]
    for d in range(1, ndays + 1):
        yield date(year, month, d)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Llenar granja (source=binance) por mes/TF con pacing")
    ap.add_argument("--symbols", required=True, help="Lista separada por comas, ej.: BTC-USD,ETH-USD")
    ap.add_argument("--month", required=True, help="YYYY-MM (UTC)")
    ap.add_argument("--tfs", default="M1,M5,M15,M30", help="TFs separados por coma; soportados: M1,M5,M15,M30")
    ap.add_argument("--region", choices=["global","us"], default="global", help="Binance región")
    ap.add_argument("--sleep-per-call", type=float, default=0.2, help="Pausa (s) por request planificada")
    ap.add_argument("--max-weight-per-minute", type=int, default=5000, help="Budget de weight/min (conservador)")
    ap.add_argument("--dry-run", action="store_true", help="No ingesta, solo plan y pacing")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    try:
        year, month = map(int, args.month.split("-"))
        assert 1 <= month <= 12
    except Exception:
        print("--month debe ser YYYY-MM", file=sys.stderr)
        return 2

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    tfs = [t.strip().upper() for t in args.tfs.split(",") if t.strip()]
    for t in tfs:
        if t not in TF_EXPECTED:
            print(f"TF no soportado: {t}", file=sys.stderr)
            return 2

    budget = Budget(max_weight_per_minute=args.max_weight_per_minute)

    total_days = calendar.monthrange(year, month)[1]
    print(f"Plan: symbols={symbols} month={args.month} tfs={tfs} region={args.region} days={total_days}")
    print(f"Pacing: sleep_per_call={args.sleep_per_call}s, max_weight/min={args.max_weight_per_minute}")

    # Recorremos símbolo → TF → días
    for sym in symbols:
        for tf in tfs:
            planned_reqs_per_day = TF_REQS_PER_DAY[tf]
            exp_rows = TF_EXPECTED[tf]
            print(f"\n=== {sym} | {tf} | {args.month} ===")
            for d in iter_days_of_month(year, month):
                day_str = d.isoformat()
                # Pacing conservador antes de lanzar la ingesta del día
                budget.maybe_wait(planned_reqs=planned_reqs_per_day, sleep_per_call=args.sleep_per_call)

                print(f"[RUN] {sym} {tf} {day_str} (esperado {exp_rows} filas)")
                if args.dry_run:
                    continue
                # Construimos args para el ingestor interno de Binance (que ya pagina y escribe)
                ns = SimpleNamespace(
                    symbols=sym,
                    date_from=day_str,
                    date_to=day_str,
                    tf=tf,
                    binance_region=args.region,
                )
                try:
                    ingest(ns)  # escribe al lake y loguea warn si filas!=esperado
                except SystemExit as se:
                    # por si el ingest hace SystemExit
                    if int(se.code) != 0:
                        print(f"[ERROR] ingest SystemExit code={se.code} en {sym} {tf} {day_str}", file=sys.stderr)
                except Exception as e:
                    print(f"[ERROR] ingest falló en {sym} {tf} {day_str}: {e}", file=sys.stderr)
                    # seguimos con el siguiente día sin abortar todo
                    continue

    print("\nOK: proceso completado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
