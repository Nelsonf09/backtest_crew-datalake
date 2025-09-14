"""Inspecta una partición diaria verificando huecos y filas esperadas."""

import argparse, glob, os
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", os.getcwd()))
    ap.add_argument("--symbol", required=True, help="BTC-USD, ETH-USD, etc.")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--timeframe", default="M1")
    ap.add_argument("--market", default="crypto")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Exit code 1 si faltan minutos o rows != 1440",
    )
    args = ap.parse_args()

    base = os.path.join(
        args.lake_root,
        "data",
        "source=ibkr",
        f"market={args.market}",
        f"timeframe={args.timeframe}",
        f"symbol={args.symbol}",
    )
    yy = args.date[:4]
    mm = args.date[5:7]
    patt = [
        os.path.join(base, f"year={yy}", f"month={mm}", "*.parquet"),
        # por si la fecha cae cerca del borde de mes anterior/siguiente
        os.path.join(base, f"year={yy}", "month=*", "*.parquet"),
    ]
    files = []
    for p in patt:
        files.extend(glob.glob(p))
    if not files:
        print("No se hallaron archivos parquet para el patrón:", patt[0])
        return 1

    df = pd.concat((pd.read_parquet(f) for f in sorted(set(files))), ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    start = pd.Timestamp(args.date + " 00:00:00+00:00")
    end = pd.Timestamp(args.date + " 23:59:00+00:00")
    d = df[(df["ts"] >= start) & (df["ts"] <= end)].sort_values("ts").copy()

    print("rows:", len(d), "| range:", d["ts"].min(), "->", d["ts"].max())
    if "is_synth" in d.columns:
        synth_cnt = int(d["is_synth"].sum())
        print("synthetic_bars:", synth_cnt)
    per_hour = (
        d.set_index("ts").groupby(d["ts"].dt.hour).size().reindex(range(24), fill_value=0)
    )
    print("per_hour:")
    print(per_hour)

    full = pd.date_range(start, end, freq="1min")
    missing = full.difference(pd.DatetimeIndex(d["ts"]))
    print("missing_minutes:", len(missing))
    exit_code = 0
    if len(missing):
        ranges = []
        s = missing[0]
        prev = s
        for ts in missing[1:]:
            if ts - prev == pd.Timedelta(minutes=1):
                prev = ts
            else:
                ranges.append((s, prev))
                s = ts
                prev = ts
        ranges.append((s, prev))
        print("missing_ranges:")
        for a, b in ranges:
            print(f"  {a.isoformat()} -> {b.isoformat()}")
        print(
            "first_missing:", ranges[0][0].isoformat(),
            "last_missing:", ranges[-1][1].isoformat(),
        )
        exit_code = 1
    expected = 1440 if args.timeframe == "M1" else None
    if expected is not None and len(d) != expected:
        exit_code = 1
    if args.strict:
        return exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

