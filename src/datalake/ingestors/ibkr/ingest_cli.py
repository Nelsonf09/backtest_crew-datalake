import os
import argparse
import pandas as pd
from datetime import datetime, timezone, timedelta
from ib_insync import IB, Contract
from datalake.config import LakeConfig
from datalake.ingestors.ibkr.writer import write_month

# --- Helpers de contrato, chunking y fetch robusto (2h) ---
CHUNK_HOURS = 8
BACKFILL_SLICE_HOURS = 2


def _crypto_contract(symbol: str, exchange: str = "PAXOS") -> Contract:
    base, quote = symbol.split("-")
    return Contract(secType="CRYPTO", symbol=base, currency=quote, exchange=exchange)


def _day_chunks_utc(day_utc: datetime, chunk_hours: int = CHUNK_HOURS):
    day_utc = day_utc.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end_day = day_utc.replace(hour=23, minute=59, second=0, microsecond=0)
    chunks = []
    cur = day_utc
    while cur < end_day:
        nxt = min(cur + timedelta(hours=chunk_hours), end_day)
        chunks.append((cur, nxt))
        cur = nxt + timedelta(minutes=1)  # evitar solape de un minuto
    return chunks


def _fetch_m1_window(ib: IB, contract: Contract, start_utc: datetime, end_utc: datetime, what_to_show: str) -> pd.DataFrame:
    dfs = []
    cur_end = end_utc
    # retrocede en slices de 2h para mayor tasa de acierto en HMDS
    while cur_end >= start_utc:
        dur = f"{BACKFILL_SLICE_HOURS} H"
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=cur_end,
            durationStr=dur,
            barSizeSetting="1 min",
            whatToShow=what_to_show,
            useRTH=False,
            formatDate=2,
            keepUpToDate=False,
        )
        if bars:
            df = pd.DataFrame(b.__dict__ for b in bars)[["date", "open", "high", "low", "close", "volume"]]
            df["ts"] = pd.to_datetime(df["date"], utc=True)
            df = df.drop(columns=["date"]).sort_values("ts")
            df = df[(df["ts"] >= start_utc) & (df["ts"] <= end_utc)]
            if not df.empty:
                dfs.append(df)
        cur_end = cur_end - timedelta(hours=BACKFILL_SLICE_HOURS)
    if not dfs:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    out = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", required=True, help="Lista separada por comas. Ej: BTC-USD,ETH-USD")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD (UTC)")
    args = ap.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    d0 = datetime.fromisoformat(args.date_from).replace(tzinfo=timezone.utc)
    d1 = datetime.fromisoformat(args.date_to).replace(tzinfo=timezone.utc)

    lake_root = os.getenv("LAKE_ROOT", os.getcwd())
    exchange = os.getenv("IB_EXCHANGE_CRYPTO", "PAXOS")
    what = os.getenv("IB_WHAT_TO_SHOW", "AGGTRADES")

    cfg = LakeConfig()
    cfg.data_root = lake_root
    cfg.market = "crypto"
    cfg.timeframe = "M1"
    cfg.source = "ibkr"
    cfg.vendor = "ibkr"
    cfg.exchange = exchange
    cfg.what_to_show = what
    cfg.tz = "UTC"

    host = os.getenv("IB_HOST", "127.0.0.1")
    port = int(os.getenv("IB_PORT", "7497"))
    client_id = int(os.getenv("IB_CLIENT_ID", "1"))

    ib = IB()
    ib.connect(host, port, clientId=client_id, timeout=15)

    for sym in symbols:
        print(f"Ingestando {sym} {args.date_from}→{args.date_to}")
        cur = d0
        while cur <= d1:
            all_dfs = []
            for start_utc, end_utc in _day_chunks_utc(cur, CHUNK_HOURS):
                cont = _crypto_contract(sym, exchange=exchange)
                dfw = _fetch_m1_window(ib, cont, start_utc, end_utc, what_to_show=what)
                if not dfw.empty:
                    all_dfs.append(dfw)
            if all_dfs:
                day_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts")
                # metadatos mínimos (writer también asegura schema)
                day_df["source"] = "ibkr"
                day_df["market"] = "crypto"
                day_df["timeframe"] = "M1"
                day_df["symbol"] = sym
                day_df["exchange"] = exchange
                day_df["what_to_show"] = what
                day_df["vendor"] = "ibkr"
                day_df["tz"] = "UTC"
                # escribir (writer calcula y particiona por mes)
                path = write_month(day_df, symbol=sym, cfg=cfg)
                print(f"OK {sym} → {path}")
            else:
                print(f"WARN {sym} {cur.date()}: sin barras devueltas por IB")
            cur = (cur + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    ib.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

