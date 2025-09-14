from __future__ import annotations

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- Columnas base y normalización para escritura Parquet ---
COLS_BASE = [
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "market",
    "timeframe",
    "symbol",
    "exchange",
    "what_to_show",
    "vendor",
    "tz",
]

TEXT_COLS = [
    "source",
    "market",
    "timeframe",
    "symbol",
    "exchange",
    "what_to_show",
    "vendor",
    "tz",
]

# -- Asegurar metadatos requeridos antes del schema --
def _val(obj, *names, default=None):
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None and v != "":
                return v
    return default


def _ensure_metadata(pdf: pd.DataFrame, symbol: str, cfg) -> pd.DataFrame:
    pdf = pdf.copy()
    # Defaults desde cfg o entorno
    market = _val(cfg, "market", default="crypto")
    timeframe = _val(cfg, "timeframe", default="M1")
    source = _val(cfg, "source", "vendor", default="ibkr")
    vendor = _val(cfg, "vendor", "source", default="ibkr")
    exchange = _val(
        cfg,
        "exchange",
        "ib_exchange",
        default=os.getenv("IB_EXCHANGE_CRYPTO", "PAXOS"),
    )
    what_show = _val(
        cfg,
        "what_to_show",
        default=os.getenv("IB_WHAT_TO_SHOW", "AGGTRADES"),
    )
    tz = _val(cfg, "tz", default="UTC")

    defaults = {
        "market": market,
        "timeframe": timeframe,
        "source": source,
        "vendor": vendor,
        "symbol": str(symbol),
        "exchange": exchange,
        "what_to_show": what_show,
        "tz": tz,
    }
    for k, v in defaults.items():
        if k not in pdf.columns:
            pdf[k] = v
        else:
            # Rellena NaN con el default
            try:
                pdf[k] = pdf[k].fillna(v)
            except Exception:
                pdf[k] = v
    return pdf


def _normalize_schema_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    # tiempo en UTC bar-end
    pdf["ts"] = pd.to_datetime(pdf["ts"], utc=True)
    # numéricos
    for c in ("open", "high", "low", "close"):
        if c in pdf:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").astype("float64")
    if "volume" in pdf:
        pdf["volume"] = pd.to_numeric(pdf["volume"], errors="coerce")
    # strings planos (evitar categoricals/dictionary)
    for c in TEXT_COLS:
        if c in pdf:
            pdf[c] = pdf[c].astype("string")
    if "is_synth" in pdf:
        pdf["is_synth"] = pdf["is_synth"].astype("bool")
    return pdf


def write_month(pdf: pd.DataFrame, symbol: str, cfg) -> str:
    """Escribe/actualiza el part-YYYY-MM.parquet forzando schema consistente
    (strings planos, sin dictionary). Devuelve la ruta del archivo escrito.
    """
    import pathlib

    # Asegura metadatos y normaliza tipos antes de operar
    pdf_new = _ensure_metadata(pdf, symbol=symbol, cfg=cfg)
    pdf_new = _normalize_schema_pdf(pdf_new)

    # Determina año/mes a partir de ts UTC
    year = int(pd.Series(pdf_new["ts"]).dt.year.mode().iloc[0])
    month = int(pd.Series(pdf_new["ts"]).dt.month.mode().iloc[0])

    data_root = getattr(cfg, "data_root", getattr(cfg, "root", "."))
    market = getattr(cfg, "market", "crypto")
    timeframe = getattr(cfg, "timeframe", "M1")
    base = (
        pathlib.Path(data_root)
        / "data"
        / "source=ibkr"
        / f"market={market}"
        / f"timeframe={timeframe}"
        / f"symbol={symbol}"
        / f"year={year}"
        / f"month={month:02d}"
    )
    base.mkdir(parents=True, exist_ok=True)
    dest_file = base / f"part-{year}-{month:02d}.parquet"
    existing_pdf = pd.DataFrame()
    if dest_file.exists():
        existing_tbl = pq.ParquetFile(dest_file).read()
        existing_pdf = existing_tbl.to_pandas(types_mapper=pd.ArrowDtype)
        existing_pdf = _normalize_schema_pdf(existing_pdf)

    # Alinea columnas opcionales como 'is_synth'
    has_synth = ("is_synth" in pdf_new.columns) or ("is_synth" in existing_pdf.columns)
    if has_synth:
        if "is_synth" not in pdf_new.columns:
            pdf_new["is_synth"] = False
        if "is_synth" not in existing_pdf.columns:
            existing_pdf["is_synth"] = False
        pdf_new["is_synth"] = pdf_new["is_synth"].astype("bool")
        existing_pdf["is_synth"] = existing_pdf["is_synth"].astype("bool")
        cols = COLS_BASE + ["is_synth"]
    else:
        if "is_synth" in pdf_new.columns:
            pdf_new = pdf_new.drop(columns=["is_synth"])
        if "is_synth" in existing_pdf.columns:
            existing_pdf = existing_pdf.drop(columns=["is_synth"])
        cols = COLS_BASE

    if not existing_pdf.empty:
        pdf_all = pd.concat([existing_pdf[cols], pdf_new[cols]], ignore_index=True)
    else:
        pdf_all = pdf_new[cols]

    pdf_all = pdf_all.drop_duplicates(subset=["ts"]).sort_values("ts")
    out_tbl = pa.Table.from_pandas(pdf_all[cols], preserve_index=False)

    # Escribe SIN dictionary encoding para strings (evita futuros conflictos)
    pq.write_table(out_tbl, dest_file, compression="zstd", use_dictionary=False)
    return str(dest_file)

