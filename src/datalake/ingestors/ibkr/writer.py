from __future__ import annotations

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- Schema y normalización consistentes para escritura Parquet ---
TEXT_COLS = [
    "source", "market", "timeframe", "symbol",
    "exchange", "what_to_show", "vendor", "tz"
]

SCHEMA = pa.schema([
    pa.field("ts", pa.timestamp("us", tz="UTC")),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.float64()),
    pa.field("source", pa.string()),
    pa.field("market", pa.string()),
    pa.field("timeframe", pa.string()),
    pa.field("symbol", pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("what_to_show", pa.string()),
    pa.field("vendor", pa.string()),
    pa.field("tz", pa.string())
])


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
    return pdf


def _to_table(pdf: pd.DataFrame) -> pa.Table:
    pdf = _normalize_schema_pdf(pdf)
    return pa.Table.from_pandas(pdf, schema=SCHEMA, preserve_index=False)


def write_month(pdf: pd.DataFrame, symbol: str, cfg) -> str:
    """Escribe/actualiza el part-YYYY-MM.parquet forzando schema consistente
    (strings planos, sin dictionary). Devuelve la ruta del archivo escrito.
    """
    import pathlib

    # Normaliza y determina año/mes a partir de ts UTC
    pdf = _normalize_schema_pdf(pdf)
    year = int(pd.Series(pdf["ts"]).dt.year.mode().iloc[0])
    month = int(pd.Series(pdf["ts"]).dt.month.mode().iloc[0])

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

    # Tabla nueva ya con schema consistente
    new_tbl = _to_table(pdf)

    if dest_file.exists():
        # Leer SOLO este archivo, no como dataset de carpeta
        existing_tbl = pq.ParquetFile(dest_file).read()
        # Castear al schema fuerte (convierte dictionary->string si hace falta)
        try:
            existing_tbl = existing_tbl.cast(SCHEMA)
        except Exception:
            # Si falla cast directo, baja a pandas y reconstituye
            existing_pdf = existing_tbl.to_pandas(types_mapper=pd.ArrowDtype)
            existing_pdf = _normalize_schema_pdf(existing_pdf)
            existing_tbl = pa.Table.from_pandas(existing_pdf, schema=SCHEMA, preserve_index=False)

        # Concatena en pandas para poder deduplicar por ts y ordenar
        pdf_old = existing_tbl.to_pandas(types_mapper=pd.ArrowDtype)
        pdf_new = new_tbl.to_pandas(types_mapper=pd.ArrowDtype)
        pdf_all = pd.concat([pdf_old, pdf_new], ignore_index=True)
        pdf_all = pdf_all.drop_duplicates(subset=["ts"]).sort_values("ts")
        out_tbl = pa.Table.from_pandas(pdf_all, schema=SCHEMA, preserve_index=False)
    else:
        out_tbl = new_tbl

    # Escribe SIN dictionary encoding para strings (evita futuros conflictos)
    pq.write_table(out_tbl, dest_file, compression="zstd", use_dictionary=False)
    return str(dest_file)

