from __future__ import annotations

import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("ibkr.writer")

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

STR_COLS = [
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
    for c in STR_COLS:
        if c in pdf:
            pdf[c] = pdf[c].astype("string")
    if "is_synth" in pdf:
        pdf["is_synth"] = pdf["is_synth"].astype("bool")
    return pdf


def _to_string(df: pd.DataFrame) -> pd.DataFrame:
    for c in STR_COLS:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df


def _ensure_synth(df: pd.DataFrame, has_synth: bool) -> pd.DataFrame:
    if has_synth and "is_synth" not in df.columns:
        df["is_synth"] = False
    if has_synth:
        df["is_synth"] = df["is_synth"].astype(bool)
    return df


def write_month(pdf_new: pd.DataFrame, symbol: str, cfg) -> str:
    """Escribe/actualiza el parquet mensual para ``symbol`` evitando choques de tipos.

    - Lee el archivo existente como un solo parquet (no dataset) para evitar
      columnas de partición.
    - Normaliza columnas de texto y alinea ``is_synth`` en ambos DataFrames.
    - Deduplica por ``ts`` y escribe desactivando dictionary encoding.
    """
    import pathlib

    pdf_new = _ensure_metadata(pdf_new, symbol=symbol, cfg=cfg)
    pdf_new = _normalize_schema_pdf(pdf_new)

    if pdf_new is None or len(pdf_new) == 0:
        return getattr(cfg, "last_dest_file", "")

    year = int(pdf_new["ts"].dt.year.iloc[0])
    month = int(pdf_new["ts"].dt.month.iloc[0])

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

    existing_pdf: pd.DataFrame | None = None
    if dest_file.exists():
        try:
            existing_tbl = pq.ParquetFile(dest_file).read()
        except Exception:
            existing_tbl = pq.read_table(dest_file, use_legacy_dataset=True)
        existing_pdf = existing_tbl.to_pandas()

    pdf_new = _to_string(pdf_new)
    if existing_pdf is not None:
        existing_pdf = _to_string(existing_pdf)

    has_synth = ("is_synth" in pdf_new.columns) or (
        existing_pdf is not None and "is_synth" in existing_pdf.columns
    )
    pdf_new = _ensure_synth(pdf_new, has_synth)
    if existing_pdf is not None:
        existing_pdf = _ensure_synth(existing_pdf, has_synth)

    target_cols = COLS_BASE + (["is_synth"] if has_synth else [])

    for c in target_cols:
        if c not in pdf_new.columns:
            pdf_new[c] = pd.Series([None] * len(pdf_new))
    pdf_new = pdf_new[target_cols]

    if existing_pdf is not None:
        for c in target_cols:
            if c not in existing_pdf.columns:
                existing_pdf[c] = pd.Series([None] * len(existing_pdf))
        existing_pdf = existing_pdf[target_cols]

    if existing_pdf is not None and len(existing_pdf) > 0:
        merged = pd.concat([existing_pdf, pdf_new], ignore_index=True)
    else:
        merged = pdf_new.copy()

    merged["ts"] = pd.to_datetime(merged["ts"], utc=True)
    merged = merged.sort_values("ts").drop_duplicates("ts", keep="last")

    table = pa.Table.from_pandas(merged, preserve_index=False)
    pq.write_table(
        table,
        dest_file,
        compression="zstd",
        version="2.6",
        use_dictionary=False,
    )

    existing_rows = 0 if existing_pdf is None else len(existing_pdf)
    logger.debug(
        "existing=%s new=%s merged=%s ts=[%s -> %s]",
        existing_rows,
        len(pdf_new),
        len(merged),
        merged["ts"].iloc[0] if not merged.empty else None,
        merged["ts"].iloc[-1] if not merged.empty else None,
    )

    if hasattr(cfg, "__dict__"):
        cfg.last_dest_file = str(dest_file)

    return str(dest_file)

