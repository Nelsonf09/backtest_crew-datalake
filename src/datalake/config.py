from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv(override=True)

@dataclass
class LakeConfig:
    root: str = os.getenv("DATA_LAKE_ROOT", "./")
    format: str = os.getenv("DATALAKE_FORMAT", "parquet").lower()
    compression: str = os.getenv("PARQUET_COMPRESSION", "ZSTD").upper()
    bar_semantics: str = os.getenv("BAR_SEMANTICS", "bar_end")
    default_tz: str = os.getenv("DEFAULT_TIMEZONE", "UTC")
    catalog_db: str = os.getenv("CATALOG_DB", "./catalog.sqlite")
    crypto_or_profile: str = os.getenv("CRYPTO_OR_PROFILE", "us_equity_open")
