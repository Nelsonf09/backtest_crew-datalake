from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Literal

class ParquetOptions(BaseModel):
    compression: Literal["ZSTD", "SNAPPY"] = "ZSTD"
    bar_semantics: Literal["bar_end", "bar_start"] = "bar_end"
    timezone: str = "UTC"

class DatasetDescriptor(BaseModel):
    source: str = "ibkr"
    market: str = "crypto"
    timeframe: str = "M1"
    symbol: str = "BTC-USD"
    year: int = 2025
    month: int = 8
    path: str = "data/source=ibkr/market=crypto/timeframe=M1/symbol=BTC-USD/year=2025/month=08/part-2025-08.parquet"
    parquet: ParquetOptions = Field(default_factory=ParquetOptions)
