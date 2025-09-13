import os
from typing import Iterator, Tuple

def months_between(date_from: str, date_to: str) -> Iterator[Tuple[int, int]]:
    """Genera (año, mes) inclusivo desde date_from a date_to (YYYY-MM-DD)."""
    y0, m0 = int(date_from[:4]), int(date_from[5:7])
    y1, m1 = int(date_to[:4]), int(date_to[5:7])
    y, m = y0, m0
    while (y < y1) or (y == y1 and m <= m1):
        yield (y, m)
        m += 1
        if m == 13:
            y += 1
            m = 1

def symbol_base(lake_root: str, market: str, timeframe: str, symbol: str) -> str:
    return os.path.join(
        lake_root,
        "data",
        "source=ibkr",
        f"market={market}",
        f"timeframe={timeframe}",
        f"symbol={symbol}"
    )
