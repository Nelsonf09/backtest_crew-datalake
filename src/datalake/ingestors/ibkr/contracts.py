# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Tuple
import importlib
from datalake.ingestors.ibkr.submodule_bridge import ensure_submodule_on_syspath

ensure_submodule_on_syspath()

try:
    # Cargamos módulo de símbolos del submódulo vendor.
    _cfg = importlib.import_module("config.crypto_symbols")
    CRYPTO_SYMBOLS = getattr(_cfg, "CRYPTO_SYMBOLS", [])
    DEFAULT_CRYPTO = getattr(_cfg, "DEFAULT_CRYPTO", "BTC-USD")
    IB_CRYPTO_EX = getattr(_cfg, "IB_CRYPTO_EX", {})
    DEFAULT_EXCHANGE = getattr(_cfg, "IB_CRYPTO_EXCHANGE", "PAXOS")
except Exception as e:
    raise ImportError(f"No se pudo importar config.crypto_symbols desde submódulo: {e}")

try:
    from ib_insync import Contract  # lightweight import aquí
except Exception as e:
    raise ImportError(f"Falta dependencia ib_insync en el entorno: {e}")


def split_symbol(symbol: str) -> Tuple[str, str]:
    """Convierte 'BTC-USD' o 'BTCUSD' en ('BTC','USD')."""
    s = symbol.replace(':', '-').replace('/', '-').upper()
    if '-' in s:
        base, quote = s.split('-', 1)
    else:
        # heurística simple para 3/4 letras de quote comunes
        if s.endswith('USDT'):
            base, quote = s[:-4], 'USDT'
        elif s.endswith('USD'):
            base, quote = s[:-3], 'USD'
        else:
            raise ValueError(f"No puedo inferir quote en símbolo: {symbol}")
    return base, quote


def make_crypto_contract(symbol: str, exchange: str | None = None) -> Contract:
    """Construye Contract CRYPTO para IB reutilizando el mapeo del submódulo.
    - symbol: 'BTC-USD', 'ETH-USD', etc.
    - exchange: si no se pasa, usa IB_CRYPTO_EX.get(base, 'PAXOS').
    """
    base, quote = split_symbol(symbol)
    # valida contra CRYPTO_SYMBOLS del submódulo si está disponible
    if CRYPTO_SYMBOLS and base not in CRYPTO_SYMBOLS:
        # permitir símbolos nuevos, pero avisar
        pass
    ex = exchange or IB_CRYPTO_EX.get(base, DEFAULT_EXCHANGE)
    c = Contract()
    c.secType = 'CRYPTO'
    c.symbol = base
    c.currency = quote
    c.exchange = ex
    return c
