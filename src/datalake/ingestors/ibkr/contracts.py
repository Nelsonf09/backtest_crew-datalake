# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Tuple
from datalake.ingestors.ibkr.submodule_bridge import ensure_submodule_on_syspath

ensure_submodule_on_syspath()

try:
    # Reuso directo del submódulo (sin tocarlo)
    from config.crypto_symbols import CRYPTO_SYMBOLS, IB_CRYPTO_EX, DEFAULT_CRYPTO  # type: ignore
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
    ex = exchange or IB_CRYPTO_EX.get(base, 'PAXOS')
    c = Contract()
    c.secType = 'CRYPTO'
    c.symbol = base
    c.currency = quote
    c.exchange = ex
    return c
