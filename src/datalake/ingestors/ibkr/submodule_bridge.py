# -*- coding: utf-8 -*-
"""Bridge para importar módulos desde vendor/backtest_crew sin tocar el original.
Uso:
    from datalake.ingestors.ibkr.submodule_bridge import ensure_submodule_on_syspath
    ensure_submodule_on_syspath()
    from config.crypto_symbols import CRYPTO_SYMBOLS
"""
from __future__ import annotations
import sys
from pathlib import Path

_ADDED = False

def ensure_submodule_on_syspath() -> None:
    """Inserta vendor/backtest_crew en sys.path si no existe ya."""
    global _ADDED
    if _ADDED:
        return
    here = Path(__file__).resolve()
    # .../backtest_crew-datalake/src/datalake/ingestors/ibkr/submodule_bridge.py
    repo_root = here.parents[4]
    sub = repo_root / 'vendor' / 'backtest_crew'
    if not sub.exists():
        raise RuntimeError("Submódulo vendor/backtest_crew no encontrado. Ejecuta 'git submodule update --init --recursive'.")
    sub_path = str(sub)
    if sub_path not in sys.path:
        sys.path.insert(0, sub_path)
    _ADDED = True
