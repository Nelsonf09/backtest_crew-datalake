from datalake.ingestors.ibkr.submodule_bridge import ensure_submodule_on_syspath

def test_bridge_imports():
    ensure_submodule_on_syspath()
    import importlib
    m = importlib.import_module('config.crypto_symbols')
    assert hasattr(m, 'CRYPTO_SYMBOLS')
