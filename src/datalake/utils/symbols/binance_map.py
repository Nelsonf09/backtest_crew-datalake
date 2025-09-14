from __future__ import annotations

# Reglas simples de mapeo para Spot:
#  - 'BTC-USD' -> 'BTCUSDT'
#  - 'ETH-USD' -> 'ETHUSDT'
#  - Si termina en '-USD', lo convertimos a 'USDT'.

SPECIALS = {
    'BTC-USD': 'BTCUSDT',
    'ETH-USD': 'ETHUSDT',
}

def to_binance_symbol(symbol_logico: str) -> str:
    s = (symbol_logico or '').upper().strip()
    if s in SPECIALS:
        return SPECIALS[s]
    if '-' in s:
        base, quote = s.split('-', 1)
        # Para Spot usamos USDT por defecto cuando viene USD
        if quote == 'USD':
            quote = 'USDT'
        return f"{base}{quote}"
    # Si ya viene sin guión, se retorna tal cual
    return s
