from __future__ import annotations
import pandas as pd

# Forzamos semántica bar_end: ts = start + 1min en M1

def to_bar_end_utc(df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
    if date_col not in df.columns:
        raise ValueError(f"Columna {date_col} no encontrada")
    ts = pd.to_datetime(df[date_col], utc=True, errors='coerce')
    # IB suele dar inicio de barra; llevamos a fin de barra M1
    ts = ts + pd.Timedelta(minutes=1)
    out = df.copy()
    out['ts'] = ts
    # orden, sin duplicados, índice limpio
    out = (out.sort_values('ts')
              .drop_duplicates('ts', keep='last')
              .reset_index(drop=True))
    return out


def enforce_m1_grid(df: pd.DataFrame) -> pd.DataFrame:
    # Asumimos df['ts'] ya UTC y bar_end
    return df.sort_values('ts').reset_index(drop=True)
