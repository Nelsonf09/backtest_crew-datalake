from __future__ import annotations

import pandas as pd


def repair_day(symbol: str, date_utc: str, timeframe: str, exchange: str, what_to_show: str, cfg) -> None:
    from datalake.ingestors.ibkr.downloader import fetch_bars_range, bars_to_df
    from datalake.ingestors.ibkr.writer import write_month
    from datalake.tools.gaps import find_missing_ranges_utc

    day = pd.to_datetime(date_utc).tz_localize('UTC').normalize()
    day_start = day
    day_end = day + pd.Timedelta(hours=23, minutes=59)

    gaps = find_missing_ranges_utc(
        symbol=symbol,
        date_utc=day.strftime('%Y-%m-%d'),
        timeframe=timeframe,
        exchange=exchange,
        what_to_show=what_to_show,
        cfg=cfg,
    )

    if not gaps:
        if cfg and getattr(cfg, 'logger', None):
            cfg.logger.info("No hay gaps que reparar para %s %s", symbol, day.date())
        return

    all_new = []
    for (g_start, g_end) in gaps:
        g_start = pd.to_datetime(g_start).tz_convert('UTC')
        g_end = pd.to_datetime(g_end).tz_convert('UTC')
        fetch_end = g_end + pd.Timedelta(minutes=1)
        bars = fetch_bars_range(
            symbol=symbol,
            exchange=exchange,
            end_dt_utc=fetch_end,
            duration_seconds=int((fetch_end - g_start).total_seconds()),
            timeframe=timeframe,
            what_to_show=what_to_show,
        )
        df_part = bars_to_df(bars, exchange=exchange)
        df_part = df_part[(df_part['ts'] >= g_start) & (df_part['ts'] <= g_end)]
        all_new.append(df_part)

    if not all_new:
        if cfg and getattr(cfg, 'logger', None):
            cfg.logger.warning("Descarga de gaps devolvió vacío. Nada que escribir.")
        return

    df_gap_only = pd.concat(all_new, ignore_index=True).sort_values('ts')

    if len(df_gap_only) and cfg and getattr(cfg, 'logger', None):
        cfg.logger.debug(
            "repair: nuevos=%d ts=[%s -> %s]",
            len(df_gap_only),
            df_gap_only['ts'].min(),
            df_gap_only['ts'].max(),
        )

    write_month(pdf_new=df_gap_only, symbol=symbol, cfg=cfg)
