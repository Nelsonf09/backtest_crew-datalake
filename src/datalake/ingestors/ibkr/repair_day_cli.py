import argparse
import logging
import os
from datalake.config import LakeConfig
from datalake.ingestors.ibkr.ingest_cli import BAR_SIZES
from datalake.commands.repair_day import repair_day as repair_day_core

logger = logging.getLogger("ibkr.repair_day")


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--tf", choices=list(BAR_SIZES.keys()), default="M1")
    ap.add_argument("--exchange", default=os.getenv("IB_EXCHANGE_CRYPTO", "PAXOS"))
    ap.add_argument(
        "--what-to-show",
        dest="what",
        default=os.getenv("IB_WHAT_TO_SHOW", "AGGTRADES"),
    )
    ap.add_argument(
        "--use-rth",
        dest="use_rth",
        choices=[0, 1],
        type=int,
        default=int(os.getenv("IB_USE_RTH", "0")),
    )
    ap.add_argument("--lake-root", default=os.getenv("LAKE_ROOT", os.getcwd()))
    ap.add_argument("--allow-synth", action="store_true", help="Relleno sintético")
    ap.add_argument(
        "--log-level",
        choices=["INFO", "DEBUG"],
        default="INFO",
        help="Nivel de logging",
    )
    return ap


def repair_day(args) -> str:
    tf = args.tf
    exchange = args.exchange
    what = args.what
    rth = bool(args.use_rth)
    lake_root = args.lake_root

    cfg = LakeConfig()
    cfg.data_root = lake_root
    cfg.market = "crypto"
    cfg.timeframe = tf
    cfg.source = "ibkr"
    cfg.vendor = "ibkr"
    cfg.exchange = exchange
    cfg.what_to_show = what
    cfg.use_rth = rth
    cfg.tz = "UTC"
    cfg.logger = logger

    repair_day_core(
        symbol=args.symbol,
        date_utc=args.date,
        timeframe=tf,
        exchange=exchange,
        what_to_show=what,
        use_rth=rth,
        cfg=cfg,
    )
    return getattr(cfg, "last_dest_file", "")


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    level = getattr(logging, args.log_level.upper())
    logging.basicConfig(level=level)
    logger.setLevel(level)
    logging.getLogger("ibkr.ingest").setLevel(level)
    logging.getLogger("ibkr.downloader").setLevel(level)
    repair_day(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
