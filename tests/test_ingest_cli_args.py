import os
import pandas as pd

from datalake.ingestors.ibkr import ingest_cli


def test_argparse_and_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv("DATALAKE_SYNTH", "1")
    # ensure defaults from env are used when flags omitted
    monkeypatch.setenv("IB_EXCHANGE_CRYPTO", "PAXOS")
    monkeypatch.delenv("IB_WHAT_TO_SHOW", raising=False)

    parser = ingest_cli._build_parser()
    args = parser.parse_args([
        "--symbols",
        "BTC-USD",
        "--from",
        "2024-01-01",
        "--to",
        "2024-01-01",
    ])
    assert args.tf == "M1"
    assert args.exchange == "PAXOS"
    assert args.what is None
    assert args.use_rth is None

    paths = ingest_cli.ingest(args, data_root=str(tmp_path))
    assert paths and paths[0].endswith(".parquet")
    df = pd.read_parquet(paths[0])
    assert "timeframe" in df.columns and "symbol" in df.columns
    assert (df["timeframe"] == "M1").all()
    assert (df["symbol"] == "BTC-USD").all()
    assert (df["what_to_show"] == "TRADES").all()
