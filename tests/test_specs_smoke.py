import json, pathlib

def test_schema_files_exist():
    for name in [
        'docs/specs/schema_m1.parquet.json',
        'docs/specs/schema_levels_daily.parquet.json',
        'docs/specs/partitioning.md'
    ]:
        assert pathlib.Path(name).exists()

def test_schema_m1_json_valid():
    p = pathlib.Path('docs/specs/schema_m1.parquet.json')
    d = json.loads(p.read_text())
    assert d['properties']['ts']['description'].startswith('UTC')
