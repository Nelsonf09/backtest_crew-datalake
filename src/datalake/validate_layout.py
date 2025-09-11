from __future__ import annotations
import os, re
from pathlib import Path
from rich import print
from datalake.config import LakeConfig

LAYOUT_RE = re.compile(
    r"^data/source=([^/]+)/market=([^/]+)/timeframe=([^/]+)/symbol=([^/]+)/"
    r"year=([0-9]{4})/month=([0-9]{2})/part-\\5-\\6\\.parquet$"
)

def main() -> int:
    cfg = LakeConfig()
    root = Path(cfg.root).resolve()
    data_dir = root / "data"
    if not data_dir.exists():
        print("[yellow]No hay carpeta 'data' aún. (OK en Fase 0)[/yellow]")
        return 0
    errors = 0
    for p in data_dir.rglob("*.parquet"):
        rel = p.relative_to(root).as_posix()
        if not LAYOUT_RE.match(rel):
            errors += 1
            print(f"[red]Layout inválido:[/red] {rel}")
    if errors:
        print(f"[red]Archivos fuera de estándar: {errors}[/red]")
        return 2
    print("[green]Layout OK[/green]")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
