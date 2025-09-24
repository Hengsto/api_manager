# ingest/run_import.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, logging, argparse
from typing import List, Optional

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL","INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s run_import: %(message)s",
)
log = logging.getLogger("run_import")

from .pipeline import run_import

def main():
    ap = argparse.ArgumentParser(description="Unified import runner (central pipeline + per-source adapters)")
    ap.add_argument("--source", required=True, choices=["eodhd"], help="Quelle/Adapter (zunächst: eodhd)")
    ap.add_argument("--exchanges", nargs="+", required=True, help="Exchange Codes (z. B. XNAS XNYS XETR)")
    ap.add_argument("--limit", type=int, default=None, help="Max Symbole pro Exchange")
    ap.add_argument("--sleep", type=float, default=0.0, help="Pause zwischen Items")
    ap.add_argument("--dry-run", action="store_true", help="Nur loggen, nicht schreiben")
    ap.add_argument("--tags", nargs="*", default=None, help="Zusätzliche Tags für alle neuen Assets")
    ap.add_argument("--unsorted", action="store_true", help="Neue Assets als 'unsorted' markieren")
    ap.add_argument("--registry-endpoint", type=str, default=os.getenv("REGISTRY_ENDPOINT"), help="Registry API Base (optional override)")
    args = ap.parse_args()

    totals = run_import(
        source=args.source,
        exchanges=args.exchanges,
        limit=args.limit,
        sleep_sec=args.sleep,
        dry_run=args.dry_run,
        extra_tags=args.tags,
        force_unsorted=args.unsorted,
        registry_endpoint=args.registry_endpoint,
    )
    log.info(f"[EXIT] {totals}")

if __name__ == "__main__":
    main()
