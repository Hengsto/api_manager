# registry_manager/run_import.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, logging, argparse, inspect
from pathlib import Path

# ── .env robust laden ────────────────────────────────────────────────────────
dotenv_loaded_from = None
try:
    from dotenv import load_dotenv, find_dotenv
    # 1) Suche .env vom aktuellen Arbeitsverzeichnis nach oben
    dotenv_path = find_dotenv(filename=".env", usecwd=True)
    # 2) Fallback: eine Ebene über diesem File (…/api_manager/.env)
    if not dotenv_path:
        fallback = Path(__file__).resolve().parents[1] / ".env"
        if fallback.exists():
            dotenv_path = str(fallback)
    if dotenv_path:
        load_dotenv(dotenv_path=dotenv_path)
        dotenv_loaded_from = dotenv_path
except Exception as e:
    print(f"[DBG] .env load skipped/failed: {e}")

# 3) Letzter Fallback: dein zentrales config.py lädt selbst load_dotenv()
if not os.getenv("API_KEY_EODHD"):
    try:
        import config as _cfg  # noqa: F401
        if not dotenv_loaded_from:
            dotenv_loaded_from = "via config.py (load_dotenv())"
    except Exception:
        pass

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s run_import: %(message)s",
)
log = logging.getLogger("run_import")
log.warning("RUN_IMPORT: package=registry_manager (nicht ingest)")

print(f"[DBG] .env source: {dotenv_loaded_from or 'NOT FOUND'}")
print(f"[DBG] REGISTRY_ENDPOINT={os.getenv('REGISTRY_ENDPOINT', 'N/A')}")
_api_key = os.getenv("API_KEY_EODHD", "")
print(f"[DBG] API_KEY_EODHD set={bool(_api_key)} len={(len(_api_key) if _api_key else 0)}")

# ── Imports NACH dem dotenv/Config-Load ─────────────────────────────────────
from .pipeline import run_import  # noqa: E402
import registry_manager.sources.base as _base  # noqa: E402

# Debug: sicherstellen, dass das richtige base.py geladen wird
print(f"[DBG] base.py loaded from: {inspect.getfile(_base)}")

def main():
    ap = argparse.ArgumentParser(description="Unified import runner (central pipeline + per-source adapters)")
    ap.add_argument("--source", required=True, choices=["eodhd", "binance"], help="Quelle/Adapter (z. B. eodhd oder binance)")
    ap.add_argument(
        "--exchanges",
        nargs="+",
        required=True,
        help="Exchange Codes (z. B. XNAS XNYS XETR; für Binance: BINANCE_SPOT)",
    )
    ap.add_argument("--limit", type=int, default=None, help="Max Symbole pro Exchange")
    ap.add_argument("--sleep", type=float, default=0.0, help="Pause zwischen Items")
    ap.add_argument("--dry-run", action="store_true", help="Nur loggen, nicht schreiben")
    ap.add_argument("--tags", nargs="*", default=None, help="Zusätzliche Tags für alle neuen Assets")
    ap.add_argument("--unsorted", action="store_true", help="Neue Assets als 'unsorted' markieren")
    ap.add_argument("--registry-endpoint", type=str, default=os.getenv("REGISTRY_ENDPOINT"), help="Registry API Base (optional override)")
    args = ap.parse_args()

    # Debug-Zusammenfassung
    log.debug(f"args.source={args.source} exchanges={args.exchanges} limit={args.limit} sleep={args.sleep} dry_run={args.dry_run}")
    log.info (f"Using REGISTRY_ENDPOINT={args.registry_endpoint or os.getenv('REGISTRY_ENDPOINT','N/A')}")

    if args.source == "eodhd" and not _api_key:
        print("FATAL: API_KEY_EODHD nicht gesetzt. Lege ihn in .env (im Projekt-Root) "
              "oder exportiere ihn in der Shell (z. B. $env:API_KEY_EODHD='...').")
        sys.exit(2)

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
