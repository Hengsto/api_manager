# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import threading
import time
import tempfile
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from pathlib import Path

import uvicorn
from fastapi import FastAPI

import config as cfg

# ── Router optional laden ────────────────────────────────────────────────────
notifier_router = None
alarms_router = None
try:
    from api.notifier_api import router as notifier_router  # type: ignore
    print("[DEBUG] notifier_api gefunden → /notifier wird gemountet.")
except Exception:
    print("[DEBUG] kein notifier_api gefunden (optional).")

try:
    from api.alarms_api import router as alarms_router  # type: ignore
    print("[DEBUG] alarms_api gefunden → /alarms wird gemountet.")
except Exception:
    print("[DEBUG] kein alarms_api gefunden (optional).")

# ── Registry (optional mounten) ───────────────────────────────────────────────
registry_app = None
try:
    # Erwartet: registry_api.py im PYTHONPATH mit "app = FastAPI(...)"
    from api.registry_api import app as registry_app  # type: ignore
    print("[DEBUG] registry_api gefunden → /registry wird gemountet.")
except Exception as e:
    print(f"[DEBUG] kein registry_api gefunden (optional). {e}")

# ── Notifier-Worker (Dateiwächter & Evaluator) ───────────────────────────────
from notifier.watch_profiles import run as watch_profiles_run
from notifier.evaluator import run_check as evaluator_run_check
from notifier.alarm_checker import run_alarm_checker

def _start_profile_watcher_if_enabled() -> None:
    enabled = str(getattr(cfg, "ENABLE_PROFILE_WATCH", os.getenv("ENABLE_PROFILE_WATCH", "0"))).lower() in ("1", "true", "yes", "on")
    interval = float(getattr(cfg, "WATCH_INTERVAL", os.getenv("WATCH_INTERVAL", "1.0")))
    path = (getattr(cfg, "WATCH_PATH", os.getenv("WATCH_PATH", "")) or None)
    print(f"[WATCH] enabled={enabled} interval={interval} path={path or 'DEFAULT'}")
    if not enabled:
        return
    t = threading.Thread(target=watch_profiles_run, kwargs={"interval_sec": interval, "path_override": path}, daemon=True)
    t.start()

def _start_evaluator_loop_if_enabled() -> None:
    enabled = str(getattr(cfg, "ENABLE_EVALUATOR", os.getenv("ENABLE_EVALUATOR", "0"))).lower() in ("1", "true", "yes", "on")
    interval = float(getattr(cfg, "EVALUATOR_INTERVAL_SEC", os.getenv("EVALUATOR_INTERVAL_SEC", "60")))
    print(f"[EVAL] enabled={enabled} interval={interval}s")
    if not enabled:
        return

    def _loop():
        print("[EVAL] Worker gestartet.")
        while True:
            try:
                events = evaluator_run_check() or []
                print(f"[EVAL] run_check → {len(events)} events")
                if events:
                    try:
                        run_alarm_checker(events)
                    except Exception as e:
                        print(f"[EVAL] run_alarm_checker failed: {e}")
            except Exception as e:
                print(f"[EVAL] Loop-Fehler: {e}")
            time.sleep(max(1.0, float(interval)))

    t = threading.Thread(target=_loop, daemon=True)
    t.start()

# ── Utility: Debug-Pfade ausgeben (wie in deinem Log) ────────────────────────
def _print_debug_paths() -> None:
    try:
        profiles_path = Path(getattr(cfg, "PROFILES_NOTIFIER"))
        alarms_path = Path(getattr(cfg, "ALARMS_NOTIFIER"))
        print(f"[DEBUG] Profiles path: {profiles_path}")
        print(f"[DEBUG] Alarms   path: {alarms_path}")

        # Warnung, falls im Projektbaum (Hot-Reload-Risiko)
        cwd = Path.cwd().resolve()
        if str(profiles_path.resolve()).startswith(str(cwd)) or str(alarms_path.resolve()).startswith(str(cwd)):
            print("⚠️  WARN: JSONs liegen im Projektbaum → Hot-Reload-Risiko. Lege sie besser außerhalb ab.")

        # Externes Lock-Verzeichnis anzeigen / setzen (optional)
        lock_dir = getattr(cfg, "LOCK_DIR", None) or Path(tempfile.gettempdir()) / "notifier_locks"
        Path(lock_dir).mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("NOTIFIER_LOCK_DIR", str(lock_dir))
        print(f"[DEBUG] Using external lock dir: {lock_dir}")
    except Exception as e:
        print(f"[DEBUG] Pfad-Debugging fehlgeschlagen: {e}")

# ── FastAPI App ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    _print_debug_paths()
    _start_profile_watcher_if_enabled()
    _start_evaluator_loop_if_enabled()
    yield
    print("[DEBUG] Notifier API wird heruntergefahren.")

app = FastAPI(title="Notifier API", version="1.0.0", lifespan=lifespan)

# Registry-Subapp mounten (wenn vorhanden)
if registry_app:
    app.mount("/registry", registry_app)

if notifier_router:
    app.include_router(notifier_router)

if alarms_router:
    app.include_router(alarms_router, prefix="/alarms")

@app.get("/health")
def health():
    return {
        "status": "ok",
        "notifier_router": notifier_router is not None,
        "alarms_router": alarms_router is not None,
        "registry_mounted": registry_app is not None,
        "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
    }

def _port_from_config(default: int = 8000) -> int:
    try:
        u = urlparse(getattr(cfg, "NOTIFIER_ENDPOINT", ""))
        if u.port:
            return int(u.port)
        if u.scheme == "http":
            return 80
        if u.scheme == "https":
            return 443
    except Exception:
        pass
    return int(os.getenv("PORT", default))

if __name__ == "__main__":
    host = getattr(cfg, "MAIN_IP", "127.0.0.1")
    port = _port_from_config(8099)
    print(f"[DEBUG] uvicorn.run host={host} port={port} (PORT env override möglich)")
    uvicorn.run("main:app", host=host, port=port, reload=False, log_level="info")
