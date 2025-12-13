# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import threading
import time
import tempfile
import importlib.util
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import config as cfg

# ── sys.path primen ──────────────────────────────────────────────────────────
try:
    _cwd = str(Path.cwd().resolve())
    _here = str(Path(__file__).resolve().parent)
    for _p in (_cwd, _here):
        if _p not in sys.path:
            sys.path.insert(0, _p)
    print(f"[DEBUG] sys.path primed: cwd={_cwd}, here={_here}")
except Exception as e:
    print(f"[DEBUG] sys.path priming failed: {e}")

# ── Optional: Router laden ───────────────────────────────────────────────────
notifier_router = None
alarms_router = None
try:
    from api.notifier_api import router as notifier_router  # type: ignore
    print("[DEBUG] notifier_api gefunden → /notifier wird gemountet.")
except Exception as e:
    print(f"[DEBUG] kein notifier_api gefunden (optional). {e}")

try:
    from api.alarms_api import router as alarms_router  # type: ignore
    print("[DEBUG] alarms_api gefunden → /alarms wird gemountet.")
except Exception as e:
    print(f"[DEBUG] kein alarms_api gefunden (optional). {e}")

# ── Registry (optional mounten; 3-fach Fallback) ─────────────────────────────
registry_app = None
_import_errs: list[str] = []

def _import_registry_from_file(file_path: str):
    try:
        file_path = str(Path(file_path).resolve())
        spec = importlib.util.spec_from_file_location("registry_api_from_file", file_path)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            app_obj = getattr(mod, "app", None)
            if app_obj is None:
                raise RuntimeError(f"'app' nicht in Modul aus Datei: {file_path}")
            print(f"[DEBUG] registry_api via Datei geladen: {file_path}")
            return app_obj
        raise RuntimeError("spec_from_file_location lieferte kein valides spec/loader")
    except Exception as e:
        _import_errs.append(f"FILE:{file_path} -> {e}")
        return None

try:
    from api.registry_api import app as registry_app  # type: ignore
    print("[DEBUG] registry_api gefunden (api.registry_api) → /registry wird gemountet.")
except Exception as e1:
    _import_errs.append(f"api.registry_api -> {e1}")
    try:
        from registry_api import app as registry_app  # type: ignore
        print("[DEBUG] registry_api gefunden (registry_api) → /registry wird gemountet.")
    except Exception as e2:
        _import_errs.append(f"registry_api -> {e2}")
        _reg_file = os.getenv("REGISTRY_API_FILE", "").strip()
        if _reg_file:
            registry_app = _import_registry_from_file(_reg_file)
        if registry_app is None:
            print("❌ [DEBUG] registry_api nicht importierbar. Versucht: api.registry_api, registry_api, REGISTRY_API_FILE.")
            for _line in _import_errs:
                print("   ↳", _line)

# ── Indicators-Proxy als ROUTER (keine Pfadänderung) ────────────────────────
ind_router = None
try:
    # WICHTIG: indicators_api MUSS einen APIRouter namens `router` exportieren
    from api.indicators_api import router as ind_router  # type: ignore
    print("[DEBUG] indicators_api Router gefunden → Routen werden 1:1 eingebunden (kein Prefix).")
except Exception as e:
    print(f"[DEBUG] kein indicators_api Router gefunden (optional). {e}")

# ── Notifier-Worker ─────────────────────────────────────────────────────────
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

# ── Utility ─────────────────────────────────────────────────────────────────
def _print_debug_paths() -> None:
    try:
        profiles_path = Path(getattr(cfg, "PROFILES_NOTIFIER"))
        alarms_path = Path(getattr(cfg, "ALARMS_NOTIFIER"))
        print(f"[DEBUG] Profiles path: {profiles_path}")
        print(f"[DEBUG] Alarms   path: {alarms_path}")
        cwd = Path.cwd().resolve()
        if str(profiles_path.resolve()).startswith(str(cwd)) or str(alarms_path.resolve()).startswith(str(cwd)):
            print("⚠️  WARN: JSONs liegen im Projektbaum → Hot-Reload-Risiko. Lege sie besser außerhalb ab.")
        lock_dir = getattr(cfg, "LOCK_DIR", None) or Path(tempfile.gettempdir()) / "notifier_locks"
        Path(lock_dir).mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("NOTIFIER_LOCK_DIR", str(lock_dir))
        print(f"[DEBUG] Using external lock dir: {lock_dir}")
    except Exception as e:
        print(f"[DEBUG] Pfad-Debugging fehlgeschlagen: {e}")

def _resolve_port(default: int = 8099) -> int:
    try:
        u = urlparse(getattr(cfg, "NOTIFIER_ENDPOINT", os.getenv("NOTIFIER_ENDPOINT", "")))
        if u.scheme:
            if u.port:
                return int(u.port)
            if u.scheme == "http":
                return 80
            if u.scheme == "https":
                return 443
    except Exception:
        pass
    try:
        u = urlparse(os.getenv("REGISTRY_ENDPOINT", ""))
        if u.scheme:
            if u.port:
                return int(u.port)
            if u.scheme == "http":
                return 80
            if u.scheme == "https":
                return 443
    except Exception:
        pass
    try:
        return int(os.getenv("PORT", default))
    except Exception:
        return default

def _compute_cors_origins() -> list[str]:
    raw = (
        os.getenv("NOTIFIER_CORS_ORIGINS")
        or os.getenv("REGISTRY_CORS_ORIGINS")
        or os.getenv("IND_PROXY_CORS_ORIGINS")
        or "*"
    )
    if raw.strip() == "*":
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]

def _apply_cors(app: FastAPI) -> None:
    allow = _compute_cors_origins()
    allow_credentials = not (len(allow) == 1 and allow[0] == "*")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    print(f"[DEBUG] CORS allow_origins={allow} allow_credentials={allow_credentials}")

# ── FastAPI App ─────────────────────────────────────────────────────────────
from notifier.watch_profiles import run as watch_profiles_run  # noqa: E402 (falls mypy)
from notifier.evaluator import run_check as evaluator_run_check  # noqa: E402
from notifier.alarm_checker import run_alarm_checker  # noqa: E402

from pathlib import Path  # nach oben gezogen, aber safe erneut

@asynccontextmanager
async def lifespan(app: FastAPI):
    _print_debug_paths()
    _start_profile_watcher_if_enabled()
    _start_evaluator_loop_if_enabled()
    yield
    print("[DEBUG] Unified API wird heruntergefahren.")

app = FastAPI(title="Unified API (Notifier + Registry + Indicators Proxy)", version="1.1.1", lifespan=lifespan)
_apply_cors(app)

# Registry-Subapp mounten (wenn vorhanden)
if registry_app:
    app.mount("/registry", registry_app)
    print("[DEBUG] Registry unter /registry gemountet.")
    try:
        reg_paths = [getattr(r, "path", str(r)) for r in getattr(registry_app, "routes", [])]
        print(f"[DEBUG] Registry routes (n={len(reg_paths)}):", reg_paths[:20], "…" if len(reg_paths) > 20 else "")
    except Exception as e:
        print("[DEBUG] Konnte Registry-Routen nicht auflisten:", e)
else:
    print("[DEBUG] Registry NICHT gemountet.")

# Indicators-Proxy-Router *ohne* Prefix einhängen → Pfade bleiben gleich
if ind_router:
    app.include_router(ind_router)
    print("[DEBUG] Indicators-Proxy Router eingebunden (kein Prefix). Pfade bleiben identisch.")
else:
    print("[DEBUG] Indicators-Proxy NICHT eingebunden (Router fehlt).")

# Notifier-/Alarms-Router einhängen (wenn vorhanden)
if notifier_router:
    # WICHTIG: Prefix setzen, damit /notifier/... entsteht
    app.include_router(notifier_router, prefix="/notifier")
    print("[DEBUG] Notifier-Router unter /notifier gemountet.")
else:
    print("[DEBUG] Notifier-Router NICHT eingebunden (notifier_router is None).")

if alarms_router:
    app.include_router(alarms_router, prefix="/alarms")
    print("[DEBUG] Alarms-Router unter /alarms gemountet.")
else:
    print("[DEBUG] Alarms-Router NICHT eingebunden (alarms_router is None).")

# ── Endpoints ───────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "ok": True,
        "see": ["/health", "/customs", "/custom", "/indicator", "/signal", "/registry/health"],
        "registry_mounted": registry_app is not None,
        "indicators_proxy_attached": ind_router is not None,
    }

@app.get("/health")
def health():
    return {
        "status": "ok",
        "notifier_router": notifier_router is not None,
        "alarms_router": alarms_router is not None,
        "registry_mounted": registry_app is not None,
        "indicators_proxy_attached": ind_router is not None,
        "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
    }

if __name__ == "__main__":
    host = getattr(cfg, "MAIN_IP", os.getenv("MAIN_IP", "127.0.0.1"))
    port = int(os.getenv("PORT", os.getenv("NOTIFIER_PORT", "8098")))
    print(f"[DEBUG] uvicorn.run host={host} port={port} (ENV PORT/NOTIFIER_PORT kann überschreiben)")
    uvicorn.run("main:app", host=host, port=port, reload=False, log_level="info")
