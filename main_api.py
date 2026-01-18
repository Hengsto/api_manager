# main_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import tempfile
import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import config as cfg


# ── sys.path primen ──────────────────────────────────────────────────────────
def _prime_sys_path() -> None:
    try:
        _cwd = str(Path.cwd().resolve())
        _here = str(Path(__file__).resolve().parent)
        for _p in (_cwd, _here):
            if _p not in sys.path:
                sys.path.insert(0, _p)
        print(f"[DEBUG] sys.path primed: cwd={_cwd}, here={_here}")
    except Exception as e:
        print(f"[DEBUG] sys.path priming failed: {e}")


_prime_sys_path()

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

# ── Registry (optional) ─────────────────────────────────────────────────────
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
                raise RuntimeError("'app' nicht gefunden")
            print(f"[DEBUG] registry_api via Datei geladen: {file_path}")
            return app_obj
        raise RuntimeError("spec_from_file_location lieferte kein valides spec/loader")
    except Exception as e:
        _import_errs.append(f"{file_path}: {e}")
        return None


try:
    from api.registry_api import app as registry_app  # type: ignore
    print("[DEBUG] registry_api gefunden (api.registry_api) → /registry wird gemountet.")
except Exception as e1:
    _import_errs.append(f"api.registry_api: {e1}")
    try:
        from registry_api import app as registry_app  # type: ignore
        print("[DEBUG] registry_api gefunden (registry_api) → /registry wird gemountet.")
    except Exception as e2:
        _import_errs.append(f"registry_api: {e2}")
        reg_file = os.getenv("REGISTRY_API_FILE", "").strip()
        if reg_file:
            registry_app = _import_registry_from_file(reg_file)

if registry_app is None and _import_errs:
    print("⚠️ [DEBUG] registry_api nicht importierbar. Details:")
    for _line in _import_errs:
        print("   ↳", _line)

# ── Indicators Proxy ────────────────────────────────────────────────────────
ind_router = None
try:
    from api.indicators_api import router as ind_router  # type: ignore
    print("[DEBUG] indicators_api Router eingebunden.")
except Exception as e:
    print(f"[DEBUG] kein indicators_api Router (optional). {e}")


# ── Utility ─────────────────────────────────────────────────────────────────
def _apply_cors(app: FastAPI) -> None:
    raw = os.getenv("NOTIFIER_CORS_ORIGINS") or os.getenv("REGISTRY_CORS_ORIGINS") or os.getenv("IND_PROXY_CORS_ORIGINS") or "*"
    if raw.strip() == "*":
        origins = ["*"]
    else:
        origins = [o.strip() for o in raw.split(",") if o.strip()]

    allow_credentials = not (len(origins) == 1 and origins[0] == "*")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    print(f"[DEBUG] CORS origins={origins} allow_credentials={allow_credentials}")


def _print_debug_paths() -> None:
    try:
        print(f"[DEBUG] cfg_loaded_from={Path(cfg.__file__).resolve()}")
        print(f"[DEBUG] DATA_DIR={getattr(cfg, 'DATA_DIR', None)}")
        print(f"[DEBUG] NOTIFIER_DATA_DIR={getattr(cfg, 'NOTIFIER_DATA_DIR', None)}")
        print(f"[DEBUG] EVALUATOR_DATA_DIR={getattr(cfg, 'EVALUATOR_DATA_DIR', None)}")
        print(f"[DEBUG] PROFILES_NOTIFIER={getattr(cfg, 'PROFILES_NOTIFIER', None)}")
        print(f"[DEBUG] ALARMS_NOTIFIER={getattr(cfg, 'ALARMS_NOTIFIER', None)}")

        profiles_path = Path(getattr(cfg, "PROFILES_NOTIFIER"))
        alarms_path = Path(getattr(cfg, "ALARMS_NOTIFIER"))
        print(f"[DEBUG] Profiles path: {profiles_path}")
        print(f"[DEBUG] Alarms   path: {alarms_path}")

        cwd = Path.cwd().resolve()
        if str(profiles_path.resolve()).startswith(str(cwd)) or str(alarms_path.resolve()).startswith(str(cwd)):
            print("⚠️ WARN: JSONs liegen im Projektbaum → Hot-Reload-Risiko.")

        lock_dir = getattr(cfg, "LOCK_DIR", None) or (Path(tempfile.gettempdir()) / "notifier_locks")
        Path(lock_dir).mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("NOTIFIER_LOCK_DIR", str(lock_dir))
        print(f"[DEBUG] Lock dir: {lock_dir}")
    except Exception as e:
        print(f"[DEBUG] Pfad-Debugging fehlgeschlagen: {e}")


# ── FastAPI App ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[DEBUG] lifespan startup (API ONLY, evaluator NOT started here)")
    _print_debug_paths()
    yield
    print("[DEBUG] shutdown")


app = FastAPI(title="API (Notifier + Registry + Indicators Proxy)", lifespan=lifespan)
_apply_cors(app)

# ── Mounts / Routers ─────────────────────────────────────────────────────────
if registry_app:
    app.mount("/registry", registry_app)
    print("[DEBUG] Registry gemountet: /registry")
else:
    print("[DEBUG] Registry NICHT gemountet (optional).")

if ind_router:
    app.include_router(ind_router)
    print("[DEBUG] Indicators Router eingebunden.")
else:
    print("[DEBUG] Indicators Router NICHT eingebunden (optional).")

if notifier_router:
    app.include_router(notifier_router, prefix="/notifier")
    print("[DEBUG] Notifier Router eingebunden: /notifier")
else:
    print("[DEBUG] ❌ Notifier Router fehlt → /notifier/* nicht verfügbar.")

if alarms_router:
    app.include_router(alarms_router, prefix="/alarms")
    print("[DEBUG] Alarms Router eingebunden: /alarms")
else:
    print("[DEBUG] Alarms Router NICHT eingebunden (optional).")

# ── Endpoints ───────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "ok": True,
        "mode": "api_only",
        "see": ["/health", "/notifier/health", "/registry/health", "/indicators"],
        "registry_mounted": registry_app is not None,
        "indicators_router": ind_router is not None,
        "notifier_router": notifier_router is not None,
        "alarms_router": alarms_router is not None,
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "mode": "api_only",
        "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
    }


if __name__ == "__main__":
    host = os.getenv("MAIN_IP", "127.0.0.1")
    port = int(os.getenv("PORT", "8098"))
    print(f"[DEBUG] uvicorn.run host={host} port={port}")
    uvicorn.run("main_api:app", host=host, port=port, reload=False, log_level="info")
