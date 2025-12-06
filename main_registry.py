# main_registry.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ── ENV / Defaults ───────────────────────────────────────────────────────────
MAIN_IP = os.getenv("MAIN_IP", "127.0.0.1")
REGISTRY_PORT = int(os.getenv("REGISTRY_PORT", "8098"))
REGISTRY_CORS_ORIGINS = os.getenv("REGISTRY_CORS_ORIGINS", "*")

# ── Registry (Sub-App laden, ohne Notifier) ──────────────────────────────────
registry_app = None
_import_err = None
try:
    # 1) Paket-Variante
    from api.registry_api import app as registry_app  # type: ignore
    print("[DEBUG] registry_api gefunden (api.registry_api).")
except Exception as e1:
    try:
        # 2) Root-Modul-Variante
        from registry_api import app as registry_app  # type: ignore
        print("[DEBUG] registry_api gefunden (registry_api).")
    except Exception as e2:
        _import_err = (e1, e2)
        registry_app = None

# ── Host-App (nur Mount + Health) ────────────────────────────────────────────
app = FastAPI(title="Registry Host", version="1.0.0")

# CORS (optional)
if REGISTRY_CORS_ORIGINS:
    allow = ["*"] if REGISTRY_CORS_ORIGINS.strip() == "*" else [
        o.strip() for o in REGISTRY_CORS_ORIGINS.split(",") if o.strip()
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    print(f"[DEBUG] CORS allow_origins={allow}")

if registry_app is not None:
    app.mount("/registry", registry_app)
    print("[DEBUG] Registry unter /registry gemountet.")
else:
    print("❌ FATAL: registry_api nicht importierbar. Versucht: api.registry_api und registry_api.")
    if _import_err:
        print(f"   ↳ Errors: { _import_err[0] }  |  { _import_err[1] }")

@app.get("/health")
def health():
    return {
        "status": "ok",
        "registry_mounted": registry_app is not None,
        "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
    }

@app.get("/")
def root():
    return {"ok": True, "see": "/registry/health"}

# ── Port-Resolver (ohne Notifier-Quatsch) ────────────────────────────────────
def _resolve_port(default: int = 8098) -> int:
    # Falls REGISTRY_ENDPOINT gesetzt, Port daraus ableiten
    try:
        endpoint = os.getenv("REGISTRY_ENDPOINT", "")
        if endpoint:
            u = urlparse(endpoint)
            if u.port:
                return int(u.port)
            if u.scheme == "http":
                return 80
            if u.scheme == "https":
                return 443
    except Exception:
        pass
    # Sonst ENV PORT oder REGISTRY_PORT oder Default
    try:
        return int(os.getenv("PORT", REGISTRY_PORT or default))
    except Exception:
        return default

if __name__ == "__main__":
    host = MAIN_IP
    port = _resolve_port(8098)
    print(f"[DEBUG] uvicorn.run host={host} port={port} (ENV REGISTRY_PORT/PORT überschreibt)")
    uvicorn.run("main_registry:app", host=host, port=port, reload=False, log_level="info")
