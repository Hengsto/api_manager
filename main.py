# -*- coding: utf-8 -*-
from __future__ import annotations

import os, threading
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from api.notifier_api import router as notifier_router
import config as cfg

# Watcher import (entkoppelt von API)
from notifier.watch_profiles import run as watch_run

def _start_watcher_if_enabled():
    enabled = str(getattr(cfg, "ENABLE_PROFILE_WATCH", os.getenv("ENABLE_PROFILE_WATCH","0"))).lower() in ("1","true","yes","on")
    interval = float(getattr(cfg, "WATCH_INTERVAL", os.getenv("WATCH_INTERVAL","1.0")))
    path = (getattr(cfg, "WATCH_PATH", os.getenv("WATCH_PATH","")).strip() or None)
    print(f"[WATCH] enabled={enabled} interval={interval} path={path or 'DEFAULT'}")
    if not enabled:
        return
    t = threading.Thread(target=watch_run, kwargs={"interval_sec": interval, "path_override": path}, daemon=True)
    t.start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[DEBUG] Notifier API gestartet. Routen:")
    for r in app.router.routes:
        try:
            methods = sorted(getattr(r, "methods", {"GET"}))
            path = getattr(r, "path", getattr(r, "path_format", str(r)))
            print(f"[DEBUG]  {methods} {path}")
        except Exception:
            pass
    _start_watcher_if_enabled()
    yield
    print("[DEBUG] Notifier API wird heruntergefahren.")

app = FastAPI(title="Notifier API", version="1.0.0", lifespan=lifespan)
app.include_router(notifier_router, prefix="/notifier")

@app.get("/notifier/health")
def health():
    return {"status": "ok"}

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
