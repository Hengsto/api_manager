# ingest/base.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Dict, Any

import requests

log = logging.getLogger("ingest.base")

# ── Env Keys ────────────────────────────────────────────────────────────────
API_KEY_EODHD = os.getenv("API_KEY_EODHD", "").strip()  # ← dein Key
if not API_KEY_EODHD:
    log.warning("⚠️  Kein API_KEY_EODHD gesetzt! EODHD-Importer wird scheitern.")

# Registry Endpoint (ENV oder Config-Fallback)
REGISTRY_ENDPOINT = os.getenv("REGISTRY_ENDPOINT", "http://127.0.0.1:8099/registry").rstrip("/")

# ── Registry Client ─────────────────────────────────────────────────────────
_session = requests.Session()
_session.headers.update({"Content-Type": "application/json"})

def registry_health() -> bool:
    """Check ob Registry erreichbar ist."""
    try:
        r = _session.get(f"{REGISTRY_ENDPOINT}/health", timeout=5)
        return r.ok
    except Exception as e:
        log.error(f"[REG][HEALTH] Fehler: {e}")
        return False

def registry_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST an Registry-API mit Fehler-Log."""
    url = f"{REGISTRY_ENDPOINT}{path}"
    try:
        r = _session.post(url, json=payload, timeout=10)
        if not r.ok:
            log.error(f"[REG][POST] {url} → {r.status_code} {r.text}")
            r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"[REG][POST] Exception {url}: {e}")
        raise

def registry_get(path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """GET an Registry-API mit Fehler-Log."""
    url = f"{REGISTRY_ENDPOINT}{path}"
    try:
        r = _session.get(url, params=params, timeout=10)
        if not r.ok:
            log.error(f"[REG][GET] {url} → {r.status_code} {r.text}")
            r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"[REG][GET] Exception {url}: {e}")
        raise
