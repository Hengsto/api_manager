# ingest/registry_client.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, logging
from typing import Any, Dict, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("ingest.registry")

def _new_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET","POST","PATCH","DELETE"])
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", ad)
    s.mount("https://", ad)
    return s

class RegistryClient:
    def __init__(self, base: Optional[str]=None, timeout: float=15.0):
        self.base = (base or os.getenv("REGISTRY_ENDPOINT", "http://127.0.0.1:8099/registry")).rstrip("/")
        self.timeout = timeout
        self.http = _new_session()
        log.info(f"[REG] endpoint={self.base}")

    def _url(self, path: str) -> str:
        return self.base + path

    def health(self) -> Dict[str,Any]:
        r = self.http.get(self._url("/health"), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def search(self, q: str, limit: int=50) -> Dict[str, Any]:
        r = self.http.get(self._url("/search"), params={"q": q, "limit": limit}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def get_asset(self, asset_id: str) -> Optional[Dict[str,Any]]:
        r = self.http.get(self._url(f"/assets/{asset_id}"), timeout=self.timeout)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def create_asset(self, payload: Dict[str,Any]) -> Dict[str,Any]:
        r = self.http.post(self._url("/assets"), json=payload, timeout=self.timeout)
        if r.status_code == 409:
            raise RuntimeError("Asset already exists (409)")
        r.raise_for_status()
        return r.json()

    def add_listing(self, asset_id: str, listing: Dict[str,Any]) -> Dict[str,Any]:
        r = self.http.post(self._url(f"/assets/{asset_id}/listings"), json=listing, timeout=self.timeout)
        if r.status_code == 409:
            log.debug(f"[REG][LISTING] duplicate ignored: {asset_id} {listing}")
            return {"ok": True, "duplicate": True}
        r.raise_for_status()
        return r.json()

    def add_tag(self, asset_id: str, tag: str) -> None:
        r = self.http.post(self._url(f"/assets/{asset_id}/tags/{tag}"), timeout=self.timeout)
        if r.status_code not in (200, 204):
            r.raise_for_status()

    def upsert_identifier(self, asset_id: str, key: str, value: str) -> None:
        r = self.http.post(self._url(f"/assets/{asset_id}/identifiers"), json={"key": key, "value": value}, timeout=self.timeout)
        if r.status_code not in (200, 204):
            r.raise_for_status()
