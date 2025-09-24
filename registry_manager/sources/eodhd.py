# ingest/sources/eodhd.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, logging, time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass, asdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import SourceAdapter, AssetDraft, Listing

log = logging.getLogger("ingest.eodhd")

def _new_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", ad)
    s.mount("https://", ad)
    return s

def _slugify(text: str) -> str:
    s = "".join(ch.lower() for ch in (text or "") if ch.isalnum())
    return f"asset:{s or 'unknown'}"

def _classify(eod_type: str) -> Tuple[str,str]:
    t = (eod_type or "").lower()
    if "etf" in t: return "etf","ETFs"
    if "fund" in t or "mutual" in t: return "etf","Funds"
    if "index" in t: return "index","Indices"
    if "bond" in t: return "bond","Bonds"
    if "commodity" in t: return "commodity","Commodities"
    if "stock" in t or "reit" in t or "preferred" in t: return "equity","Stocks"
    return "unknown","Unsorted"

class EODHDAdapter(SourceAdapter):
    def __init__(self, base_url: Optional[str]=None, token: Optional[str]=None, timeout: float=30.0):
        self.base = (base_url or os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")).rstrip("/")
        # Neu: API_KEY_EODHD statt EODHD_API_TOKEN
        self.token = (token or os.getenv("API_KEY_EODHD", "")).strip()
        if not self.token:
            log.warning("⚠ API_KEY_EODHD fehlt. Setze ENV API_KEY_EODHD oder übergib 'token=' beim Adapter.")
        self.timeout = timeout
        self.http = _new_session()

    def name(self) -> str: return "eodhd"

    def _get(self, path: str, **params) -> Any:
        params = {**params, "api_token": self.token, "fmt": "json"}
        url = f"{self.base}{path}"
        log.debug(f"[EODHD][GET] {url} params={params}")
        r = self.http.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()


    def exchanges(self) -> Iterable[Dict[str,Any]]:
        data = self._get("/exchanges-list/")
        if not isinstance(data, list):
            raise RuntimeError("Unexpected exchanges payload")
        log.info(f"[EODHD] exchanges n={len(data)}")
        return data

    def symbols(self, exchange_code: str) -> Iterable[Dict[str,Any]]:
        data = self._get(f"/exchange-symbol-list/{exchange_code.upper()}")
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected symbols payload for {exchange_code}")
        log.info(f"[EODHD] {exchange_code} symbols n={len(data)}")
        return data

    def normalize(
        self, exchange_code: str, raw: Dict[str, Any], mic_map: Dict[str,str]
    ) -> Tuple[AssetDraft, str]:
        code = (raw.get("Code") or "").strip()
        name = (raw.get("Name") or "").strip() or code
        eod_type = (raw.get("Type") or "").strip()
        isin = (raw.get("Isin") or "").strip() or None
        country = (raw.get("Country") or raw.get("CountryISO") or "").strip() or None
        currency = (raw.get("Currency") or "").strip() or None

        a_type, cat = _classify(eod_type)
        listing = Listing(
            source="EODHD",
            symbol=code,
            exchange=exchange_code,
            mic=(mic_map.get(exchange_code.upper()) or None),
            isin=isin,
            note=f"{eod_type} {currency or ''}".strip()
        )
        draft = AssetDraft(
            id=_slugify(name or code),
            type=a_type,
            name=name,
            primary_category=cat,
            status="unsorted" if a_type=="unknown" else "active",
            country=country,
            sector=None,
            listings=[listing],
            tags=[],
            identifiers=([{"key":"isin","value":isin}] if isin else []),
        )
        # match_key: ISIN bevorzugen, sonst Symbol
        match_key = isin or code
        return draft, match_key
