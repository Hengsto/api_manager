# registry_manager/sources/eodhd.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import SourceAdapter, AssetDraft, Listing

log = logging.getLogger("registry_manager.eodhd")

# ── HTTP Session mit Retry ───────────────────────────────────────────────────
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

# ── Utils ────────────────────────────────────────────────────────────────────
def _slugify(text: str) -> str:
    s = "".join(ch.lower() for ch in (text or "") if ch.isalnum())
    return f"asset:{s or 'unknown'}"

def _classify(eod_type: str) -> Tuple[str, str]:
    t = (eod_type or "").lower()
    if "etf" in t: return "etf", "ETFs"
    if "fund" in t or "mutual" in t: return "etf", "Funds"
    if "index" in t: return "index", "Indices"
    if "bond" in t: return "bond", "Bonds"
    if "commodity" in t: return "commodity", "Commodities"
    if "stock" in t or "reit" in t or "preferred" in t: return "equity", "Stocks"
    return "unknown", "Unsorted"

# ── Adapter ──────────────────────────────────────────────────────────────────
class EODHDAdapter(SourceAdapter):
    """
    EODHD Symbol-Importer:
      - akzeptiert MIC (z. B. XNAS), EODHD-Code (NASDAQ) oder Namen (XETRA)
      - löst automatisch auf → /exchange-symbol-list/{EODHD_CODE}
    """

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None, timeout: float = 30.0):
        self.base = (base_url or os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")).rstrip("/")
        self.token = (token or os.getenv("API_KEY_EODHD", "")).strip()
        if not self.token:
            log.warning("⚠ API_KEY_EODHD fehlt. Setze ENV API_KEY_EODHD oder übergib 'token=' beim Adapter.")
        self.timeout = timeout
        self.http = _new_session()
        self._ex_index: Optional[Dict[str, Dict[str, str]]] = None  # Cache: mapping dicts

    def name(self) -> str:
        return "eodhd"

    # --- HTTP ---
    def _get(self, path: str, **params) -> Any:
        params = {**params, "api_token": self.token, "fmt": "json"}
        url = f"{self.base}{path}"
        log.debug(f"[EODHD][GET] {url} params={params}")
        r = self.http.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    # --- Exchanges & Mapping ---
    def exchanges(self) -> Iterable[Dict[str, Any]]:
        data = self._get("/exchanges-list/")
        if not isinstance(data, list):
            raise RuntimeError("Unexpected exchanges payload")
        log.info(f"[EODHD] exchanges n={len(data)}")
        return data

    def _build_exchange_index(self) -> None:
        """Baut einen robusten Index: Code→Code, MIC→Code, Name→Code (einmalig)."""
        if self._ex_index is not None:
            return
        data = list(self.exchanges())
        code_to_code: Dict[str, str] = {}
        mic_to_code: Dict[str, str] = {}
        name_to_code: Dict[str, str] = {}

        for x in data:
            code = (x.get("Code") or x.get("Exchange") or "").strip().upper()
            # EODHD schwankt bei Feldnamen, decken wir ab:
            mic = (x.get("Mic") or x.get("MIC") or x.get("OperatingMIC") or "").strip().upper()
            name = (x.get("Name") or "").strip().upper()
            if code:
                code_to_code[code] = code
            if mic and code:
                mic_to_code[mic] = code
            if name and code:
                name_to_code[name] = code

        # ein paar sinnvolle Aliase (häufige MICs)
        aliases = {
            "XNAS": "NASDAQ",
            "XNYS": "NYSE",
            "XASE": "AMEX",
            "XETR": "XETRA",
        }
        for k, v in aliases.items():
            if v in code_to_code:
                mic_to_code.setdefault(k, v)

        self._ex_index = {
            "code_to_code": code_to_code,
            "mic_to_code": mic_to_code,
            "name_to_code": name_to_code,
        }
        log.info(
            f"[EODHD] exchange index built: codes={len(code_to_code)} "
            f"mics={len(mic_to_code)} names={len(name_to_code)}"
        )
        log.debug(f"[EODHD][IDX] sample codes: {list(code_to_code.keys())[:10]}")
        log.debug(f"[EODHD][IDX] sample mics : {list(mic_to_code.keys())[:10]}")

    def _resolve_exchange_code(self, exchange_code: str) -> str:
        """Nimmt MIC/Code/Name und liefert EODHD-Code zurück (z. B. 'NASDAQ')."""
        self._build_exchange_index()
        assert self._ex_index is not None
        q = (exchange_code or "").strip().upper()
        idx = self._ex_index

        # direkte Matches
        if q in idx["code_to_code"]:
            log.debug(f"[EODHD][RESOLVE] {q} matched code")
            return idx["code_to_code"][q]
        if q in idx["mic_to_code"]:
            log.debug(f"[EODHD][RESOLVE] {q} matched mic")
            return idx["mic_to_code"][q]
        if q in idx["name_to_code"]:
            log.debug(f"[EODHD][RESOLVE] {q} matched name")
            return idx["name_to_code"][q]

        # Heuristik: sehr häufige Schreibweisen (Fallback)
        aliases = {"XNAS": "NASDAQ", "XNYS": "NYSE", "XASE": "AMEX", "XETR": "XETRA"}
        if q in aliases:
            log.debug(f"[EODHD][RESOLVE] {q} via alias → {aliases[q]}")
            return aliases[q]

        raise RuntimeError(f"Unbekannte Exchange/MIC/Name für EODHD: '{exchange_code}'")

    # --- Symbols ---
    def symbols(self, exchange_code: str) -> Iterable[Dict[str, Any]]:
        eod_code = self._resolve_exchange_code(exchange_code)
        log.info(f"[EODHD] resolve exchange '{exchange_code}' → '{eod_code}'")
        data = self._get(f"/exchange-symbol-list/{eod_code}")
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected symbols payload for {exchange_code} (resolved={eod_code})")
        log.info(f"[EODHD] {eod_code} symbols n={len(data)} (input was '{exchange_code}')")
        return data

    # --- Normalize ---
    def normalize(
        self, exchange_code: str, raw: Dict[str, Any], mic_map: Dict[str, str]
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
            note=f"{eod_type} {currency or ''}".strip(),
        )
        draft = AssetDraft(
            id=_slugify(name or code),
            type=a_type,
            name=name,
            primary_category=cat,
            status="unsorted" if a_type == "unknown" else "active",
            country=country,
            sector=None,
            listings=[listing],
            tags=[],
            identifiers=([{"key": "isin", "value": isin}] if isin else []),
        )
        # match_key: ISIN bevorzugen, sonst Symbol
        match_key = isin or code
        return draft, match_key
