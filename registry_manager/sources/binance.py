# registry_manager/sources/binance.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from .base import AssetDraft, Listing, SourceAdapter

log = logging.getLogger("registry_manager.binance")

_SAMPLE_SYMBOLS: List[Dict[str, Any]] = [
    {
        "symbol": "BTCUSDT",
        "status": "TRADING",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
    },
    {
        "symbol": "ETHUSDT",
        "status": "TRADING",
        "baseAsset": "ETH",
        "quoteAsset": "USDT",
    },
]


def _slugify_symbol(symbol: str) -> str:
    cleaned = "".join(ch for ch in (symbol or "").lower() if ch.isalnum())
    return f"asset:binance:{cleaned or 'unknown'}"


class BinanceAdapter(SourceAdapter):
    """Light-weight adapter that normalizes Binance spot market metadata."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 20.0,
        session: Optional[requests.Session] = None,
        use_sample: Optional[bool] = None,
        sample_symbols: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> None:
        self.base = (base_url or os.getenv("BINANCE_BASE_URL", "https://api.binance.com")).rstrip("/")
        self.timeout = timeout
        self.http = session or requests.Session()
        self._force_sample = use_sample
        self._sample_symbols = list(sample_symbols) if sample_symbols is not None else list(_SAMPLE_SYMBOLS)

    def name(self) -> str:
        return "binance"

    # --- helpers -----------------------------------------------------------------
    def _should_use_sample(self) -> bool:
        if self._force_sample is not None:
            return self._force_sample
        env = os.getenv("BINANCE_USE_SAMPLE_DATA", "")
        return env.lower() in {"1", "true", "yes", "sample"}

    def _get_exchange_info(self) -> Dict[str, Any]:
        url = f"{self.base}/api/v3/exchangeInfo"
        log.debug("[Binance][GET] %s", url)
        response = self.http.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    # --- contract ----------------------------------------------------------------
    def exchanges(self) -> List[Dict[str, Any]]:
        """Binance exposes a single logical exchange for spot markets."""
        return [
            {
                "Code": "BINANCE",
                "Name": "Binance",
                "Country": "GLOBAL",
                "OperatingMIC": "BINANCE",
            }
        ]

    def symbols(self, exchange_code: str) -> List[Dict[str, Any]]:
        code = (exchange_code or "").strip().upper()
        if code != "BINANCE":
            raise RuntimeError(f"Binance adapter only supports 'BINANCE' exchange, got '{exchange_code}'.")

        if self._should_use_sample():
            log.debug("[Binance] using sample symbol payload (env override)")
            return [dict(item) for item in self._sample_symbols]

        try:
            payload = self._get_exchange_info()
            symbols = payload.get("symbols", [])
            log.info("[Binance] fetched %d symbols", len(symbols))
            return symbols
        except Exception as exc:
            log.warning("[Binance] live fetch failed (%s). Falling back to sample payload.", exc)
            return [dict(item) for item in self._sample_symbols]

    def normalize(
        self, exchange_code: str, raw: Dict[str, Any], mic_map: Dict[str, str]
    ) -> Tuple[AssetDraft, str]:
        symbol = (raw.get("symbol") or "").strip().upper()
        base_asset = (raw.get("baseAsset") or "").strip().upper()
        quote_asset = (raw.get("quoteAsset") or "").strip().upper()
        status = (raw.get("status") or "").strip().upper()

        pair_name = "/".join(filter(None, [base_asset, quote_asset])) or symbol or "Unknown"
        listing_note = pair_name if pair_name != symbol else None
        exchange_code = (exchange_code or "BINANCE").upper()
        listing = Listing(
            source="BINANCE",
            symbol=symbol,
            exchange=exchange_code,
            mic=(mic_map.get(exchange_code) or None),
            note=listing_note,
        )

        draft = AssetDraft(
            id=_slugify_symbol(symbol or pair_name),
            type="crypto",
            name=pair_name,
            primary_category="Crypto",
            status="active" if status == "TRADING" else "inactive",
            country=None,
            sector=None,
            listings=[listing],
            tags=["binance"],
            identifiers=[
                *(
                    [{"key": "base_asset", "value": base_asset}] if base_asset else []
                ),
                *(
                    [{"key": "quote_asset", "value": quote_asset}] if quote_asset else []
                ),
            ],
        )
        match_key = symbol or pair_name
        return draft, match_key
