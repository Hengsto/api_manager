# registry_manager/sources/binance.py
# -*- coding: utf-8 -*-
"""Binance Spot Adapter auf Basis einer statischen Symbol-Liste."""
from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional

from .base import AssetDraft, Listing, SourceAdapter

log = logging.getLogger("registry_manager.binance")

# ---------------------------------------------------------------------------
# Manuell gepflegte Symbol/Namen-Liste
# ---------------------------------------------------------------------------
DEFAULT_SYMBOL_DEFINITIONS: Dict[str, Dict[str, str]] = {
    "BTCUSDT": {
        "name": "Bitcoin / Tether",  # Spot-Paar BTC/USDT
        "base_asset": "Bitcoin",
        "quote_asset": "Tether",
    },
    "ETHUSDT": {
        "name": "Ethereum / Tether",
        "base_asset": "Ethereum",
        "quote_asset": "Tether",
    },
    "BNBUSDT": {
        "name": "BNB / Tether",
        "base_asset": "BNB",
        "quote_asset": "Tether",
    },
    "XRPUSDT": {
        "name": "XRP / Tether",
        "base_asset": "XRP",
        "quote_asset": "Tether",
    },
    "ADAUSDT": {
        "name": "Cardano / Tether",
        "base_asset": "Cardano",
        "quote_asset": "Tether",
    },
    "SOLUSDT": {
        "name": "Solana / Tether",
        "base_asset": "Solana",
        "quote_asset": "Tether",
    },
    "DOTUSDT": {
        "name": "Polkadot / Tether",
        "base_asset": "Polkadot",
        "quote_asset": "Tether",
    },
    "DOGEUSDT": {
        "name": "Dogecoin / Tether",
        "base_asset": "Dogecoin",
        "quote_asset": "Tether",
    },
    "MATICUSDT": {
        "name": "Polygon / Tether",
        "base_asset": "Polygon",
        "quote_asset": "Tether",
    },
    "LTCUSDT": {
        "name": "Litecoin / Tether",
        "base_asset": "Litecoin",
        "quote_asset": "Tether",
    },
}

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def _slugify(value: str) -> str:
    cleaned = "".join(ch.lower() for ch in (value or "") if ch.isalnum())
    return f"asset:{cleaned or 'unknown'}"


def _note_for(raw: Dict[str, str]) -> str:
    base = raw.get("base_asset")
    quote = raw.get("quote_asset")
    if base and quote:
        return f"{base} / {quote}"
    return raw.get("name") or ""


# ---------------------------------------------------------------------------
# Adapter-Implementierung
# ---------------------------------------------------------------------------


class BinanceAdapter(SourceAdapter):
    """Adapter für den Import manueller Binance-Symbole."""

    EXCHANGE_CODE = "BINANCE_SPOT"
    LISTING_SOURCE = "BINANCE"

    def __init__(
        self,
        symbol_whitelist: Optional[Iterable[str]] = None,
        symbol_definitions: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        defs = symbol_definitions or DEFAULT_SYMBOL_DEFINITIONS
        # Normalisiere Keys → Uppercase
        self._symbols: Dict[str, Dict[str, str]] = {
            sym.upper(): {**values, "symbol": sym.upper()}
            for sym, values in defs.items()
        }
        if symbol_whitelist:
            wl = {sym.upper() for sym in symbol_whitelist}
            self._symbols = {sym: data for sym, data in self._symbols.items() if sym in wl}
        if not self._symbols:
            raise ValueError("BinanceAdapter: keine Symbole verfügbar (Whitelist leer?)")

    # ------------------------------------------------------------------
    # SourceAdapter API
    # ------------------------------------------------------------------
    def name(self) -> str:
        return "binance"

    def exchanges(self) -> List[Dict[str, str]]:
        return [
            {
                "Code": self.EXCHANGE_CODE,
                "Name": "Binance Spot",
                "Country": "GLOBAL",
            }
        ]

    def symbols(self, exchange_code: str) -> List[Dict[str, str]]:
        code = (exchange_code or "").upper()
        if code != self.EXCHANGE_CODE:
            raise ValueError(
                f"BinanceAdapter kennt nur '{self.EXCHANGE_CODE}' als Exchange, nicht '{exchange_code}'."
            )
        data = list(self._symbols.values())
        log.info(f"[BINANCE] symbols requested for {code}: n={len(data)}")
        return data

    def normalize(
        self,
        exchange_code: str,
        raw: Dict[str, str],
        mic_map: Dict[str, str],
    ) -> tuple[AssetDraft, str]:
        if (exchange_code or "").upper() != self.EXCHANGE_CODE:
            raise ValueError(
                f"BinanceAdapter.normalize erwartet Exchange '{self.EXCHANGE_CODE}', erhielt '{exchange_code}'"
            )
        symbol = (raw.get("symbol") or "").upper()
        if not symbol:
            raise ValueError("BinanceAdapter.normalize: 'symbol' fehlt")

        name = raw.get("name") or symbol
        base_asset_name = raw.get("base_asset") or symbol
        listing = Listing(
            source=self.LISTING_SOURCE,
            symbol=symbol,
            exchange=self.EXCHANGE_CODE,
            mic=(mic_map.get(self.EXCHANGE_CODE) or None),
            note=_note_for(raw) or None,
        )
        draft = AssetDraft(
            id=_slugify(f"binance-{symbol}"),
            type="crypto",
            name=name,
            primary_category="Digital Assets",
            status="active",
            country=None,
            sector=None,
            listings=[listing],
            tags=["binance"],
            identifiers=[{"key": "binance_symbol", "value": symbol}],
        )
        match_key = symbol or base_asset_name
        return draft, match_key


__all__ = ["BinanceAdapter", "DEFAULT_SYMBOL_DEFINITIONS"]
