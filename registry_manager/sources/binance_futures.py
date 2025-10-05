# registry_manager/sources/binance_futures.py
# -*- coding: utf-8 -*-
"""Binance COIN-M Futures Adapter (statische Symboltabelle)."""
from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional

from .base import AssetDraft, Listing, SourceAdapter

log = logging.getLogger("registry_manager.binance_futures")

# ---------------------------------------------------------------------------
# Manuell gepflegte Symbol/Namen-Liste (Futures – COIN-M)
# ---------------------------------------------------------------------------
DEFAULT_SYMBOL_DEFINITIONS: Dict[str, Dict[str, str]] = {
    # COIN-M Perpetual (BTCUSD)
    "BTCUSD_F": {
        "name": "BTCUSD Futures Perpetual",
        "underlying": "BTCUSD",
        "contract_type": "perpetual",
        "settlement_asset": "BTC",
        "base_asset": "Bitcoin",
        "quote_asset": "US Dollar",
    },
    # COIN-M Quarterly rollierend (BTCUSD)
    "BTCUSD_F_3M": {
        "name": "BTCUSD Futures Quarterly",
        "underlying": "BTCUSD",
        "contract_type": "quarterly",
        "settlement_asset": "BTC",
        "base_asset": "Bitcoin",
        "quote_asset": "US Dollar",
    },
}

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def _slugify(value: str) -> str:
    cleaned = "".join(ch.lower() for ch in (value or "") if ch.isalnum())
    return f"asset:{cleaned or 'unknown'}"


def _note_for(raw: Dict[str, str]) -> str:
    ct = (raw.get("contract_type") or "").capitalize()
    sett = raw.get("settlement_asset") or ""
    und = raw.get("underlying") or ""
    parts = []
    if und:
        parts.append(und)
    if ct:
        parts.append(ct)
    if sett:
        parts.append(f"settlement={sett}")
    return " / ".join(parts) if parts else (raw.get("name") or "")


# ---------------------------------------------------------------------------
# Adapter-Implementierung
# ---------------------------------------------------------------------------

class BinanceFuturesAdapter(SourceAdapter):
    """Adapter für den Import manueller Binance-COIN-M-Futures."""

    EXCHANGE_CODE = "BINANCE_COINM"
    LISTING_SOURCE = "BINANCE_FUTURES"

    def __init__(
        self,
        symbol_whitelist: Optional[Iterable[str]] = None,
        symbol_definitions: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        defs = symbol_definitions or DEFAULT_SYMBOL_DEFINITIONS
        # Normalisiere Keys → Uppercase
        self._symbols: Dict[str, Dict[str, str]] = {
            sym.upper(): {**values, "symbol": sym.upper()} for sym, values in defs.items()
        }
        if symbol_whitelist:
            wl = {sym.upper() for sym in symbol_whitelist}
            before = len(self._symbols)
            self._symbols = {sym: data for sym, data in self._symbols.items() if sym in wl}
            log.debug("[BINANCE_FUTURES][WL] filtered symbols: %d -> %d", before, len(self._symbols))
        if not self._symbols:
            raise ValueError("BinanceFuturesAdapter: keine Symbole verfügbar (Whitelist leer?)")
        log.info("[BINANCE_FUTURES][INIT] symbols_loaded=%d", len(self._symbols))

    # ------------------------------------------------------------------
    # SourceAdapter API
    # ------------------------------------------------------------------
    def name(self) -> str:
        # Wichtig: muss dem CLI --source entsprechen
        return "binance_futures"

    def exchanges(self) -> List[Dict[str, str]]:
        return [
            {
                "Code": self.EXCHANGE_CODE,
                "Name": "Binance Futures (COIN-M)",
                "Country": "GLOBAL",
            }
        ]

    def symbols(self, exchange_code: str) -> List[Dict[str, str]]:
        code = (exchange_code or "").upper()
        if code != self.EXCHANGE_CODE:
            raise ValueError(
                f"BinanceFuturesAdapter kennt nur '{self.EXCHANGE_CODE}' als Exchange, nicht '{exchange_code}'."
            )
        data = list(self._symbols.values())
        log.info("[BINANCE_FUTURES] symbols requested for %s: n=%d", code, len(data))
        return data

    def normalize(
        self,
        exchange_code: str,
        raw: Dict[str, str],
        mic_map: Dict[str, str],
    ) -> tuple[AssetDraft, str]:
        if (exchange_code or "").upper() != self.EXCHANGE_CODE:
            raise ValueError(
                f"BinanceFuturesAdapter.normalize erwartet Exchange '{self.EXCHANGE_CODE}', erhielt '{exchange_code}'"
            )
        symbol = (raw.get("symbol") or "").upper()
        if not symbol:
            raise ValueError("BinanceFuturesAdapter.normalize: 'symbol' fehlt")

        name = raw.get("name") or symbol
        base_asset_name = raw.get("base_asset") or "Bitcoin"
        listing = Listing(
            source=self.LISTING_SOURCE,
            symbol=symbol,
            exchange=self.EXCHANGE_CODE,
            mic=(mic_map.get(self.EXCHANGE_CODE) or None),
            note=_note_for(raw) or None,
        )
        draft = AssetDraft(
            id=_slugify(f"binance-fut-{symbol}"),
            type="crypto",
            name=name,
            primary_category="Futures",   # statt generischem "Digital Asset Derivatives"
            status="active",
            country=None,
            sector=None,
            listings=[listing],
            tags=[],                       # leer lassen
            identifiers=[{"key": "binance_futures_symbol", "value": symbol}],
        )

        match_key = symbol or raw.get("underlying") or base_asset_name
        log.debug("[BINANCE_FUTURES][NORM] symbol=%s name=%s match_key=%s", symbol, name, match_key)
        return draft, match_key


__all__ = ["BinanceFuturesAdapter", "DEFAULT_SYMBOL_DEFINITIONS"]
