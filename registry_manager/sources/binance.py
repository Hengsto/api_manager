# registry_manager/sources/binance.py
# -*- coding: utf-8 -*-
"""Binance Spot Adapter auf Basis einer statischen Symbol-Liste."""
from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional

from .base import AssetDraft, Listing, SourceAdapter

log = logging.getLogger("registry_manager.binance")

# ---------------------------------------------------------------------------
# Manuell gepflegte Symbol/Namen-Liste (Spot)
# Hinweis:
# - Name wird möglichst lesbar gesetzt ("<Base> / <Quote>").
# - Für seltene Tokens ohne saubere Bezeichnung wird der Ticker als Name benutzt.
# - Diese Liste ist Spot-ONLY. Futures -> separater Adapter (siehe Hinweise unten).
# ---------------------------------------------------------------------------
DEFAULT_SYMBOL_DEFINITIONS: Dict[str, Dict[str, str]] = {
    # Core Bluechips
    "BTCUSDT": {"name": "Bitcoin / Tether", "base_asset": "Bitcoin", "quote_asset": "Tether"},
    "BTCUSDC": {"name": "Bitcoin / USD Coin", "base_asset": "Bitcoin", "quote_asset": "USD Coin"},
    "ETHUSDT": {"name": "Ethereum / Tether", "base_asset": "Ethereum", "quote_asset": "Tether"},
    "ETHUSDC": {"name": "Ethereum / USD Coin", "base_asset": "Ethereum", "quote_asset": "USD Coin"},
    "ETHUSD":  {"name": "Ethereum / US Dollar", "base_asset": "Ethereum", "quote_asset": "US Dollar"},
    "ETHBTC":  {"name": "Ethereum / Bitcoin", "base_asset": "Ethereum", "quote_asset": "Bitcoin"},
    "XRPUSDT": {"name": "XRP / Tether", "base_asset": "XRP", "quote_asset": "Tether"},
    "BNBUSDT": {"name": "BNB / Tether", "base_asset": "BNB", "quote_asset": "Tether"},
    "BNBBTC":  {"name": "BNB / Bitcoin", "base_asset": "BNB", "quote_asset": "Bitcoin"},
    "SOLUSDT": {"name": "Solana / Tether", "base_asset": "Solana", "quote_asset": "Tether"},
    "DOGEUSDT": {"name": "Dogecoin / Tether", "base_asset": "Dogecoin", "quote_asset": "Tether"},
    "TRXUSDT": {"name": "TRON / Tether", "base_asset": "TRON", "quote_asset": "Tether"},
    "ADAUSDT": {"name": "Cardano / Tether", "base_asset": "Cardano", "quote_asset": "Tether"},
    "XLMUSDT": {"name": "Stellar / Tether", "base_asset": "Stellar", "quote_asset": "Tether"},
    "SUIUSDT": {"name": "Sui / Tether", "base_asset": "Sui", "quote_asset": "Tether"},
    "LINKUSDT": {"name": "Chainlink / Tether", "base_asset": "Chainlink", "quote_asset": "Tether"},
    "HBARUSDT": {"name": "Hedera / Tether", "base_asset": "Hedera", "quote_asset": "Tether"},
    "BCHUSDT": {"name": "Bitcoin Cash / Tether", "base_asset": "Bitcoin Cash", "quote_asset": "Tether"},
    "AVAXUSDT": {"name": "Avalanche / Tether", "base_asset": "Avalanche", "quote_asset": "Tether"},
    "SHIBUSDT": {"name": "Shiba Inu / Tether", "base_asset": "Shiba Inu", "quote_asset": "Tether"},
    "TONUSDT": {"name": "Toncoin / Tether", "base_asset": "Toncoin", "quote_asset": "Tether"},
    "LTCUSDT": {"name": "Litecoin / Tether", "base_asset": "Litecoin", "quote_asset": "Tether"},
    "DOTUSDT": {"name": "Polkadot / Tether", "base_asset": "Polkadot", "quote_asset": "Tether"},
    "UNIUSDT": {"name": "Uniswap / Tether", "base_asset": "Uniswap", "quote_asset": "Tether"},
    "DAIUSDT": {"name": "Dai / Tether", "base_asset": "Dai", "quote_asset": "Tether"},
    "PEPEUSDT": {"name": "PEPE / Tether", "base_asset": "PEPE", "quote_asset": "Tether"},
    "AAVEUSDT": {"name": "Aave / Tether", "base_asset": "Aave", "quote_asset": "Tether"},
    "TAOUSDT": {"name": "Bittensor / Tether", "base_asset": "Bittensor", "quote_asset": "Tether"},
    "APTUSDT": {"name": "Aptos / Tether", "base_asset": "Aptos", "quote_asset": "Tether"},
    "NEARUSDT": {"name": "NEAR Protocol / Tether", "base_asset": "NEAR Protocol", "quote_asset": "Tether"},
    "ICPUSDT": {"name": "Internet Computer / Tether", "base_asset": "Internet Computer", "quote_asset": "Tether"},
    "ONDOUSDT": {"name": "Ondo / Tether", "base_asset": "Ondo", "quote_asset": "Tether"},
    "ETCUSDT": {"name": "Ethereum Classic / Tether", "base_asset": "Ethereum Classic", "quote_asset": "Tether"},
    "ALGOUSDT": {"name": "Algorand / Tether", "base_asset": "Algorand", "quote_asset": "Tether"},
    "POLUSDT": {"name": "POL / Tether", "base_asset": "POL", "quote_asset": "Tether"},
    "BONKUSDT": {"name": "BONK / Tether", "base_asset": "BONK", "quote_asset": "Tether"},
    "USD1USDT": {"name": "USD1 / Tether", "base_asset": "USD1", "quote_asset": "Tether"},
    "ENAUSDT": {"name": "Ethena / Tether", "base_asset": "Ethena", "quote_asset": "Tether"},
    "VETUSDT": {"name": "VeChain / Tether", "base_asset": "VeChain", "quote_asset": "Tether"},
    "SEIUSDT": {"name": "Sei / Tether", "base_asset": "Sei", "quote_asset": "Tether"},
    "ARBUSDT": {"name": "Arbitrum / Tether", "base_asset": "Arbitrum", "quote_asset": "Tether"},
    "RENDERUSDT": {"name": "Render / Tether", "base_asset": "Render", "quote_asset": "Tether"},
    "PENGUUSDT": {"name": "PENGU / Tether", "base_asset": "PENGU", "quote_asset": "Tether"},
    "TRUMPUSDT": {"name": "TRUMP / Tether", "base_asset": "TRUMP", "quote_asset": "Tether"},
    "WLDUSDT": {"name": "Worldcoin / Tether", "base_asset": "Worldcoin", "quote_asset": "Tether"},
    "ATOMUSDT": {"name": "Cosmos / Tether", "base_asset": "Cosmos", "quote_asset": "Tether"},
    "FILUSDT": {"name": "Filecoin / Tether", "base_asset": "Filecoin", "quote_asset": "Tether"},
    "FETUSDT": {"name": "Fetch.ai / Tether", "base_asset": "Fetch.ai", "quote_asset": "Tether"},
    "FDUSDUSDT": {"name": "First Digital USD / Tether", "base_asset": "FDUSD", "quote_asset": "Tether"},
    "JUPUSDT": {"name": "Jupiter / Tether", "base_asset": "Jupiter", "quote_asset": "Tether"},
    "QNTUSDT": {"name": "Quant / Tether", "base_asset": "Quant", "quote_asset": "Tether"},
    "TIAUSDT": {"name": "Celestia / Tether", "base_asset": "Celestia", "quote_asset": "Tether"},
    "FORMUSDT": {"name": "Formation Fi / Tether", "base_asset": "Formation Fi", "quote_asset": "Tether"},
    "INJUSDT": {"name": "Injective / Tether", "base_asset": "Injective", "quote_asset": "Tether"},
    "STXUSDT": {"name": "Stacks / Tether", "base_asset": "Stacks", "quote_asset": "Tether"},
    "OPUSDT": {"name": "Optimism / Tether", "base_asset": "Optimism", "quote_asset": "Tether"},
    "VIRTUALUSDT": {"name": "VIRTUAL / Tether", "base_asset": "VIRTUAL", "quote_asset": "Tether"},
    "WIFUSDT": {"name": "dogwifhat / Tether", "base_asset": "dogwifhat", "quote_asset": "Tether"},
    "SUSDT": {"name": "SUSDT / Tether", "base_asset": "SUSDT", "quote_asset": "Tether"},  # Sonder-Token
    "IMXUSDT": {"name": "Immutable / Tether", "base_asset": "Immutable", "quote_asset": "Tether"},
    "CRVUSDT": {"name": "Curve DAO / Tether", "base_asset": "Curve DAO", "quote_asset": "Tether"},
    "GRTUSDT": {"name": "The Graph / Tether", "base_asset": "The Graph", "quote_asset": "Tether"},
    "PAXGUSDT": {"name": "PAX Gold / Tether", "base_asset": "PAX Gold", "quote_asset": "Tether"},
    "KAIAUSDT": {"name": "Kaia / Tether", "base_asset": "Kaia", "quote_asset": "Tether"},
    "FLOKIUSDT": {"name": "FLOKI / Tether", "base_asset": "FLOKI", "quote_asset": "Tether"},
    "AUSDT": {"name": "AUSDT / Tether", "base_asset": "AUSDT", "quote_asset": "Tether"},
    "IOTAUSDT": {"name": "IOTA / Tether", "base_asset": "IOTA", "quote_asset": "Tether"},
    "NEXOUSDT": {"name": "NEXO / Tether", "base_asset": "NEXO", "quote_asset": "Tether"},
    "CAKEUSDT": {"name": "PancakeSwap / Tether", "base_asset": "PancakeSwap", "quote_asset": "Tether"},
    "ENSUSDT": {"name": "Ethereum Name Service / Tether", "base_asset": "Ethereum Name Service", "quote_asset": "Tether"},
    "THETAUSDT": {"name": "Theta Network / Tether", "base_asset": "Theta Network", "quote_asset": "Tether"},
    "JASMYUSDT": {"name": "JasmyCoin / Tether", "base_asset": "JasmyCoin", "quote_asset": "Tether"},
    "SANDUSDT": {"name": "The Sandbox / Tether", "base_asset": "The Sandbox", "quote_asset": "Tether"},
    "LDOUSDT": {"name": "Lido DAO / Tether", "base_asset": "Lido DAO", "quote_asset": "Tether"},
    "GALAUSDT": {"name": "GALA / Tether", "base_asset": "GALA", "quote_asset": "Tether"},
    "RAYUSDT": {"name": "Raydium / Tether", "base_asset": "Raydium", "quote_asset": "Tether"},
    "ZECUSDT": {"name": "Zcash / Tether", "base_asset": "Zcash", "quote_asset": "Tether"},
    "PYTHUSDT": {"name": "Pyth Network / Tether", "base_asset": "Pyth Network", "quote_asset": "Tether"},
    "XTZUSDT": {"name": "Tezos / Tether", "base_asset": "Tezos", "quote_asset": "Tether"},
    "QTUMUSDT": {"name": "Qtum / Tether", "base_asset": "Qtum", "quote_asset": "Tether"},
    "EOSUSDT": {"name": "EOS / Tether", "base_asset": "EOS", "quote_asset": "Tether"},
    "DASHUSDT": {"name": "Dash / Tether", "base_asset": "Dash", "quote_asset": "Tether"},
    "MANAUSDT": {"name": "Decentraland / Tether", "base_asset": "Decentraland", "quote_asset": "Tether"},
    "SLPUSDT": {"name": "Smooth Love Potion / Tether", "base_asset": "Smooth Love Potion", "quote_asset": "Tether"},
    "PAXUSDT": {"name": "Paxos Standard / Tether", "base_asset": "Paxos Standard", "quote_asset": "Tether"},

    # BTC-Quoted Pairs (aus deiner Liste)
    "IOTABTC": {"name": "IOTA / Bitcoin", "base_asset": "IOTA", "quote_asset": "Bitcoin"},
    "RENDERBTC": {"name": "Render / Bitcoin", "base_asset": "Render", "quote_asset": "Bitcoin"},
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
    """Adapter für den Import manueller Binance-Symbole (Spot)."""

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
            before = len(self._symbols)
            self._symbols = {sym: data for sym, data in self._symbols.items() if sym in wl}
            log.debug("[BINANCE][WL] filtered symbols: %d -> %d", before, len(self._symbols))
        if not self._symbols:
            raise ValueError("BinanceAdapter: keine Symbole verfügbar (Whitelist leer?)")
        log.info("[BINANCE][INIT] symbols_loaded=%d", len(self._symbols))

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
        log.info("[BINANCE] symbols requested for %s: n=%d", code, len(data))
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
            primary_category="Spot",   # eindeutiger als "Digital Assets"
            status="active",
            country=None,
            sector=None,
            listings=[listing],
            tags=[],                   # leer lassen
            identifiers=[{"key": "binance_symbol", "value": symbol}],
        )

        match_key = symbol or base_asset_name
        # Debug-Ausgabe pro Normalisierung (hilft bei Dry-Runs)
        log.debug("[BINANCE][NORM] symbol=%s name=%s match_key=%s", symbol, name, match_key)
        return draft, match_key


__all__ = ["BinanceAdapter", "DEFAULT_SYMBOL_DEFINITIONS"]
