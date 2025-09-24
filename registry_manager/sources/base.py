# registry_manager/sources/base.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

log = logging.getLogger("registry_manager.sources.base")

# ── Basis-Klassen ────────────────────────────────────────────────────────────

@dataclass
class Listing:
    """Eine konkrete Notierung eines Assets auf einer Börse."""
    source: str                   # z. B. "EODHD"
    symbol: str                   # z. B. "MSFT"
    exchange: str                 # z. B. "XNAS"
    mic: Optional[str] = None     # Market Identifier Code (optional, hilfreich)
    isin: Optional[str] = None    # ISIN, wenn vorhanden
    note: Optional[str] = None    # Freitext-Notiz, z. B. "Common Stock USD"

@dataclass
class AssetDraft:
    """Ein Asset-Entwurf, der in die Registry geschrieben werden kann."""
    id: str                       # Kanonische, stabile ID, z. B. "asset:msft"
    type: str                     # z. B. "equity", "index", "commodity"
    name: str                     # Voller Name, z. B. "Microsoft Corp."
    primary_category: str         # Ober-Kategorie, z. B. "Stocks"
    status: str                   # "active", "unsorted", ...
    country: Optional[str] = None
    sector: Optional[str] = None
    listings: List[Listing] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    identifiers: List[Dict[str, Any]] = field(default_factory=list)  # ISIN, CUSIP, WKN ...

class SourceAdapter(abc.ABC):
    """Abstrakte Basisklasse für jede Quelle (z. B. EODHD, Binance, etc.)."""

    @abc.abstractmethod
    def name(self) -> str:
        """Kurzname der Quelle, z. B. 'eodhd'."""
        raise NotImplementedError

    @abc.abstractmethod
    def exchanges(self) -> List[Dict[str, Any]]:
        """Liefert Liste aller Exchanges dieser Quelle."""
        raise NotImplementedError

    @abc.abstractmethod
    def symbols(self, exchange_code: str) -> List[Dict[str, Any]]:
        """Liefert alle Symbole für eine Exchange."""
        raise NotImplementedError

    @abc.abstractmethod
    def normalize(
        self, exchange_code: str, raw: Dict[str, Any], mic_map: Dict[str, str]
    ) -> tuple[AssetDraft, str]:
        """
        Wandelt Rohdaten (z. B. von der API) in einen AssetDraft um.
        Gibt außerdem einen 'match_key' zurück (z. B. ISIN oder Symbol),
        mit dem erkannt wird, ob ein Asset bereits existiert.
        """
        raise NotImplementedError
