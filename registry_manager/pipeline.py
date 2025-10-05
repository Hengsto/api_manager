# ingest/pipeline.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import logging, time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .registry_client import RegistryClient
from .sources.base import SourceAdapter, AssetDraft, Listing
from .sources.binance import BinanceAdapter
from .sources.eodhd import EODHDAdapter

from registry_manager.sources.binance import BinanceAdapter
from registry_manager.sources.binance_futures import BinanceFuturesAdapter  # NEU

ADAPTERS = {
    "eodhd": EODHDAdapter(),
    "binance": BinanceAdapter(),                 # Spot
    "binance_futures": BinanceFuturesAdapter(),  # COIN-M
}

# Debug: Adapterübersicht (hilft sofort beim nächsten Problem)
import logging
log = logging.getLogger("pipeline")
log.info("[PIPE] adapters available=%s", list(ADAPTERS))


def _asset_payload(a: AssetDraft) -> Dict[str, Any]:
    return {
        "id": a.id,
        "type": a.type,
        "name": a.name,
        "primary_category": a.primary_category,
        "status": a.status,
        "country": a.country,
        "sector": a.sector,
        "listings": [vars(l) for l in a.listings],
        "tags": a.tags,
        "identifiers": a.identifiers,
    }

def _pick_asset_from_search(sr: Dict[str, Any], source: str, symbol: str, mic: Optional[str], exchange: Optional[str]) -> Optional[str]:
    sym_upper = (symbol or "").upper()
    src_upper = (source or "").upper()
    cand = None
    for l in sr.get("listings", []):
        if (l.get("source","").upper()==src_upper) and (l.get("symbol","").upper()==sym_upper):
            if mic and l.get("mic") and l.get("mic","").upper()==mic.upper():
                return l.get("asset_id")
            if exchange and l.get("exchange") and l.get("exchange","").upper()==(exchange or "").upper():
                cand = l.get("asset_id")
            if not cand:
                cand = l.get("asset_id")
    return cand

def build_mic_map(adapter: SourceAdapter) -> Dict[str, str]:
    mm: Dict[str,str] = {}
    try:
        for ex in adapter.exchanges():
            code = (ex.get("Code") or "").upper()
            opmic = (ex.get("OperatingMIC") or ex.get("MIC") or "").upper()
            if code:
                mm[code] = opmic or ""
        log.info(f"[MIC] map entries={len(mm)}")
    except Exception as e:
        log.warning(f"[MIC] could not fetch exchanges: {e}")
    return mm

def run_import(
    source: str,
    exchanges: List[str],
    limit: Optional[int]=None,
    sleep_sec: float=0.0,
    dry_run: bool=False,
    extra_tags: Optional[List[str]]=None,
    force_unsorted: bool=False,
    registry_endpoint: Optional[str]=None,
) -> Dict[str,int]:
    adapter = ADAPTERS.get(source.lower())
    if not adapter:
        raise ValueError(f"Unknown source: {source}. Available: {list(ADAPTERS)}")

    reg = RegistryClient(base=registry_endpoint)
    health = reg.health()
    log.info(f"[PIPE] registry ok={health.get('ok')} engine={health.get('engine')}")

    mic_map = build_mic_map(adapter)

    totals = dict(new_assets=0, linked_listings=0, skipped=0, errors=0)
    for exch in exchanges:
        stats = dict(new_assets=0, linked_listings=0, skipped=0, errors=0)
        try:
            raws = list(adapter.symbols(exch))
            if limit: raws = raws[:int(limit)]
            log.info(f"[{source.upper()}][{exch}] processing n={len(raws)} limit={limit}")
            for i, raw in enumerate(raws, 1):
                try:
                    draft, key = adapter.normalize(exch, raw, mic_map)
                    if extra_tags: draft.tags.extend(extra_tags)
                    if force_unsorted: draft.status = "unsorted"

                    # 1) ISIN match
                    matched_id: Optional[str] = None
                    isin = None
                    for ident in draft.identifiers:
                        if ident.get("key")=="isin":
                            isin = ident.get("value")
                            break
                    if isin:
                        try:
                            sr = reg.search(isin, limit=10)
                            cand = next((x for x in sr.get("identifiers", []) if x.get("value","").upper()==isin.upper()), None)
                            if cand:
                                matched_id = cand.get("asset_id")
                                if not dry_run:
                                    reg.add_listing(matched_id, vars(draft.listings[0]))
                                    reg.upsert_identifier(matched_id, "isin", isin)
                                stats["linked_listings"] += 1
                                if i % 200 == 0:
                                    log.info(f"[LINK][ISIN] {exch} {i}: {matched_id} <- {draft.listings[0].symbol}")
                                if sleep_sec: time.sleep(sleep_sec)
                                continue
                        except Exception as e:
                            log.warning(f"[WARN][ISIN] search failed ({isin}): {e}")

                    # 2) Symbol+Source(+MIC/Exchange) search
                    try:
                        sym = draft.listings[0].symbol
                        sr2 = reg.search(sym, limit=10)
                        matched_id = _pick_asset_from_search(sr2, draft.listings[0].source, sym, draft.listings[0].mic, draft.listings[0].exchange)
                        if matched_id:
                            if not dry_run:
                                reg.add_listing(matched_id, vars(draft.listings[0]))
                                if isin:
                                    reg.upsert_identifier(matched_id, "isin", isin)
                            stats["linked_listings"] += 1
                            if i % 200 == 0:
                                log.info(f"[LINK][SYM] {exch} {i}: {matched_id} <- {sym}")
                            if sleep_sec: time.sleep(sleep_sec)
                            continue
                    except Exception as e:
                        log.warning(f"[WARN][SYM] search failed ({draft.listings[0].symbol}): {e}")

                    # 3) Neues Asset
                    payload = _asset_payload(draft)
                    if not dry_run:
                        try:
                            reg.create_asset(payload)
                            stats["new_assets"] += 1
                            if i % 100 == 0:
                                log.info(f"[NEW] {exch} {i}: {draft.id} ({draft.listings[0].symbol})")
                        except Exception as e:
                            # ID-Kollision → Alt-ID
                            if "409" in str(e):
                                payload["id"] = payload["id"] + f"-{exch.lower()}-{draft.listings[0].symbol.lower()}"
                                reg.create_asset(payload)
                                stats["new_assets"] += 1
                                log.info(f"[NEW][ALT-ID] {exch} {i}: {payload['id']}")
                            else:
                                raise
                    else:
                        stats["new_assets"] += 1
                        if i % 100 == 0:
                            log.info(f"[DRY][NEW] {exch} {i}: {draft.id} ({draft.listings[0].symbol})")

                    if sleep_sec: time.sleep(sleep_sec)

                except Exception as e:
                    stats["errors"] += 1
                    log.error(f"[ERR][{exch}] idx={i} sym={raw.get('Code') or '?'} : {e}")
        except Exception as e:
            stats["errors"] += 1
            log.error(f"[ERR][{exch}] fetch symbols failed: {e}")

        for k,v in stats.items():
            totals[k] = totals.get(k,0) + v
        log.info(f"[DONE][{exch}] new={stats['new_assets']} linked={stats['linked_listings']} skip={stats['skipped']} err={stats['errors']}")

    log.info(f"[SUMMARY] source={source} new={totals['new_assets']} linked={totals['linked_listings']} skip={totals['skipped']} err={totals['errors']}")
    return totals