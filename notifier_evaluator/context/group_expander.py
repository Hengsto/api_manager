# notifier_evaluator/context/group_expander.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Tuple

from notifier_evaluator.models.schema import Group


# ──────────────────────────────────────────────────────────────────────────────
# Group Expansion
#
# Input:
#   group.symbols: list[str]         (konkrete Symbole)
#   group.group_tags: list[str]      (z.B. "top50", "meme", "forex_major", ...)
#
# Output:
#   expanded_symbols: list[str]      (unique, stable order)
#   version_key: str                (für "stetig aktuell" + optional skip)
#
# Design:
# - DataSource als Protocol (lokal/HTTP/DB austauschbar)
# - TTL Cache optional (default on)
# - "stetig aktuell": entweder TTL=0 (immer fresh) oder TTL klein (z.B. 10s)
# ──────────────────────────────────────────────────────────────────────────────


class GroupManagerSource(Protocol):
    """
    Interface für die Expansion von group_tags -> symbols.

    Du kannst später implementieren:
    - HTTP Client (Asset-IDs -> Symbols)
    - DB Query
    - Static mapping
    """
    def expand_tags(self, tags: List[str]) -> List[str]:
        ...


@dataclass
class ExpandResult:
    symbols: List[str]
    version_key: str
    debug: Dict[str, str]


class ExpandError(RuntimeError):
    pass


def _strip(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    ss = str(s).strip()
    return ss or None


def _uniq_stable(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for it in items:
        it2 = _strip(it)
        if not it2:
            continue
        if it2 in seen:
            continue
        seen.add(it2)
        out.append(it2)
    return out


def _mk_version_key(symbols: List[str], tags: List[str]) -> str:
    """
    Version key (cheap).
    Wenn du später eine echte "group version" hast (z.B. etag, updated_at),
    kannst du das hier ersetzen.
    """
    s = "|".join(symbols) + "||" + "|".join(tags)
    # nicht cryptographic; reicht als stable key
    return f"v1:{hash(s)}"


class TTLGroupExpander:
    """
    Expander mit TTL Cache über group_tags.
    """

    def __init__(self, source: GroupManagerSource, ttl_sec: int = 10):
        self.source = source
        self.ttl_sec = max(0, int(ttl_sec))
        self._cache: Dict[str, Tuple[float, List[str]]] = {}  # tag_key -> (expires_ts, symbols)

    def _cache_key(self, tags: List[str]) -> str:
        # tags order-insensitive, stable
        cleaned = sorted(_uniq_stable(tags))
        return "|".join(cleaned)

    def expand_group(self, group: Group) -> ExpandResult:
        """
        Expandiert group.symbols + group.group_tags -> symbols_expanded
        """
        base_symbols = _uniq_stable(list(group.symbols or []))
        tags = _uniq_stable(list(group.group_tags or []))

        debug: Dict[str, str] = {
            "gid": getattr(group, "gid", "<no-gid>"),
            "base_symbols_n": str(len(base_symbols)),
            "tags_n": str(len(tags)),
            "ttl_sec": str(self.ttl_sec),
        }

        tag_symbols: List[str] = []
        if tags:
            tag_key = self._cache_key(tags)
            now = time.time()

            if self.ttl_sec > 0:
                hit = self._cache.get(tag_key)
                if hit:
                    exp_ts, cached = hit
                    if now <= exp_ts:
                        tag_symbols = list(cached)
                        debug["cache"] = "hit"
                    else:
                        debug["cache"] = "expired"
                else:
                    debug["cache"] = "miss"
            else:
                debug["cache"] = "disabled"

            if not tag_symbols:
                try:
                    tag_symbols = _uniq_stable(self.source.expand_tags(tags))
                except Exception as e:
                    raise ExpandError(f"[group_expander] expand_tags failed gid={group.gid} tags={tags}: {e}") from e

                if self.ttl_sec > 0:
                    self._cache[tag_key] = (now + self.ttl_sec, list(tag_symbols))

        merged = _uniq_stable(base_symbols + tag_symbols)

        # Debug print (extra noisy)
        print(
            "[group_expander] gid=%s enabled=%s | base=%d tags=%d -> expanded=%d | cache=%s"
            % (group.gid, getattr(group, "enabled", True), len(base_symbols), len(tags), len(merged), debug.get("cache", "?"))
        )

        return ExpandResult(
            symbols=merged,
            version_key=_mk_version_key(merged, tags),
            debug=debug,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Minimal default source for development/tests
# ──────────────────────────────────────────────────────────────────────────────

class StaticMappingSource:
    """
    Simple mapping tag -> symbols
    Gut für Tests / quick start.
    """
    def __init__(self, mapping: Dict[str, List[str]]):
        self.mapping = mapping

    def expand_tags(self, tags: List[str]) -> List[str]:
        out: List[str] = []
        for t in tags:
            out.extend(self.mapping.get(t, []))
        return out
