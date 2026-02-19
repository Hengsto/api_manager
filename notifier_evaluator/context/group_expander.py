# notifier_evaluator/context/group_expander.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


DEBUG_PRINT = True


def _dbg(msg: str) -> None:
    if DEBUG_PRINT:
        try:
            print(msg)
        except Exception:
            pass


def _safe_strip(x: Any) -> str:
    try:
        return (str(x).strip() if x is not None else "").strip()
    except Exception:
        return ""


def _uniq_stable(items: List[str]) -> List[str]:
    seen: Dict[str, None] = {}
    out: List[str] = []
    for it in items or []:
        s = _safe_strip(it)
        if not s:
            continue
        if s in seen:
            continue
        seen[s] = None
        out.append(s)
    return out


@dataclass
class ExpandedGroup:
    symbols: List[str]
    # Optional diagnostics / compatibility
    version_key: str = ""
    tags: List[str] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)


class StaticMappingSource:
    """
    Simple in-memory mapping resolver.
    Mapping format:
        {
            "majors": ["BTCUSDT", "ETHUSDT"],
            "alts": ["SOLUSDT", ...],
        }
    """

    def __init__(self, mapping: Optional[Dict[str, List[str]]] = None):
        self.mapping: Dict[str, List[str]] = mapping or {}
        _dbg(f"[group_expander] StaticMappingSource init mapping={len(self.mapping)}")

    def resolve(self, key: str) -> List[str]:
        k = _safe_strip(key)
        if not k:
            return []
        res = self.mapping.get(k, []) or []
        # keep stable order
        return [x for x in res if _safe_strip(x)]


class TTLGroupExpander:
    """
    Backwards compatible constructor.

    Your main_evaluator.py calls:
        TTLGroupExpander(group_source, ttl_sec=...)

    So we MUST accept `source` as first positional argument.
    """

    def __init__(
        self,
        source: Optional[Any] = None,  # <-- positional compatible
        *,
        ttl_sec: int = 10,
        symbol_groups: Optional[Dict[str, List[str]]] = None,
        resolver: Optional[Callable[[str], List[str]]] = None,
    ):
        self.ttl_sec = int(ttl_sec or 10)

        # If caller passed dict positionally -> treat as symbol_groups
        if isinstance(source, dict) and symbol_groups is None:
            symbol_groups = source  # type: ignore[assignment]
            source = None

        self.resolver = resolver
        self.source = source
        self.symbol_groups = symbol_groups or {}

        # cache: cache_key -> (ts, ExpandedGroup)
        self._cache: Dict[str, Tuple[float, ExpandedGroup]] = {}

        _dbg(
            "[group_expander] TTLGroupExpander init ttl_sec=%s has_source=%s has_symbol_groups=%s has_resolver=%s"
            % (
                self.ttl_sec,
                bool(self.source),
                bool(self.symbol_groups),
                bool(self.resolver),
            )
        )

    def _resolve_one(self, key: str) -> List[str]:
        k = _safe_strip(key)
        if not k:
            return []

        # 1) explicit resolver callable
        if self.resolver:
            try:
                res = self.resolver(k) or []
                _dbg(f"[group_expander] resolve via resolver key={k!r} n={len(res)}")
                return res
            except Exception as e:
                _dbg(f"[group_expander] WARN resolver failed key={k!r} err={e!r}")

        # 2) source.resolve(key)
        if self.source is not None:
            try:
                fn = getattr(self.source, "resolve", None)
                if callable(fn):
                    res = fn(k) or []
                    _dbg(f"[group_expander] resolve via source.resolve key={k!r} n={len(res)}")
                    return res
            except Exception as e:
                _dbg(f"[group_expander] WARN source.resolve failed key={k!r} err={e!r}")

        # 3) static dict mapping
        if self.symbol_groups:
            res = self.symbol_groups.get(k, []) or []
            _dbg(f"[group_expander] resolve via symbol_groups dict key={k!r} n={len(res)}")
            return res

        _dbg(f"[group_expander] resolve key={k!r} -> empty (no resolver/source/mapping)")
        return []

    def _build_cache_key(self, *, symbol_group: str, tags: List[str], explicit_symbols: List[str]) -> str:
        sg = _safe_strip(symbol_group)
        t = ",".join(_uniq_stable([_safe_strip(x) for x in (tags or [])]))
        es = ",".join(_uniq_stable([_safe_strip(x) for x in (explicit_symbols or [])]))
        return f"sg={sg}|tags={t}|explicit={es}"

    def expand_group(self, group: Any) -> ExpandedGroup:
        """
        Schema drift tolerant:

        NEW schema fields we expect in 2026:
          - group.symbol_group: Optional[str]
          - group.symbols: Optional[List[str]]
          - group.active: bool
          - group.conditions: List[...]
          (NO group_tags by default)

        Legacy-ish fields we tolerate:
          - group.group_tags
          - group.tags
          - group.symbol_groups / symbol_group_name
        """

        # -------- read inputs (tolerant) --------
        symbol_group = _safe_strip(getattr(group, "symbol_group", None))
        if not symbol_group:
            # tolerate other names
            symbol_group = _safe_strip(getattr(group, "symbol_group_name", None))

        explicit_symbols_raw = getattr(group, "symbols", None)
        if explicit_symbols_raw is None:
            explicit_symbols_raw = getattr(group, "symbol", None)  # extremely legacy
        explicit_symbols: List[str] = []
        if isinstance(explicit_symbols_raw, list):
            explicit_symbols = [x for x in explicit_symbols_raw]
        elif isinstance(explicit_symbols_raw, str):
            # tolerate single string (bad UI payload)
            explicit_symbols = [explicit_symbols_raw]

        # tags are optional; DO NOT assume they exist
        tags_raw = getattr(group, "group_tags", None)
        if tags_raw is None:
            tags_raw = getattr(group, "tags", None)
        tags: List[str] = []
        if isinstance(tags_raw, list):
            tags = [x for x in tags_raw]
        elif isinstance(tags_raw, str):
            tags = [tags_raw]

        explicit_symbols = _uniq_stable([_safe_strip(x) for x in explicit_symbols])
        tags = _uniq_stable([_safe_strip(x) for x in tags])

        # -------- cache --------
        cache_key = self._build_cache_key(symbol_group=symbol_group, tags=tags, explicit_symbols=explicit_symbols)
        now = time.time()

        hit = self._cache.get(cache_key)
        if hit:
            ts, eg = hit
            age = now - ts
            if age <= self.ttl_sec:
                _dbg(
                    "[group_expander] cache HIT key=%s age=%.2fs symbols=%d"
                    % (cache_key, age, len(eg.symbols or []))
                )
                return eg
            _dbg("[group_expander] cache EXPIRE key=%s age=%.2fs" % (cache_key, age))

        # -------- expand --------
        expanded: List[str] = []
        source_used = None

        # (A) resolve symbol_group
        if symbol_group:
            resolved = self._resolve_one(symbol_group)
            expanded.extend(resolved)
            source_used = "symbol_group"

        # (B) tags can also behave like groups (optional feature)
        # If you want tags to map to symbols, we try resolve each tag too.
        # If you *don't* want that, comment this block out.
        if tags:
            tag_symbols: List[str] = []
            for t in tags:
                tag_symbols.extend(self._resolve_one(t))
            if tag_symbols:
                expanded.extend(tag_symbols)
                source_used = (source_used + "+tags") if source_used else "tags"

        # (C) explicit symbols always included
        if explicit_symbols:
            expanded.extend(explicit_symbols)
            source_used = (source_used + "+explicit") if source_used else "explicit"

        expanded = _uniq_stable([_safe_strip(x) for x in expanded])

        # Version key: stable signature for downstream debugging / tick caching
        version_key = f"{cache_key}|n={len(expanded)}"

        eg = ExpandedGroup(
            symbols=expanded,
            version_key=version_key,
            tags=tags,
            debug={
                "cache_key": cache_key,
                "symbol_group": symbol_group,
                "explicit_n": len(explicit_symbols),
                "tags_n": len(tags),
                "resolved_n": len(expanded),
                "source_used": source_used,
            },
        )

        self._cache[cache_key] = (now, eg)

        _dbg(
            "[group_expander] expand_group key=%s source=%s symbols=%d"
            % (cache_key, source_used, len(expanded))
        )
        if not expanded:
            _dbg(
                "[group_expander] WARN expanded empty: symbol_group=%r tags=%r explicit=%r"
                % (symbol_group, tags, explicit_symbols)
            )

        return eg
