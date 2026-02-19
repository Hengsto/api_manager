# notifier_evaluator/context/group_expander.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


def _dbg(msg: str) -> None:
    print(f"[evaluator][DBG] {msg}")


def _safe_strip(x: Any) -> str:
    return (str(x).strip() if x is not None else "").strip()


def _uniq_stable(items: List[str]) -> List[str]:
    seen: Dict[str, None] = {}
    out: List[str] = []
    for it in items or []:
        s = _safe_strip(it)
        if not s or s in seen:
            continue
        seen[s] = None
        out.append(s)
    return out


@dataclass
class ExpandedGroup:
    symbols: List[str]
    version_key: str = ""
    debug: Dict[str, Any] = field(default_factory=dict)


class StaticMappingSource:
    def __init__(self, mapping: Optional[Dict[str, List[str]]] = None):
        self.mapping: Dict[str, List[str]] = mapping or {}

    def resolve(self, key: str) -> List[str]:
        return list(self.mapping.get(_safe_strip(key), []) or [])


class TTLGroupExpander:
    def __init__(
        self,
        source: Optional[Any] = None,
        *,
        ttl_sec: int = 10,
        symbol_groups: Optional[Dict[str, List[str]]] = None,
        resolver: Optional[Callable[[str], List[str]]] = None,
    ):
        if isinstance(source, dict) and symbol_groups is None:
            symbol_groups = source
            source = None
        self.source = source
        self.ttl_sec = int(ttl_sec or 10)
        self.symbol_groups = symbol_groups or {}
        self.resolver = resolver
        self._cache: Dict[str, Tuple[float, ExpandedGroup]] = {}

    def _resolve_symbol_group(self, key: str) -> List[str]:
        k = _safe_strip(key)
        if not k:
            return []
        if self.resolver is not None:
            resolved = self.resolver(k) or []
            _dbg(f"group_expander resolver key={k} n={len(resolved)}")
            return resolved
        if self.source is not None and callable(getattr(self.source, "resolve", None)):
            resolved = self.source.resolve(k) or []
            _dbg(f"group_expander source.resolve key={k} n={len(resolved)}")
            return resolved
        resolved = self.symbol_groups.get(k, []) or []
        _dbg(f"group_expander symbol_groups key={k} n={len(resolved)}")
        return resolved

    def _build_cache_key(self, symbol_group: Optional[str], symbols: Optional[List[str]]) -> str:
        return f"sg={_safe_strip(symbol_group)}|symbols={','.join(_uniq_stable(symbols or []))}"

    def expand_group(self, group: Any) -> ExpandedGroup:
        symbol_group = _safe_strip(getattr(group, "symbol_group", None))
        explicit_symbols_raw = getattr(group, "symbols", None)
        explicit_symbols = _uniq_stable(list(explicit_symbols_raw or [])) if explicit_symbols_raw is not None else []

        cache_key = self._build_cache_key(symbol_group, explicit_symbols)
        now = time.time()
        hit = self._cache.get(cache_key)
        if hit and (now - hit[0]) <= self.ttl_sec:
            _dbg(f"group_expander cache hit key={cache_key}")
            return hit[1]

        resolved_group_symbols = self._resolve_symbol_group(symbol_group) if symbol_group else []
        symbols = _uniq_stable((explicit_symbols or []) + (resolved_group_symbols or []))

        if not symbols:
            raise ValueError(
                f"group '{getattr(group, 'gid', '<missing-gid>')}' is invalid: both symbols and symbol_group resolved symbols are empty"
            )

        expanded = ExpandedGroup(
            symbols=symbols,
            version_key=f"{cache_key}|n={len(symbols)}",
            debug={
                "symbol_group": symbol_group,
                "explicit_symbols_n": len(explicit_symbols),
                "resolved_group_symbols_n": len(resolved_group_symbols),
            },
        )
        self._cache[cache_key] = (now, expanded)
        _dbg(f"group_expander expanded gid={getattr(group, 'gid', '?')} symbols={len(symbols)}")
        return expanded