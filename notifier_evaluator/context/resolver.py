# notifier_evaluator/context/resolver.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from notifier_evaluator.models.schema import Condition, EngineDefaults, Group, Profile
from notifier_evaluator.models.runtime import ResolvedContext, ResolvedPair


def _dbg(msg: str) -> None:
    print(f"[evaluator][DBG] {msg}")


def _safe_strip(x: Any) -> str:
    return (str(x).strip() if x is not None else "").strip()


def _first_non_empty(*vals: Any) -> Optional[str]:
    for v in vals:
        s = _safe_strip(v)
        if s:
            return s
    return None


def resolve_contexts(
    *,
    profile: Profile,
    group: Group,
    cond: Condition,
    defaults: EngineDefaults,
    base_symbol: str,
) -> Tuple[ResolvedPair, Dict[str, Any]]:
    pid = profile.id
    gid = group.gid
    rid = cond.rid
    base_symbol = _safe_strip(base_symbol)

    left_symbol = _first_non_empty(cond.left.symbol, base_symbol) or ""
    right_symbol = _first_non_empty(cond.right.symbol, base_symbol) or ""

    left_interval = _first_non_empty(cond.left.interval, group.interval, defaults.default_interval) or ""
    right_interval = _first_non_empty(cond.right.interval, group.interval, defaults.default_interval) or ""

    exchange = _first_non_empty(group.exchange, defaults.default_exchange) or ""
    clock_interval = _first_non_empty(group.interval, defaults.default_interval) or ""

    left_ctx = ResolvedContext(
        symbol=left_symbol,
        interval=left_interval,
        exchange=exchange,
        clock_interval=clock_interval,
    )
    right_ctx = ResolvedContext(
        symbol=right_symbol,
        interval=right_interval,
        exchange=exchange,
        clock_interval=clock_interval,
    )

    dbg: Dict[str, Any] = {
        "pid": pid,
        "gid": gid,
        "rid": rid,
        "base_symbol": base_symbol,
        "clock_interval": clock_interval,
        "exchange": exchange,
        "left": {"symbol": left_symbol, "interval": left_interval},
        "right": {"symbol": right_symbol, "interval": right_interval},
    }
    _dbg(
        "resolver pid=%s gid=%s rid=%s base=%s ex=%s clock=%s L(%s,%s) R(%s,%s)"
        % (pid, gid, rid, base_symbol, exchange, clock_interval, left_symbol, left_interval, right_symbol, right_interval)
    )

    return ResolvedPair(left=left_ctx, right=right_ctx), dbg
