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

    # interval resolution: row -> group -> engine defaults
    left_interval = _first_non_empty(cond.left.interval, group.interval, getattr(defaults, "interval", None)) or ""
    right_interval = _first_non_empty(cond.right.interval, group.interval, getattr(defaults, "interval", None)) or ""

    # exchange resolution: group -> engine defaults
    exchange = _first_non_empty(group.exchange, getattr(defaults, "exchange", None)) or ""

    # clock resolution: group interval preferred, otherwise explicit engine clock_interval, otherwise engine interval
    clock_interval = _first_non_empty(
        group.interval,
        getattr(defaults, "clock_interval", None),
        getattr(defaults, "interval", None),
    ) or ""

    _dbg(
        "resolver inputs pid=%s gid=%s rid=%s base_symbol=%s | "
        "group.interval=%s group.exchange=%s | "
        "cond.L.interval=%s cond.R.interval=%s | "
        "defaults.exchange=%s defaults.interval=%s defaults.clock_interval=%s"
        % (
            pid,
            gid,
            rid,
            base_symbol,
            _safe_strip(getattr(group, "interval", None)),
            _safe_strip(getattr(group, "exchange", None)),
            _safe_strip(getattr(cond.left, "interval", None)),
            _safe_strip(getattr(cond.right, "interval", None)),
            _safe_strip(getattr(defaults, "exchange", None)),
            _safe_strip(getattr(defaults, "interval", None)),
            _safe_strip(getattr(defaults, "clock_interval", None)),
        )
    )

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