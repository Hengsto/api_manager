# notifier_evaluator/context/resolver.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from notifier_evaluator.models.schema import Condition, EngineDefaults, Group, Profile
from notifier_evaluator.models.runtime import ResolvedContext, ResolvedPair


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


def _first_non_empty(*vals: Any) -> Optional[str]:
    for v in vals:
        s = _safe_strip(v)
        if s:
            return s
    return None


def _build_model(cls: Any, data: Dict[str, Any]) -> Any:
    """
    Build either Pydantic v2 model (model_validate) or classic ctor.
    """
    mv = getattr(cls, "model_validate", None)
    if callable(mv):
        return mv(data)
    return cls(**data)


def resolve_contexts(
    *,
    profile: Profile,
    group: Group,
    cond: Condition,
    defaults: EngineDefaults,
    base_symbol: str,
) -> Tuple[ResolvedPair, Dict[str, Any]]:
    """
    Resolve runtime context for a condition row.
    NEW-schema tolerant:
      - NO profile.default_interval / profile.default_exchange required
      - exchange is typically group-level only
      - per-side interval/symbol may be set on cond.left/cond.right (indicator objects)
      - legacy helper fields (left_interval/right_interval/left_exchange/...) tolerated if present

    Returns:
      (ResolvedPair(left=ResolvedContext, right=ResolvedContext), debug_dict)
    """

    pid = _safe_strip(getattr(profile, "profile_id", None)) or _safe_strip(getattr(profile, "id", None)) or "<pid?>"
    gid = _safe_strip(getattr(group, "gid", None)) or "<gid?>"
    rid = _safe_strip(getattr(cond, "rid", None)) or "<rid?>"
    base_symbol = _safe_strip(base_symbol)

    # ---- pull indicator objects (NEW schema) ----
    left_ind = getattr(cond, "left", None)
    right_ind = getattr(cond, "right", None)

    # These are "nice to have"; engine can survive missing ones but your eval will be junk.
    left_name = _safe_strip(getattr(left_ind, "name", None) if left_ind is not None else None)
    right_name = _safe_strip(getattr(right_ind, "name", None) if right_ind is not None else None)
    left_output = _safe_strip(getattr(left_ind, "output", None) if left_ind is not None else None)
    right_output = _safe_strip(getattr(right_ind, "output", None) if right_ind is not None else None)
    left_params = getattr(left_ind, "params", None) if left_ind is not None else None
    right_params = getattr(right_ind, "params", None) if right_ind is not None else None

    # ---- schema drift tolerant legacy fields (may exist after your normalizer) ----
    left_it_legacy = getattr(cond, "left_interval", None)
    right_it_legacy = getattr(cond, "right_interval", None)
    it_legacy = getattr(cond, "interval", None)

    left_ex_legacy = getattr(cond, "left_exchange", None)
    right_ex_legacy = getattr(cond, "right_exchange", None)
    ex_legacy = getattr(cond, "exchange", None)

    # ---- per-side overrides (NEW schema indicator objects) ----
    left_symbol_override = getattr(left_ind, "symbol", None) if left_ind is not None else None
    right_symbol_override = getattr(right_ind, "symbol", None) if right_ind is not None else None
    left_interval_override = getattr(left_ind, "interval", None) if left_ind is not None else None
    right_interval_override = getattr(right_ind, "interval", None) if right_ind is not None else None

    # ---- profile defaults are OPTIONAL (new schema doesn’t have them) ----
    prof_default_interval = getattr(profile, "default_interval", None)
    prof_default_exchange = getattr(profile, "default_exchange", None)

    # ---- group baseline ----
    group_interval = getattr(group, "interval", None)
    group_exchange = getattr(group, "exchange", None)

    # ---- engine defaults ----
    def_interval = getattr(defaults, "interval", None)
    def_clock = getattr(defaults, "clock_interval", None)
    def_exchange = getattr(defaults, "exchange", None)

    # Clock interval: group interval wins; else profile default (if legacy); else defaults
    clock_interval = _first_non_empty(group_interval, prof_default_interval, def_clock, def_interval) or ""

    # Base exchange: group wins; else profile default (if legacy); else defaults
    base_exchange = _first_non_empty(group_exchange, prof_default_exchange, def_exchange) or ""

    # Left/right interval resolution:
    left_interval = _first_non_empty(left_interval_override, left_it_legacy, it_legacy, group_interval, prof_default_interval, def_interval) or ""
    right_interval = _first_non_empty(right_interval_override, right_it_legacy, it_legacy, group_interval, prof_default_interval, def_interval) or ""

    # Left/right symbol resolution:
    left_symbol = _first_non_empty(left_symbol_override, base_symbol) or ""
    right_symbol = _first_non_empty(right_symbol_override, base_symbol) or ""

    # Left/right exchange resolution:
    left_exchange = _first_non_empty(left_ex_legacy, ex_legacy, base_exchange) or ""
    right_exchange = _first_non_empty(right_ex_legacy, ex_legacy, base_exchange) or ""

    # counts (optional; planner may also handle)
    left_count = getattr(left_ind, "count", None) if left_ind is not None else getattr(cond, "left_count", None)
    right_count = getattr(right_ind, "count", None) if right_ind is not None else getattr(cond, "right_count", None)

    try:
        left_count_i = int(left_count) if left_count is not None else 1
    except Exception:
        left_count_i = 1
    try:
        right_count_i = int(right_count) if right_count is not None else 1
    except Exception:
        right_count_i = 1

    dbg: Dict[str, Any] = {
        "pid": pid,
        "gid": gid,
        "rid": rid,
        "base_symbol": base_symbol,
        "clock_interval": clock_interval,
        "base_exchange": base_exchange,
        "left": {
            "name": left_name,
            "output": left_output,
            "symbol": left_symbol,
            "interval": left_interval,
            "exchange": left_exchange,
            "count": left_count_i,
            "interval_src": _safe_strip(left_interval_override) or _safe_strip(left_it_legacy) or _safe_strip(it_legacy) or _safe_strip(group_interval) or _safe_strip(prof_default_interval) or _safe_strip(def_interval),
            "exchange_src": _safe_strip(left_ex_legacy) or _safe_strip(ex_legacy) or _safe_strip(group_exchange) or _safe_strip(prof_default_exchange) or _safe_strip(def_exchange),
        },
        "right": {
            "name": right_name,
            "output": right_output,
            "symbol": right_symbol,
            "interval": right_interval,
            "exchange": right_exchange,
            "count": right_count_i,
            "interval_src": _safe_strip(right_interval_override) or _safe_strip(right_it_legacy) or _safe_strip(it_legacy) or _safe_strip(group_interval) or _safe_strip(prof_default_interval) or _safe_strip(def_interval),
            "exchange_src": _safe_strip(right_ex_legacy) or _safe_strip(ex_legacy) or _safe_strip(group_exchange) or _safe_strip(prof_default_exchange) or _safe_strip(def_exchange),
        },
    }

    _dbg(
        "[resolver] pid=%s gid=%s rid=%s base=%s clock=%s ex=%s | L(%s,%s,%s) R(%s,%s,%s)"
        % (
            pid,
            gid,
            rid,
            base_symbol,
            clock_interval,
            base_exchange,
            left_symbol,
            left_interval,
            left_exchange,
            right_symbol,
            right_interval,
            right_exchange,
        )
    )

    left_ctx = _build_model(
        ResolvedContext,
        {
            "indicator": left_name,
            "output": left_output,
            "symbol": left_symbol,
            "interval": left_interval,
            "exchange": left_exchange,
            "params": left_params or {},
            "count": left_count_i,
            "clock_interval": clock_interval,
        },
    )
    right_ctx = _build_model(
        ResolvedContext,
        {
            "indicator": right_name,
            "output": right_output,
            "symbol": right_symbol,
            "interval": right_interval,
            "exchange": right_exchange,
            "params": right_params or {},
            "count": right_count_i,
            "clock_interval": clock_interval,
        },
    )

    pair = _build_model(ResolvedPair, {"left": left_ctx, "right": right_ctx})
    return pair, dbg
