# notifier_evaluator/eval/operators.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from typing import Any, Callable, Optional, Tuple

from notifier_evaluator.models.runtime import TriState, safe_float


# ──────────────────────────────────────────────────────────────────────────────
# Safe Operators
# - Keine stillen Exceptions
# - None/NaN -> UNKNOWN (nicht False)
# - string compares nur für eq/ne sinnvoll (optional)
# ──────────────────────────────────────────────────────────────────────────────


def _is_nan(x: Any) -> bool:
    try:
        return isinstance(x, float) and math.isnan(x)
    except Exception:
        return False


def _both_numeric(a: Any, b: Any) -> Tuple[Optional[float], Optional[float]]:
    fa = safe_float(a)
    fb = safe_float(b)
    return fa, fb


def _unknown(reason: str) -> Tuple[TriState, str]:
    return TriState.UNKNOWN, reason


def _ok(state: TriState) -> bool:
    return state in (TriState.TRUE, TriState.FALSE)


def op_gt(a: Any, b: Any) -> Tuple[TriState, str]:
    fa, fb = _both_numeric(a, b)
    if fa is None or fb is None:
        return _unknown("missing_numeric")
    return (TriState.TRUE, "ok") if fa > fb else (TriState.FALSE, "ok")


def op_gte(a: Any, b: Any) -> Tuple[TriState, str]:
    fa, fb = _both_numeric(a, b)
    if fa is None or fb is None:
        return _unknown("missing_numeric")
    return (TriState.TRUE, "ok") if fa >= fb else (TriState.FALSE, "ok")


def op_lt(a: Any, b: Any) -> Tuple[TriState, str]:
    fa, fb = _both_numeric(a, b)
    if fa is None or fb is None:
        return _unknown("missing_numeric")
    return (TriState.TRUE, "ok") if fa < fb else (TriState.FALSE, "ok")


def op_lte(a: Any, b: Any) -> Tuple[TriState, str]:
    fa, fb = _both_numeric(a, b)
    if fa is None or fb is None:
        return _unknown("missing_numeric")
    return (TriState.TRUE, "ok") if fa <= fb else (TriState.FALSE, "ok")


def op_eq(a: Any, b: Any) -> Tuple[TriState, str]:
    # eq: numeric if possible, else string compare
    fa, fb = _both_numeric(a, b)
    if fa is not None and fb is not None:
        return (TriState.TRUE, "ok") if fa == fb else (TriState.FALSE, "ok")

    # fallback: raw compare for simple types
    if a is None or b is None:
        return _unknown("missing_value")
    try:
        return (TriState.TRUE, "ok") if a == b else (TriState.FALSE, "ok")
    except Exception:
        return _unknown("compare_exc")


def op_ne(a: Any, b: Any) -> Tuple[TriState, str]:
    st, reason = op_eq(a, b)
    if st == TriState.UNKNOWN:
        return st, reason
    return (TriState.FALSE, "ok") if st == TriState.TRUE else (TriState.TRUE, "ok")


OPS: dict[str, Callable[[Any, Any], Tuple[TriState, str]]] = {
    "gt": op_gt,
    "gte": op_gte,
    "lt": op_lt,
    "lte": op_lte,
    "eq": op_eq,
    "ne": op_ne,
}


def apply_op(op: str, left: Any, right: Any) -> Tuple[TriState, str]:
    fn = OPS.get((op or "").strip().lower())
    if fn is None:
        return TriState.UNKNOWN, f"unknown_op:{op}"
    try:
        return fn(left, right)
    except Exception as e:
        return TriState.UNKNOWN, f"op_exc:{e}"
