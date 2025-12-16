# notifier_evaluator/eval/condition_eval.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, Optional, Tuple

from notifier_evaluator.eval.operators import apply_op
from notifier_evaluator.fetch.types import RequestKey
from notifier_evaluator.models.schema import Condition
from notifier_evaluator.models.runtime import ConditionResult, FetchResult, ResolvedPair, RowSide, TriState


# ──────────────────────────────────────────────────────────────────────────────
# Condition Eval (single row)
# - holt konkrete Werte aus FetchResult (latest_value)
# - evaluiert OP
# - liefert TriState + Debug
# ──────────────────────────────────────────────────────────────────────────────


def eval_condition_row(
    *,
    profile_id: str,
    gid: str,
    base_symbol: str,
    cond: Condition,
    pair: ResolvedPair,
    row_map: Dict[Tuple[str, str, str, str, str], RequestKey],
    fetch_results: Dict[RequestKey, FetchResult],
) -> ConditionResult:
    """
    Evaluates one Condition row for one (profile_id, gid, base_symbol).

    fetch_results: dict[RequestKey] -> FetchResult
    """
    rid = cond.rid
    map_key_left = (profile_id, gid, rid, base_symbol, RowSide.LEFT.value)
    map_key_right = (profile_id, gid, rid, base_symbol, RowSide.RIGHT.value)

    k_left = row_map.get(map_key_left)
    k_right = row_map.get(map_key_right)

    if k_left is None or k_right is None:
        reason = "missing_row_map"
        print("[cond_eval] WARN %s profile=%s gid=%s base_symbol=%s rid=%s left=%s right=%s"
              % (reason, profile_id, gid, base_symbol, rid, bool(k_left), bool(k_right)))
        return ConditionResult(
            rid=rid,
            state=TriState.UNKNOWN,
            op=cond.op,
            left_value=None,
            right_value=None,
            reason=reason,
            debug={
                "profile_id": profile_id,
                "gid": gid,
                "base_symbol": base_symbol,
                "rid": rid,
            },
        )

    fr_left = fetch_results.get(k_left)
    fr_right = fetch_results.get(k_right)

    if fr_left is None or fr_right is None:
        reason = "missing_fetch_result"
        print("[cond_eval] WARN %s profile=%s gid=%s base_symbol=%s rid=%s l=%s r=%s"
              % (reason, profile_id, gid, base_symbol, rid, fr_left is not None, fr_right is not None))
        return ConditionResult(
            rid=rid,
            state=TriState.UNKNOWN,
            op=cond.op,
            left_value=(fr_left.latest_value if fr_left else None),
            right_value=(fr_right.latest_value if fr_right else None),
            reason=reason,
            debug={
                "k_left": k_left.short(),
                "k_right": k_right.short(),
                "left_ok": (fr_left.ok if fr_left else None),
                "right_ok": (fr_right.ok if fr_right else None),
            },
        )

    left_val = fr_left.latest_value
    right_val = fr_right.latest_value

    # if fetch failed -> UNKNOWN (do not treat as FALSE)
    if not fr_left.ok or not fr_right.ok:
        reason = "fetch_not_ok"
        print(
            "[cond_eval] WARN %s rid=%s left_ok=%s right_ok=%s lerr=%s rerr=%s"
            % (reason, rid, fr_left.ok, fr_right.ok, fr_left.error, fr_right.error)
        )
        return ConditionResult(
            rid=rid,
            state=TriState.UNKNOWN,
            op=cond.op,
            left_value=left_val,
            right_value=right_val,
            reason=reason,
            debug={
                "k_left": k_left.short(),
                "k_right": k_right.short(),
                "left_error": fr_left.error,
                "right_error": fr_right.error,
                "left_ts": fr_left.latest_ts,
                "right_ts": fr_right.latest_ts,
            },
        )

    # apply operator
    state, op_reason = apply_op(cond.op, left_val, right_val)

    # Debug print (noisy)
    print(
        "[cond_eval] profile=%s gid=%s base_symbol=%s rid=%s | %s(%s) %s %s(%s) -> %s (%s)"
        % (
            profile_id, gid, base_symbol, rid,
            left_val, k_left.short(),
            cond.op,
            right_val, k_right.short(),
            state.value, op_reason,
        )
    )

    return ConditionResult(
        rid=rid,
        state=state,
        op=cond.op,
        left_value=left_val,
        right_value=right_val,
        reason=op_reason,
        debug={
            "profile_id": profile_id,
            "gid": gid,
            "base_symbol": base_symbol,
            "rid": rid,
            "left_ctx": {
                "symbol": pair.left.symbol,
                "interval": pair.left.interval,
                "exchange": pair.left.exchange,
            },
            "right_ctx": {
                "symbol": pair.right.symbol,
                "interval": pair.right.interval,
                "exchange": pair.right.exchange,
            },
            "clock_interval": pair.left.clock_interval,
            "k_left": k_left.short(),
            "k_right": k_right.short(),
            "left_ts": fr_left.latest_ts,
            "right_ts": fr_right.latest_ts,
        },
    )
