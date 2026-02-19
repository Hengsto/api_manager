# notifier_evaluator/eval/chain_eval.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Dict, List, Literal, Optional

from notifier_evaluator.models.runtime import ChainResult, ConditionResult, TriState

logger = logging.getLogger(__name__)

LogicOp = Literal["and", "or"]


class ChainEvalError(Exception):
    """Raised when chain evaluation fails due to invalid inputs."""
    pass


def _dbg(debug: bool, msg: str) -> None:
    if debug:
        try:
            print(msg)
        except Exception:
            pass


def _normalize_logic(op: Any, *, position: int) -> LogicOp:
    """
    Normalize logic operator.
    Allowed: "and", "or", blank/None (treated as "and")
    """
    if op is None:
        return "and"
    s = str(op).strip().lower()
    if s == "":
        return "and"
    if s in ("and", "or"):
        return s  # type: ignore[return-value]
    raise ChainEvalError(f"Invalid logic operator at position {position}: {op!r}")


def _combine_and(a: TriState, b: TriState) -> TriState:
    if a == TriState.FALSE or b == TriState.FALSE:
        return TriState.FALSE
    if a == TriState.UNKNOWN or b == TriState.UNKNOWN:
        return TriState.UNKNOWN
    return TriState.TRUE


def _combine_or(a: TriState, b: TriState) -> TriState:
    if a == TriState.TRUE or b == TriState.TRUE:
        return TriState.TRUE
    if a == TriState.UNKNOWN or b == TriState.UNKNOWN:
        return TriState.UNKNOWN
    return TriState.FALSE


def eval_chain(
    results: List[ConditionResult],
    *,
    logic_to_prev: Optional[List[str]] = None,
    debug: bool = False,
    debug_print: bool = False,
) -> ChainResult:
    """
    results must be in row order.

    logic_to_prev:
      list (optional).
      logic_to_prev[i] applies between results[i-1] and results[i] for i>=1.
      values: "and" or "or"
      missing/blank entries default to "and"
      (logic_to_prev[0] is ignored)
    """
    if not results:
        return ChainResult(
            partial_true=False,
            final_state=TriState.UNKNOWN,
            debug={"reason": "no_rows"},
        )

    # partial_true = at least one TRUE row
    partial_true = any(r.state == TriState.TRUE for r in results)

    # Start with first row state
    final_state: TriState = results[0].state

    debug_steps: List[Dict[str, str]] = []
    debug_steps.append(
        {"i": "0", "rid": results[0].rid, "state": results[0].state.value, "logic": "<start>"}
    )

    for i in range(1, len(results)):
        # Default to AND if missing/short list/blank
        logic_raw = None
        if logic_to_prev is not None and i < len(logic_to_prev):
            logic_raw = logic_to_prev[i]
        logic = _normalize_logic(logic_raw, position=i)

        before = final_state
        cur = results[i].state

        if logic == "or":
            final_state = _combine_or(before, cur)
        else:
            final_state = _combine_and(before, cur)

        debug_steps.append(
            {
                "i": str(i),
                "rid": results[i].rid,
                "logic": logic,
                "before": before.value,
                "cur": cur.value,
                "after": final_state.value,
            }
        )

        if debug:
            logger.debug(
                "[chain_eval] step=%d rid=%s before=%s logic=%s cur=%s after=%s",
                i,
                results[i].rid,
                before.value,
                logic,
                cur.value,
                final_state.value,
            )
        if debug_print:
            _dbg(
                True,
                f"[chain_eval] step={i} rid={results[i].rid} before={before.value} logic={logic} cur={cur.value} after={final_state.value}",
            )

    if debug:
        logger.debug(
            "[chain_eval] done rows=%d partial_true=%s final_state=%s",
            len(results),
            partial_true,
            final_state.value,
        )
    if debug_print:
        _dbg(True, f"[chain_eval] done rows={len(results)} partial_true={partial_true} final_state={final_state.value}")
        for st in debug_steps:
            _dbg(True, f"[chain_eval] step={st}")

    return ChainResult(
        partial_true=partial_true,
        final_state=final_state,
        debug={"steps": debug_steps},
    )
