# notifier_evaluator/eval/chain_eval.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List, Optional

from notifier_evaluator.models.runtime import ChainResult, ConditionResult, TriState


# ──────────────────────────────────────────────────────────────────────────────
# Chain Eval
# - Input: ConditionResults in order + logic_to_prev list (and/or per row)
# - Output:
#     partial_true: bool         (mindestens eine ganze Zeile TRUE)
#     final_state: TriState      (TRUE/FALSE/UNKNOWN)
#
# TriState Regeln:
# - AND:
#     if any FALSE -> FALSE
#     else if any UNKNOWN -> UNKNOWN
#     else TRUE
# - OR:
#     if any TRUE -> TRUE
#     else if any UNKNOWN -> UNKNOWN
#     else FALSE
#
# Ketten-Interpretation:
#   row[0] startet mit row[0].state
#   ab row[1]: combine(prev, row[i].state) with logic_to_prev[i]
#   (logic_to_prev[0] wird ignoriert)
# ──────────────────────────────────────────────────────────────────────────────


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
) -> ChainResult:
    """
    results must be in row order.

    logic_to_prev:
      list of same length as results (optional).
      logic_to_prev[i] applies between results[i-1] and results[i] for i>=1.
      values: "and" or "or"
    """
    if not results:
        # No rows -> we treat as UNKNOWN (can't be true)
        return ChainResult(
            partial_true=False,
            final_state=TriState.UNKNOWN,
            debug={"reason": "no_rows"},
        )

    # partial_true = at least one TRUE row
    partial_true = any(r.state == TriState.TRUE for r in results)

    # Start with first row state
    final_state = results[0].state

    debug_steps: List[Dict[str, str]] = []
    debug_steps.append({"i": "0", "rid": results[0].rid, "state": results[0].state.value, "logic": "<start>"})

    for i in range(1, len(results)):
        logic = "and"
        if logic_to_prev and i < len(logic_to_prev) and (logic_to_prev[i] or "").strip():
            logic = (logic_to_prev[i] or "and").strip().lower()

        before = final_state
        cur = results[i].state

        if logic == "or":
            final_state = _combine_or(final_state, cur)
        else:
            # default to AND (hard)
            final_state = _combine_and(final_state, cur)

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

    # Debug print (noisy)
    print(
        "[chain_eval] rows=%d partial_true=%s final_state=%s"
        % (len(results), partial_true, final_state.value)
    )
    # print detailed chain steps
    for st in debug_steps:
        print("[chain_eval] step=%s" % st)

    return ChainResult(
        partial_true=partial_true,
        final_state=final_state,
        debug={"steps": debug_steps},
    )
