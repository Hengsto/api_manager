# notifier_evaluator/eval/threshold.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from notifier_evaluator.models.schema import ThresholdConfig
from notifier_evaluator.models.runtime import StatusState, TriState


@dataclass
class ThresholdResult:
    passed: bool
    reason: str
    debug: Dict[str, object]


def apply_threshold(
    *,
    final_state: TriState,
    new_tick: bool,
    cfg: Optional[ThresholdConfig],
    state: StatusState,
    now_ts: Optional[str] = None,
) -> ThresholdResult:
    if cfg is None:
        passed = final_state == TriState.TRUE
        if passed and now_ts:
            state.last_true_ts = now_ts
        print(f"[evaluator][DBG] threshold none final={final_state.value} passed={passed}")
        return ThresholdResult(passed=passed, reason="none", debug={"mode": "none"})

    if not new_tick:
        return ThresholdResult(
            passed=False,
            reason="no_new_tick",
            debug={"type": cfg.type, "min_count": cfg.min_count, "window": cfg.window},
        )

    is_true = final_state == TriState.TRUE

    if cfg.type == "streak":
        if is_true:
            state.streak_current += 1
            if now_ts:
                state.last_true_ts = now_ts
        else:
            state.streak_current = 0
        return ThresholdResult(
            passed=state.streak_current >= cfg.min_count,
            reason="streak",
            debug={"type": cfg.type, "streak_current": state.streak_current, "min_count": cfg.min_count},
        )

    state.count_window.append(is_true)
    if cfg.window is not None and len(state.count_window) > cfg.window:
        state.count_window = state.count_window[-cfg.window:]
    trues = sum(1 for x in state.count_window if x)
    if is_true and now_ts:
        state.last_true_ts = now_ts
    return ThresholdResult(
        passed=trues >= cfg.min_count,
        reason="count",
        debug={"type": cfg.type, "window": cfg.window, "min_count": cfg.min_count, "trues": trues},
    )
