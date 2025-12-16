# notifier_evaluator/eval/threshold.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from notifier_evaluator.models.schema import ThresholdConfig
from notifier_evaluator.models.runtime import StatusState, TriState


# ──────────────────────────────────────────────────────────────────────────────
# Thresholding (tick-gated)
#
# Input:
#   - final_state: TriState
#   - new_tick: bool
#   - cfg: ThresholdConfig
#   - state: StatusState (mutated)
#
# Output:
#   - threshold_passed: bool
#   - debug dict
#
# Rules:
# - Threshold advances ONLY when new_tick=True
# - final_state must be TRUE to count as success
# - UNKNOWN does not count as TRUE and does not reset streak by default
#   (but we keep it visible in debug; you can decide later)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class ThresholdResult:
    passed: bool
    reason: str
    debug: Dict[str, object]


def apply_threshold(
    *,
    final_state: TriState,
    new_tick: bool,
    cfg: ThresholdConfig,
    state: StatusState,
    now_ts: Optional[str] = None,
) -> ThresholdResult:
    mode = (cfg.mode or "none").strip().lower()

    # Default: none => immediate pass only if TRUE (no tick gating needed)
    if mode == "none":
        passed = final_state == TriState.TRUE
        if passed and now_ts:
            state.last_true_ts = now_ts
        print("[threshold] mode=none final=%s -> passed=%s" % (final_state.value, passed))
        return ThresholdResult(
            passed=passed,
            reason="none",
            debug={"mode": mode, "final_state": final_state.value},
        )

    # For streak/count: tick-gated
    if not new_tick:
        print("[threshold] mode=%s new_tick=False -> no_advance" % mode)
        return ThresholdResult(
            passed=False,
            reason="no_new_tick",
            debug={
                "mode": mode,
                "final_state": final_state.value,
                "streak_current": state.streak_current,
                "count_window_len": len(state.count_window),
            },
        )

    # new_tick=True => advance
    is_true = final_state == TriState.TRUE
    is_false = final_state == TriState.FALSE
    is_unknown = final_state == TriState.UNKNOWN

    if mode == "streak":
        n = cfg.streak_n or 1
        if is_true:
            state.streak_current += 1
            if now_ts:
                state.last_true_ts = now_ts
        elif is_false:
            state.streak_current = 0
        else:
            # UNKNOWN: do not advance positively; do not reset by default
            # (so your system doesn't get "stuck false" because of one missing datapoint)
            pass

        passed = state.streak_current >= n
        print(
            "[threshold] mode=streak n=%s final=%s -> streak=%s passed=%s"
            % (n, final_state.value, state.streak_current, passed)
        )
        return ThresholdResult(
            passed=passed,
            reason="streak",
            debug={
                "mode": mode,
                "n": n,
                "final_state": final_state.value,
                "streak_current": state.streak_current,
                "unknown_behavior": "no_reset_no_increment",
            },
        )

    if mode == "count":
        w = cfg.window_ticks or 1
        c = cfg.count_required or 1

        # append only TRUE ticks; FALSE ticks append False; UNKNOWN append False (strict)
        # If you want UNKNOWN to not count at all, you’d need a tri-window. Keeping bool window is simpler.
        val = True if is_true else False
        state.count_window.append(val)

        # keep last w entries
        if len(state.count_window) > w:
            state.count_window = state.count_window[-w:]

        trues = sum(1 for x in state.count_window if x)
        passed = trues >= c

        if is_true and now_ts:
            state.last_true_ts = now_ts

        print(
            "[threshold] mode=count w=%s c=%s final=%s -> window_len=%s trues=%s passed=%s"
            % (w, c, final_state.value, len(state.count_window), trues, passed)
        )
        return ThresholdResult(
            passed=passed,
            reason="count",
            debug={
                "mode": mode,
                "window_ticks": w,
                "count_required": c,
                "final_state": final_state.value,
                "window": list(state.count_window),
                "trues": trues,
            },
        )

    # unknown mode
    print("[threshold] WARN unknown_mode=%s final=%s" % (mode, final_state.value))
    return ThresholdResult(
        passed=False,
        reason=f"unknown_mode:{mode}",
        debug={"mode": mode, "final_state": final_state.value},
    )
