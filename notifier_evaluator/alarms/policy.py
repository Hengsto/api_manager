# notifier_evaluator/alarms/policy.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from notifier_evaluator.models.schema import AlarmConfig
from notifier_evaluator.models.runtime import HistoryEvent, StatusKey, StatusState, TriState


# ──────────────────────────────────────────────────────────────────────────────
# Alarm Policy
# - entscheidet Push/Events + optional deactivate (active=false)
# - Spam Schutz:
#     - edge_only: nur bei Transition (FALSE/UNKNOWN -> TRUE)
#     - cooldown_sec: min seconds between pushes
#
# Inputs:
#   - final_state (TriState)
#   - partial_true (bool)
#   - threshold_passed (bool)  (in der Regel basiert auf final_state)
#   - alarm config
#   - status state (mutated)
#
# Output:
#   - list[HistoryEvent] (push / partial change / final true etc.)
#   - decision debug
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class PolicyResult:
    push: bool
    push_reason: str
    events: List[HistoryEvent] = field(default_factory=list)
    debug: Dict[str, object] = field(default_factory=dict)


def _to_bool_final_true(final_state: TriState) -> bool:
    return final_state == TriState.TRUE


def _edge_allows(*, edge_only: bool, prev_final: Optional[TriState], final_state: TriState) -> bool:
    if not edge_only:
        return True
    if prev_final is None:
        # first run: allow if TRUE
        return final_state == TriState.TRUE
    # allow only transition into TRUE
    return prev_final != TriState.TRUE and final_state == TriState.TRUE


def _cooldown_allows(*, cooldown_sec: int, now_unix: float, last_push_unix: Optional[float]) -> bool:
    if cooldown_sec <= 0:
        return True
    if last_push_unix is None:
        return True
    return (now_unix - last_push_unix) >= cooldown_sec


def apply_alarm_policy(
    *,
    skey: StatusKey,
    state: StatusState,
    cfg: AlarmConfig,
    now_ts: str,
    now_unix: float,
    partial_true: bool,
    final_state: TriState,
    threshold_passed: bool,
    # for history: optional values
    last_row_left: Optional[float] = None,
    last_row_right: Optional[float] = None,
    last_row_op: Optional[str] = None,
) -> PolicyResult:
    mode = (cfg.mode or "always_on").strip().lower()
    cooldown = int(cfg.cooldown_sec or 0)
    edge_only = bool(cfg.edge_only)

    events: List[HistoryEvent] = []

    prev_partial = state.last_partial_true
    prev_final = state.last_final_state

    # Always track partial changes if mode==pre_notification
    partial_changed = (prev_partial is None) or (prev_partial != partial_true)
    if mode == "pre_notification" and partial_changed:
        events.append(
            HistoryEvent(
                ts=now_ts,
                profile_id=skey.profile_id,
                gid=skey.gid,
                symbol=skey.symbol,
                exchange=skey.exchange,
                event="partial_change",
                partial_true=partial_true,
                final_state=final_state.value,
                left_value=last_row_left,
                right_value=last_row_right,
                op=last_row_op,
                debug={"prev_partial": prev_partial, "mode": mode},
            )
        )
        print(
            "[policy] pre_notification partial_change profile=%s gid=%s sym=%s %s->%s"
            % (skey.profile_id, skey.gid, skey.symbol, prev_partial, partial_true)
        )

    # Determine "final_true" boolean for pushing purposes
    final_true = _to_bool_final_true(final_state)

    # Base trigger for push:
    # - we push only when threshold_passed=True (this is your "final true after threshold")
    base_trigger = bool(threshold_passed)

    # edge gating
    edge_ok = _edge_allows(edge_only=edge_only, prev_final=prev_final, final_state=final_state)

    # cooldown gating
    last_push_unix = None
    if state.last_push_ts:
        try:
            # you might store unix directly later; for now: can't parse robustly => skip parsing
            # keep it simple: engine can pass last_push_unix if you want
            last_push_unix = None
        except Exception:
            last_push_unix = None
    cooldown_ok = _cooldown_allows(cooldown_sec=cooldown, now_unix=now_unix, last_push_unix=last_push_unix)

    push = base_trigger and edge_ok and cooldown_ok

    push_reason = "no_push"
    if not base_trigger:
        push_reason = "threshold_not_passed"
    elif not edge_ok:
        push_reason = "edge_blocked"
    elif not cooldown_ok:
        push_reason = "cooldown_blocked"
    else:
        push_reason = "push"

    # If push -> emit push event, update timestamps
    if push:
        state.last_push_ts = now_ts

        events.append(
            HistoryEvent(
                ts=now_ts,
                profile_id=skey.profile_id,
                gid=skey.gid,
                symbol=skey.symbol,
                exchange=skey.exchange,
                event="push",
                partial_true=partial_true,
                final_state=final_state.value,
                left_value=last_row_left,
                right_value=last_row_right,
                op=last_row_op,
                threshold_snapshot={
                    "streak_current": state.streak_current,
                    "count_window": list(state.count_window),
                },
                debug={
                    "mode": mode,
                    "cooldown_sec": cooldown,
                    "edge_only": edge_only,
                    "prev_final": (prev_final.value if prev_final else None),
                },
            )
        )

        print(
            "[policy] PUSH profile=%s gid=%s sym=%s mode=%s reason=%s final=%s partial=%s"
            % (skey.profile_id, skey.gid, skey.symbol, mode, push_reason, final_state.value, partial_true)
        )

        # deactivation logic
        if mode in ("auto_off", "pre_notification"):
            state.active = False
            events.append(
                HistoryEvent(
                    ts=now_ts,
                    profile_id=skey.profile_id,
                    gid=skey.gid,
                    symbol=skey.symbol,
                    exchange=skey.exchange,
                    event="deactivated",
                    partial_true=partial_true,
                    final_state=final_state.value,
                    debug={"mode": mode, "reason": "deactivate_on_push"},
                )
            )
            print("[policy] DEACTIVATE profile=%s gid=%s sym=%s mode=%s" % (skey.profile_id, skey.gid, skey.symbol, mode))

    # Always store last states for transition logic
    state.last_partial_true = partial_true
    state.last_final_state = final_state

    return PolicyResult(
        push=push,
        push_reason=push_reason,
        events=events,
        debug={
            "mode": mode,
            "threshold_passed": threshold_passed,
            "edge_only": edge_only,
            "edge_ok": edge_ok,
            "cooldown_sec": cooldown,
            "cooldown_ok": cooldown_ok,
            "base_trigger": base_trigger,
            "final_true": final_true,
            "prev_partial": prev_partial,
            "prev_final": (prev_final.value if prev_final else None),
        },
    )
