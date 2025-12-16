# notifier_evaluator/context/tick.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from notifier_evaluator.models.runtime import StatusKey, StatusState


# ──────────────────────────────────────────────────────────────────────────────
# Tick Detection
#
# Definition:
# - Tick = "neue Kerze" im clock_interval
# - Wir brauchen eine deterministische tick_ts (string), die wir in Status speichern.
#
# Input:
# - status_state.last_tick_ts
# - current_tick_ts (aus Fetch-Meta oder series last candle timestamp)
#
# Output:
# - new_tick bool
# - updated last_tick_ts
#
# Wichtig:
# - Der Tick muss pro (profile_id,gid,symbol,clock_interval,exchange) laufen.
#   -> StatusKey enthält clock_interval
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class TickResult:
    new_tick: bool
    tick_ts: Optional[str]
    reason: str


def detect_new_tick(
    *,
    skey: StatusKey,
    state: StatusState,
    current_tick_ts: Optional[str],
) -> TickResult:
    """
    Detects if we have a new tick.
    current_tick_ts should be the timestamp of the latest candle in clock_interval.

    If current_tick_ts is None:
      -> new_tick False, reason missing_tick_ts
    """
    if current_tick_ts is None or str(current_tick_ts).strip() == "":
        print(
            "[tick] NO_TS profile=%s gid=%s symbol=%s ex=%s clock=%s last=%s"
            % (skey.profile_id, skey.gid, skey.symbol, skey.exchange, skey.clock_interval, state.last_tick_ts)
        )
        return TickResult(new_tick=False, tick_ts=None, reason="missing_tick_ts")

    cur = str(current_tick_ts).strip()
    last = (state.last_tick_ts or "").strip() or None

    if last is None:
        # First observation -> treat as new tick, initialize.
        state.last_tick_ts = cur
        print(
            "[tick] FIRST profile=%s gid=%s symbol=%s ex=%s clock=%s tick=%s"
            % (skey.profile_id, skey.gid, skey.symbol, skey.exchange, skey.clock_interval, cur)
        )
        return TickResult(new_tick=True, tick_ts=cur, reason="first_tick")

    if cur != last:
        # New candle detected
        prev = last
        state.last_tick_ts = cur
        print(
            "[tick] NEW profile=%s gid=%s symbol=%s ex=%s clock=%s prev=%s cur=%s"
            % (skey.profile_id, skey.gid, skey.symbol, skey.exchange, skey.clock_interval, prev, cur)
        )
        return TickResult(new_tick=True, tick_ts=cur, reason="tick_changed")

    # Same candle
    print(
        "[tick] SAME profile=%s gid=%s symbol=%s ex=%s clock=%s tick=%s"
        % (skey.profile_id, skey.gid, skey.symbol, skey.exchange, skey.clock_interval, cur)
    )
    return TickResult(new_tick=False, tick_ts=cur, reason="same_tick")
