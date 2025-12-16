# notifier_evaluator/alarms/formatter.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from notifier_evaluator.models.runtime import HistoryEvent


# ──────────────────────────────────────────────────────────────────────────────
# Formatter
# - baut aus HistoryEvent (push/eval/partial_change) ein Message Payload
# - KEIN Versand hier
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class FormattedMessage:
    """
    Generic message payload.
    """
    text: str
    title: str
    level: str  # "info" | "warn" | "alert"
    meta: Dict[str, Any]


def _fmt_num(x: Any) -> str:
    try:
        if x is None:
            return "None"
        # keep floats readable
        if isinstance(x, float):
            return f"{x:.6g}"
        return str(x)
    except Exception:
        return "<fmt_err>"


def format_event(event: HistoryEvent) -> FormattedMessage:
    """
    Formats a history event into a user-facing message.

    event.event:
      - "push"
      - "partial_change"
      - "deactivated"
      - "eval" (usually not pushed)
    """
    et = (event.event or "").strip().lower()
    level = "info"
    title = "Notifier"
    if et == "push":
        level = "alert"
        title = "🚨 Alert"
    elif et == "partial_change":
        level = "warn"
        title = "⚠️ Pre-Notification"
    elif et == "deactivated":
        level = "info"
        title = "🧯 Auto-Off"

    header = f"{title} | {event.profile_id} / {event.gid} | {event.symbol} ({event.exchange})"
    state_line = f"state={event.final_state} partial={event.partial_true}"
    comp_line = ""
    if event.op:
        comp_line = f"{_fmt_num(event.left_value)} {event.op} {_fmt_num(event.right_value)}"

    thr = event.threshold_snapshot or {}
    thr_line = ""
    if thr:
        thr_line = f"threshold={thr.get('mode')} passed={thr.get('passed')} streak={thr.get('streak_current')} trues={_fmt_num(thr.get('trues'))}"

    # include tick info if present
    tick_line = ""
    if "tick_ts" in thr or "new_tick" in thr:
        tick_line = f"tick=new={thr.get('new_tick')} ts={thr.get('tick_ts')}"

    # short debug reason
    dbg = event.debug or {}
    reason = dbg.get("policy_reason") or dbg.get("threshold_reason") or dbg.get("tick_reason") or ""
    reason_line = f"reason={reason}" if reason else ""

    lines = [header, f"ts={event.ts}", state_line]
    if comp_line:
        lines.append(comp_line)
    if thr_line:
        lines.append(thr_line)
    if tick_line:
        lines.append(tick_line)
    if reason_line:
        lines.append(reason_line)

    text = "\n".join(lines)

    meta = {
        "event": et,
        "profile_id": event.profile_id,
        "gid": event.gid,
        "symbol": event.symbol,
        "exchange": event.exchange,
        "ts": event.ts,
        "final_state": event.final_state,
        "partial_true": event.partial_true,
        "left_value": event.left_value,
        "right_value": event.right_value,
        "op": event.op,
        "threshold_snapshot": thr,
        "debug": dbg,
    }

    print("[formatter] event=%s title=%s level=%s text_len=%d" % (et, title, level, len(text)))
    return FormattedMessage(text=text, title=title, level=level, meta=meta)
