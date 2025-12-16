# notifier_evaluator/models/runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Literal, Tuple
from enum import Enum
import math
import time


class TriState(str, Enum):
    TRUE = "true"
    FALSE = "false"
    UNKNOWN = "unknown"


Side = Literal["left", "right"]


@dataclass(frozen=True)
class ResolvedContext:
    """
    Runtime-only resolved context.
    clock_interval ist die Engine-Tick-Clock (meist group.interval),
    NICHT zwingend left/right interval.
    """
    symbol: str
    interval: str
    exchange: str
    clock_interval: str


@dataclass(frozen=True)
class ResolvedPair:
    left: ResolvedContext
    right: ResolvedContext


@dataclass(frozen=True)
class RequestKey:
    """
    Dedupe Key für Fetch.
    mode:
      - latest: immer latest candle/value
      - as_of: bis zu einem timestamp
    """
    kind: Literal["indicator", "price"]  # value braucht keinen fetch
    name: str  # indicator name oder "price"
    symbol: str
    interval: str
    exchange: str

    params: Tuple[Tuple[str, Any], ...] = field(default_factory=tuple)
    output: Optional[str] = None
    count: int = 1

    mode: Literal["latest", "as_of"] = "latest"
    as_of: Optional[float] = None  # unix ts wenn mode=as_of


@dataclass
class FetchResult:
    """
    Normalisierte Antwort aus price_api.
    - value: letzter Wert (oder None)
    - ts_last: timestamp des letzten Werts (unix float oder None)
    - series: optional Liste von (ts, value) für count>1
    - ok: ob fetch erfolgreich war
    """
    ok: bool
    value: Optional[float]
    ts_last: Optional[float]
    series: Optional[List[Tuple[float, Optional[float]]]] = None
    raw: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class ConditionEvalResult:
    rid: str
    state: TriState
    op: str

    left_value: Optional[float]
    right_value: Optional[float]

    reason: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChainEvalResult:
    partial_true: bool
    final_state: TriState
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ThresholdEvalResult:
    passed: bool
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StatusState:
    """
    Status pro (profile_id, gid, symbol, exchange) — NICHT im Profil-JSON.
    """
    active: bool = True

    # threshold state
    streak_current: int = 0
    count_window: List[bool] = field(default_factory=list)  # rolling window

    # timestamps
    last_true_ts: Optional[float] = None
    last_push_ts: Optional[float] = None
    last_tick_ts: Optional[float] = None

    # for edge gating / pre-notification change detection
    last_final_state: TriState = TriState.UNKNOWN
    last_partial_true: bool = False


@dataclass(frozen=True)
class HistoryEvent:
    ts: float
    profile_id: str
    gid: str
    symbol: str
    exchange: str

    event: str  # e.g. "eval", "push", "deactivate", "invalid"
    partial_true: Optional[bool] = None
    final_state: Optional[str] = None
    threshold_passed: Optional[bool] = None

    left_value: Optional[float] = None
    right_value: Optional[float] = None
    op: Optional[str] = None

    threshold_state: Dict[str, Any] = field(default_factory=dict)
    debug: Dict[str, Any] = field(default_factory=dict)


def now_ts() -> float:
    return time.time()


def is_nan(x: Any) -> bool:
    try:
        return isinstance(x, float) and math.isnan(x)
    except Exception:
        return False
