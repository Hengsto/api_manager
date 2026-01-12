# notifier_evaluator/models/runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class TriState(str, Enum):
    TRUE = "true"
    FALSE = "false"
    UNKNOWN = "unknown"


class RowSide(str, Enum):
    LEFT = "left"
    RIGHT = "right"


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
class StatusKey:
    """Key für Status/Cooldown/Tick State."""

    profile_id: str
    gid: str
    symbol: str
    exchange: str
    clock_interval: str


@dataclass
class FetchResult:
    """
    Normalisierte Antwort aus price_api / indicator client.

    latest_value / latest_ts: letzter Punkt der Serie.
    series: optionale Rohserie, falls count>1 geplant wurde.
    meta: Debug/Tracing (z. B. RequestKey short, output)
    """

    ok: bool
    latest_value: Optional[float]
    latest_ts: Optional[str]
    series: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConditionResult:
    rid: str
    state: TriState
    op: str

    left_value: Optional[float]
    right_value: Optional[float]

    reason: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChainResult:
    partial_true: bool
    final_state: TriState
    debug: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StatusState:
    """
    Status pro (profile_id, gid, symbol, exchange).
    Wird NICHT im Profil-JSON gespeichert.
    """

    active: bool = True

    # threshold state
    streak_current: int = 0
    count_window: List[bool] = field(default_factory=list)  # rolling window

    # timestamps
    last_true_ts: Optional[float] = None
    last_push_ts: Optional[float] = None
    last_tick_ts: Optional[str] = None

    # for edge gating / pre-notification change detection
    last_final_state: TriState = TriState.UNKNOWN
    last_partial_true: bool = False

    # debug bookkeeping
    last_reason: str = ""
    last_debug: Dict[str, Any] = field(default_factory=dict)


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

    rid: Optional[str] = None
    left_value: Optional[float] = None
    right_value: Optional[float] = None
    op: Optional[str] = None

    threshold_snapshot: Dict[str, Any] = field(default_factory=dict)
    debug: Dict[str, Any] = field(default_factory=dict)


def now_ts() -> float:
    return time.time()


def is_nan(x: Any) -> bool:
    try:
        return isinstance(x, float) and math.isnan(x)
    except Exception:
        return False


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        fx = float(x)
        return fx if not math.isnan(fx) else None
    except Exception:
        return None
