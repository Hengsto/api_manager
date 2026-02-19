# notifier_evaluator/models/runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import math
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from notifier_evaluator.models.schema import VALID_EXCHANGES, VALID_INTERVALS

logger = logging.getLogger(__name__)

# Default stays STRICT to preserve your current behavior.
# Set NE_STRICT_RUNTIME_VALIDATION=0 to switch to warn+continue mode.
STRICT_RUNTIME_VALIDATION = os.getenv("NE_STRICT_RUNTIME_VALIDATION", "1").strip().lower() in ("1", "true", "yes", "y")
DEBUG_RUNTIME = os.getenv("NE_DEBUG_RUNTIME", "").strip().lower() in ("1", "true", "yes", "y")


class RuntimeValidationError(Exception):
    """Raised when runtime validation fails"""
    pass


class TriState(str, Enum):
    TRUE = "true"
    FALSE = "false"
    UNKNOWN = "unknown"


class RowSide(str, Enum):
    LEFT = "left"
    RIGHT = "right"


def _dbg(msg: str) -> None:
    if DEBUG_RUNTIME:
        try:
            print(msg)
        except Exception:
            pass


def _fail_or_warn(msg: str) -> None:
    if STRICT_RUNTIME_VALIDATION:
        raise RuntimeValidationError(msg)
    logger.warning("[runtime] %s", msg)
    _dbg(f"[runtime] WARN {msg}")


def _is_finite_number(x: Any) -> bool:
    try:
        if x is None:
            return True
        fx = float(x)
        return not (math.isnan(fx) or math.isinf(fx))
    except Exception:
        return False


def validate_interval(interval: str, context: str = "") -> None:
    """Validate interval format."""
    if not interval or interval not in VALID_INTERVALS:
        _fail_or_warn(f"{context}Invalid interval format: {interval!r} (valid={sorted(VALID_INTERVALS)})")


def validate_exchange(exchange: str, context: str = "") -> None:
    """Validate exchange name."""
    if not exchange or exchange not in VALID_EXCHANGES:
        _fail_or_warn(f"{context}Invalid exchange: {exchange!r} (valid={sorted(VALID_EXCHANGES)})")


def validate_timestamp(ts: Optional[str], context: str = "") -> None:
    """Validate timestamp format."""
    if ts is not None and not str(ts).strip():
        _fail_or_warn(f"{context}Invalid timestamp: {ts!r}")


@dataclass(frozen=True)
class ResolvedContext:
    """Runtime-only resolved context."""
    symbol: str
    interval: str
    exchange: str
    clock_interval: str
    source: Optional[str] = None  # e.g. "price_api", "local", "cache"

    def __post_init__(self) -> None:
        if not self.symbol or not str(self.symbol).strip():
            _fail_or_warn("ResolvedContext.symbol cannot be empty")
        validate_interval(self.interval, "ResolvedContext.interval: ")
        validate_exchange(self.exchange, "ResolvedContext.exchange: ")
        validate_interval(self.clock_interval, "ResolvedContext.clock_interval: ")


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

    def __post_init__(self) -> None:
        if not self.profile_id or not str(self.profile_id).strip():
            _fail_or_warn("StatusKey.profile_id cannot be empty")
        if not self.gid or not str(self.gid).strip():
            _fail_or_warn("StatusKey.gid cannot be empty")
        if not self.symbol or not str(self.symbol).strip():
            _fail_or_warn("StatusKey.symbol cannot be empty")
        validate_exchange(self.exchange, "StatusKey.exchange: ")
        validate_interval(self.clock_interval, "StatusKey.clock_interval: ")


@dataclass
class FetchResult:
    """Normalisierte Antwort aus price_api / indicator client."""
    ok: bool
    latest_value: Optional[float]
    latest_ts: Optional[str]
    series: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.latest_value is not None and not _is_finite_number(self.latest_value):
            _fail_or_warn("FetchResult.latest_value must be a finite number")
        validate_timestamp(self.latest_ts, "FetchResult.latest_ts: ")
        if self.series is not None:
            if not isinstance(self.series, list):
                _fail_or_warn("FetchResult.series must be a list")
            else:
                for i, item in enumerate(self.series):
                    if not isinstance(item, dict):
                        _fail_or_warn(f"FetchResult.series[{i}] items must be dictionaries")


@dataclass
class ConditionResult:
    rid: str
    state: TriState
    op: str
    left_value: Optional[float]
    right_value: Optional[float]
    reason: str = ""
    debug: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.rid or not str(self.rid).strip():
            _fail_or_warn("ConditionResult.rid cannot be empty")
        if not isinstance(self.state, TriState):
            _fail_or_warn("ConditionResult.state must be a TriState")
        if self.left_value is not None and not _is_finite_number(self.left_value):
            _fail_or_warn("ConditionResult.left_value must be a finite number")
        if self.right_value is not None and not _is_finite_number(self.right_value):
            _fail_or_warn("ConditionResult.right_value must be a finite number")


@dataclass
class ChainResult:
    partial_true: bool
    final_state: TriState
    debug: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.final_state, TriState):
            _fail_or_warn("ChainResult.final_state must be a TriState")


@dataclass
class StatusState:
    """Status pro (profile_id, gid, symbol, exchange)."""
    active: bool = True

    # threshold state
    streak_current: int = 0
    count_window: List[bool] = field(default_factory=list)  # rolling window
    max_window_size: int = 100  # Prevent unbounded growth

    # timestamps
    last_true_ts: Optional[str] = None
    last_push_ts: Optional[str] = None
    last_tick_ts: Optional[str] = None

    # edge gating / pre-notification
    last_final_state: TriState = TriState.UNKNOWN
    last_partial_true: bool = False

    # debug info
    last_reason: str = ""
    last_debug: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate initial state."""
        if self.streak_current < 0:
            _fail_or_warn("StatusState.streak_current cannot be negative")
        if not isinstance(self.count_window, list):
            _fail_or_warn("StatusState.count_window must be a list")
        if self.max_window_size < 1:
            _fail_or_warn("StatusState.max_window_size must be positive")
        validate_timestamp(self.last_true_ts, "StatusState.last_true_ts: ")
        validate_timestamp(self.last_push_ts, "StatusState.last_push_ts: ")
        validate_timestamp(self.last_tick_ts, "StatusState.last_tick_ts: ")
        if not isinstance(self.last_final_state, TriState):
            _fail_or_warn("StatusState.last_final_state must be a TriState")

    def update_state(
        self,
        final_state: TriState,
        partial_true: bool,
        ts: str,
        reason: str = "",
        debug: Optional[Dict[str, Any]] = None
    ) -> None:
        """Update state with new evaluation results."""
        validate_timestamp(ts, "StatusState.update_state.ts: ")

        # Update timestamps
        self.last_tick_ts = ts
        if final_state == TriState.TRUE:
            self.last_true_ts = ts

        # Update streak
        if final_state == TriState.TRUE:
            self.streak_current += 1
        else:
            self.streak_current = 0

        # Update count window (maintain max size)
        self.count_window.append(final_state == TriState.TRUE)
        if len(self.count_window) > self.max_window_size:
            self.count_window.pop(0)

        # Update state
        self.last_final_state = final_state
        self.last_partial_true = partial_true
        self.last_reason = reason
        if debug is not None:
            self.last_debug = debug

        _dbg(
            f"[runtime] StatusState.update_state final={final_state.value} partial={partial_true} "
            f"ts={ts} streak={self.streak_current} window={len(self.count_window)}"
        )

    def deactivate(self, ts: str, reason: str = "") -> None:
        """Deactivate the state."""
        validate_timestamp(ts, "StatusState.deactivate.ts: ")
        self.active = False
        self.last_tick_ts = ts
        self.last_reason = reason or "deactivated"
        _dbg(f"[runtime] StatusState.deactivate ts={ts} reason={self.last_reason!r}")

    def reset(self) -> None:
        """Reset state to initial values."""
        self.streak_current = 0
        self.count_window.clear()
        self.last_final_state = TriState.UNKNOWN
        self.last_partial_true = False
        self.last_reason = ""
        self.last_debug.clear()
        _dbg("[runtime] StatusState.reset")

    def get_window_stats(self) -> Dict[str, int]:
        """Get statistics about the count window."""
        true_count = sum(1 for x in self.count_window if x)
        return {
            "window_size": len(self.count_window),
            "true_count": true_count,
            "false_count": len(self.count_window) - true_count
        }


@dataclass(frozen=True)
class HistoryEvent:
    ts: str
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

    def __post_init__(self) -> None:
        validate_timestamp(self.ts, "HistoryEvent.ts: ")
        if not self.profile_id or not str(self.profile_id).strip():
            _fail_or_warn("HistoryEvent.profile_id cannot be empty")
        if not self.gid or not str(self.gid).strip():
            _fail_or_warn("HistoryEvent.gid cannot be empty")
        if not self.symbol or not str(self.symbol).strip():
            _fail_or_warn("HistoryEvent.symbol cannot be empty")
        validate_exchange(self.exchange, "HistoryEvent.exchange: ")
        if not self.event or not str(self.event).strip():
            _fail_or_warn("HistoryEvent.event cannot be empty")
        if self.left_value is not None and not _is_finite_number(self.left_value):
            _fail_or_warn("HistoryEvent.left_value must be a finite number")
        if self.right_value is not None and not _is_finite_number(self.right_value):
            _fail_or_warn("HistoryEvent.right_value must be a finite number")


def now_ts() -> float:
    """Get current timestamp."""
    return time.time()


def is_nan(x: Any) -> bool:
    """Check if value is NaN."""
    try:
        return isinstance(x, float) and math.isnan(x)
    except Exception:
        return False


def safe_float(x: Any) -> Optional[float]:
    """Convert to float safely."""
    try:
        if x is None:
            return None
        fx = float(x)
        if math.isnan(fx) or math.isinf(fx):
            return None
        return fx
    except Exception:
        return None
