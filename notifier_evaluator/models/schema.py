# notifier_evaluator/models/schema.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import logging
from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field, ValidationError

try:
    # pydantic v2
    from pydantic import ConfigDict, field_validator, model_validator
    _IS_PYD_V2 = True
except Exception:  # pragma: no cover
    ConfigDict = None  # type: ignore[assignment]
    field_validator = None  # type: ignore[assignment]
    model_validator = None  # type: ignore[assignment]
    _IS_PYD_V2 = False

logger = logging.getLogger(__name__)

# Enable noisy debug by setting env var:
#   set NOTIFIER_SCHEMA_DEBUG=1   (Windows)
_SCHEMA_DEBUG = os.getenv("NOTIFIER_SCHEMA_DEBUG", "").strip() in ("1", "true", "yes", "y", "on")


def _dbg(msg: str) -> None:
    if _SCHEMA_DEBUG:
        try:
            print(msg)
        except Exception:
            pass


# Keep these exported because other modules import them.
VALID_INTERVALS = {
    # common crypto intervals
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1mo",
}

VALID_EXCHANGES = {
    # keep this permissive; you can tighten later
    "binance",
    "binance_futures",
    "bybit",
    "kraken",
    "coinbase",
    "okx",
}

_INTERVAL_RE = re.compile(r"^\d+(m|h|d|w|mo)$", re.IGNORECASE)


def _norm_str(x: Any) -> str:
    try:
        return str(x).strip()
    except Exception:
        return ""


def _norm_lower(x: Any) -> str:
    return _norm_str(x).lower()


def _validate_interval_value(v: Optional[str], ctx: str = "") -> Optional[str]:
    if v is None:
        return None
    s = _norm_lower(v)
    if not s:
        return None
    # accept known set OR simple pattern
    if s in VALID_INTERVALS or _INTERVAL_RE.match(s):
        return s
    raise ValueError(f"{ctx}Invalid interval: {v!r}")


def _validate_exchange_value(v: Optional[str], ctx: str = "") -> Optional[str]:
    if v is None:
        return None
    s = _norm_lower(v)
    if not s:
        return None
    # exchange names are “contract-ish” -> validate against allowlist
    if s in VALID_EXCHANGES:
        return s
    raise ValueError(f"{ctx}Invalid exchange: {v!r}")


# ──────────────────────────────────────────────────────────────────────────────
# Models
# Keep them tolerant: extra fields allowed, because UI / migration / future changes.
# ──────────────────────────────────────────────────────────────────────────────

class _Base(BaseModel):
    if _IS_PYD_V2:
        model_config = ConfigDict(extra="allow")  # type: ignore[misc]
    else:
        class Config:
            extra = "allow"


class Threshold(_Base):
    """
    UI/Evaluator threshold object.
    Your authoritative schema: null OR {type: streak|min_count} OR {type: count|window+min_count}
    We'll store in normalized flat form.
    """
    type: Literal["streak", "count"]
    window: Optional[int] = None
    min_count: Optional[int] = None


class Indicator(_Base):
    """
    Indicator object used by conditions.
    Always present fields in NEW schema: {name, output, symbol|null, interval|null, params}
    """
    name: str
    output: str
    symbol: Optional[str] = None
    interval: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)

    if _IS_PYD_V2:
        @field_validator("symbol", "interval", mode="before")
        def _empty_to_none(cls, v: Any) -> Any:
            s = _norm_str(v)
            return None if s in ("", "None", "null") else v

        @field_validator("interval")
        def _validate_interval(cls, v: Optional[str]) -> Optional[str]:
            return _validate_interval_value(v, ctx="Indicator.interval: ")
    else:
        # pydantic v1
        @classmethod
        def _empty_to_none(cls, v: Any) -> Any:
            s = _norm_str(v)
            return None if s in ("", "None", "null") else v

        def __init__(self, **data: Any) -> None:
            if "symbol" in data:
                data["symbol"] = self._empty_to_none(data["symbol"])
            if "interval" in data:
                data["interval"] = self._empty_to_none(data["interval"])
            super().__init__(**data)
            self.interval = _validate_interval_value(self.interval, ctx="Indicator.interval: ")


class Condition(_Base):
    rid: str
    logic: Literal["and", "or"] = "and"   # link to previous row (row[0] ignores it anyway)
    left: Indicator
    op: str
    right: Indicator
    threshold: Optional[Threshold] = None


class AlarmConfig(_Base):
    """
    Alarm config imported by notifier_evaluator.alarms.policy.

    Keep permissive; policy can decide what it uses.
    """
    mode: str = "always_on"          # "always_on" | "auto_off" | "pre_notification" etc.
    cooldown_sec: Optional[int] = None
    edge_only: bool = False          # if True: push only on FALSE->TRUE
    deactivate_on: bool = False      # if True: group deactivates after push

class EngineDefaults(_Base):
    """
    Engine default settings imported by notifier_evaluator.context.resolver.

    This is NOT the profile/group schema. It's runtime defaults that the resolver
    can use when profile/group/row values are blank or missing.

    Keep it permissive and stable.
    """
    default_exchange: str = "binance"
    default_interval: str = "1h"

    # Optional future knobs (safe defaults)
    strict: bool = False

    if _IS_PYD_V2:
        @field_validator("default_exchange")
        def _v_ex(cls, v: str) -> str:
            vv = _validate_exchange_value(v, ctx="EngineDefaults.default_exchange: ")
            return vv or "binance"

        @field_validator("default_interval")
        def _v_it(cls, v: str) -> str:
            vv = _validate_interval_value(v, ctx="EngineDefaults.default_interval: ")
            return vv or "1h"
    else:
        def __init__(self, **data: Any) -> None:
            super().__init__(**data)
            self.default_exchange = _validate_exchange_value(self.default_exchange, ctx="EngineDefaults.default_exchange: ") or "binance"
            self.default_interval = _validate_interval_value(self.default_interval, ctx="EngineDefaults.default_interval: ") or "1h"


class ThresholdConfig(_Base):
    """
    Threshold configuration for a single condition row.

    Supported:
      - None (no threshold)
      - {"type": "streak", "min_count": N}
      - {"type": "count", "window": W, "min_count": C}

    The evaluator uses this to decide if a row's TRUE state should be considered "passed".
    """
    type: str  # "streak" or "count"
    min_count: int = 1
    window: Optional[int] = None

    if _IS_PYD_V2:
        @field_validator("type")
        def _v_type(cls, v: str) -> str:
            vv = (v or "").strip().lower()
            if vv not in ("streak", "count"):
                raise ValidationError(f"ThresholdConfig.type must be 'streak' or 'count', got {v!r}")
            return vv

        @field_validator("min_count")
        def _v_min_count(cls, v: int) -> int:
            try:
                iv = int(v)
            except Exception:
                raise ValidationError(f"ThresholdConfig.min_count must be int, got {v!r}")
            if iv < 1:
                raise ValidationError(f"ThresholdConfig.min_count must be >= 1, got {iv}")
            return iv

        @field_validator("window")
        def _v_window(cls, v: Optional[int], info=None) -> Optional[int]:
            # window only meaningful for type=="count", but allow None otherwise
            if v is None:
                return None
            try:
                iv = int(v)
            except Exception:
                raise ValidationError(f"ThresholdConfig.window must be int, got {v!r}")
            if iv < 1:
                raise ValidationError(f"ThresholdConfig.window must be >= 1, got {iv}")
            return iv

        @model_validator(mode="after")
        def _v_combo(self):
            # For count thresholds, window is required
            if self.type == "count":
                if self.window is None:
                    raise ValidationError("ThresholdConfig.window is required when type='count'")
                if self.min_count > self.window:
                    raise ValidationError("ThresholdConfig.min_count cannot be > window")
            return self
    else:
        def __init__(self, **data: Any) -> None:
            super().__init__(**data)
            self.type = (self.type or "").strip().lower()
            if self.type not in ("streak", "count"):
                raise ValidationError(f"ThresholdConfig.type must be 'streak' or 'count', got {self.type!r}")

            try:
                self.min_count = int(self.min_count)
            except Exception:
                raise ValidationError(f"ThresholdConfig.min_count must be int, got {self.min_count!r}")
            if self.min_count < 1:
                raise ValidationError(f"ThresholdConfig.min_count must be >= 1, got {self.min_count}")

            if self.window is not None:
                try:
                    self.window = int(self.window)
                except Exception:
                    raise ValidationError(f"ThresholdConfig.window must be int, got {self.window!r}")
                if self.window < 1:
                    raise ValidationError(f"ThresholdConfig.window must be >= 1, got {self.window}")

            if self.type == "count":
                if self.window is None:
                    raise ValidationError("ThresholdConfig.window is required when type='count'")
                if self.min_count > self.window:
                    raise ValidationError("ThresholdConfig.min_count cannot be > window")




class Group(_Base):
    """
    Group model imported by notifier_evaluator.context.group_expander and others.
    """
    gid: str
    name: Optional[str] = None
    active: bool = True

    symbol_group: Optional[str] = None
    symbols: Optional[List[str]] = None

    exchange: Optional[str] = None
    interval: str

    telegram_id: Optional[str] = None
    single_mode: bool = False
    deactivate_on: bool = False

    alarm: Optional[AlarmConfig] = None
    conditions: List[Condition] = Field(default_factory=list)

    if _IS_PYD_V2:
        @field_validator("interval")
        def _validate_interval(cls, v: str) -> str:
            vv = _validate_interval_value(v, ctx="Group.interval: ")
            # interval is required -> if None after normalization => error
            if vv is None:
                raise ValueError("Group.interval cannot be blank")
            return vv

        @field_validator("exchange")
        def _validate_exchange(cls, v: Optional[str]) -> Optional[str]:
            return _validate_exchange_value(v, ctx="Group.exchange: ")
    else:
        def __init__(self, **data: Any) -> None:
            super().__init__(**data)
            self.interval = _validate_interval_value(self.interval, ctx="Group.interval: ") or self.interval
            self.exchange = _validate_exchange_value(self.exchange, ctx="Group.exchange: ")


class Profile(_Base):
    """
    Profile model (NEW schema).
    """
    id: str
    name: str
    enabled: bool = True
    groups: List[Group] = Field(default_factory=list)

    if _IS_PYD_V2:
        @model_validator(mode="before")
        def _legacy_condition_groups(cls, data: Any) -> Any:
            # accept legacy payloads to keep evaluator alive
            if isinstance(data, dict):
                if "groups" not in data and "condition_groups" in data:
                    _dbg("[schema] mapped condition_groups -> groups (legacy payload)")
                    data = dict(data)
                    data["groups"] = data.get("condition_groups") or []
            return data
    else:
        def __init__(self, **data: Any) -> None:
            if "groups" not in data and "condition_groups" in data:
                _dbg("[schema] mapped condition_groups -> groups (legacy payload)")
                data["groups"] = data.get("condition_groups") or []
            super().__init__(**data)
