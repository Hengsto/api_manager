# notifier_evaluator/models/schema.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, Literal

from pydantic import BaseModel, Field, ConfigDict, field_validator


# ──────────────────────────────────────────────────────────────────────────────
# Runtime validation helpers (used by models.runtime, resolver, etc.)
# Keep these even if profile JSON is strict-new-schema, because runtime still
# wants cheap sanity checks.
# ──────────────────────────────────────────────────────────────────────────────

# You can extend these later or load from registry; for now keep minimal + common.
VALID_EXCHANGES = {
    "",
    "binance",
    "binance_spot",
    "binance_futures",
    "bybit",
    "okx",
    "kucoin",
    "coinbase",
    "kraken",
}

VALID_INTERVALS = {
    "",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
}


# ──────────────────────────────────────────────────────────────────────────────
# NEW JSON Schema Models (STRICT)
# - Persisted profile JSON: Profile -> groups[] -> conditions[]
# - Runtime config models still live here (EngineDefaults, AlarmConfig)
# - Unknown keys forbidden
# ──────────────────────────────────────────────────────────────────────────────


class EngineDefaults(BaseModel):
    """
    Runtime defaults used by resolver/engine as fallback when profile/group/row doesn't specify something.
    This is NOT the persisted profile JSON schema — it's engine runtime config.
    """
    model_config = ConfigDict(extra="forbid")

    exchange: str = ""
    interval: str = ""
    clock_interval: str = ""

    # Optional: default candle source (used by some indicators like price)
    source: str = ""


class AlarmConfig(BaseModel):
    """
    Runtime/group-level alarm behavior configuration.

    NOTE:
    Your NEW persisted JSON currently uses:
      - group.deactivate_on (e.g. "auto_off")
      - group.telegram_id
      - group.single_mode

    If you later want, you can map those fields into AlarmConfig inside engine/resolver,
    but keeping AlarmConfig here unblocks imports and keeps engine.policy stable.
    """
    model_config = ConfigDict(extra="forbid")

    mode: Literal["always_on", "auto_off", "pre_notification"] = "always_on"
    cooldown_sec: int = Field(default=0, ge=0)
    edge_only: bool = True


class IndicatorRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    output: str
    symbol: Optional[str] = None
    interval: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)


class ThresholdConfig(BaseModel):
    """
    Per-condition threshold.
    NEW: threshold belongs to the condition, not the group.
    """
    model_config = ConfigDict(extra="forbid")

    type: Literal["streak", "count"]
    min_count: int = Field(..., ge=1)

    # only for type="count"
    window: Optional[int] = Field(default=None, ge=1)

    @field_validator("window")
    @classmethod
    def _window_required_for_count(cls, v, info):
        t = info.data.get("type")
        if t == "count" and v is None:
            raise ValueError("threshold.window is required when type='count'")
        if t == "streak" and v is not None:
            raise ValueError("threshold.window must be null/omitted when type='streak'")
        return v


class Condition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rid: str
    logic: Literal["and", "or"] = "and"
    left: IndicatorRef
    op: Literal["gt", "gte", "lt", "lte", "eq", "ne"]
    right: IndicatorRef
    threshold: Optional[ThresholdConfig] = None


class Group(BaseModel):
    model_config = ConfigDict(extra="forbid")

    gid: str
    name: str = ""
    description: str = ""

    active: bool = True

    symbol_group: Optional[str] = None
    symbols: Optional[List[str]] = None

    exchange: Optional[str] = None
    interval: str

    telegram_id: Optional[Union[str, int]] = None

    single_mode: str = "symbol"
    deactivate_on: str = "auto_off"

    # NEW JSON uses "conditions"
    conditions: List[Condition] = Field(default_factory=list)

    # Optional runtime config (NOT required in JSON)
    alarm: Optional[AlarmConfig] = None


class Profile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str = ""
    enabled: bool = True
    groups: List[Group] = Field(default_factory=list)
