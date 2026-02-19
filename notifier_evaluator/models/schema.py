# notifier_evaluator/models/schema.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1mo",
}

VALID_EXCHANGES = {
    "binance",
    "binance_futures",
    "bybit",
    "kraken",
    "coinbase",
    "okx",
}


class _StrictBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ThresholdConfig(_StrictBase):
    type: Literal["streak", "count"]
    min_count: int
    window: Optional[int] = None

    @model_validator(mode="after")
    def _validate_threshold(self) -> "ThresholdConfig":
        if self.min_count < 1:
            raise ValueError("threshold.min_count must be >= 1")
        if self.type == "streak":
            if self.window is not None:
                raise ValueError("threshold.window must be null for type='streak'")
            return self

        if self.window is None:
            raise ValueError("threshold.window is required for type='count'")
        if self.window < 1:
            raise ValueError("threshold.window must be >= 1")
        if self.min_count > self.window:
            raise ValueError("threshold.min_count must be <= threshold.window")
        return self


class IndicatorRef(_StrictBase):
    name: str
    output: str
    symbol: Optional[str] = None
    interval: Optional[str] = None
    params: Dict[str, Any]


class Condition(_StrictBase):
    rid: str
    logic: Literal["and", "or"]
    left: IndicatorRef
    op: Literal["gt", "gte", "lt", "lte", "eq", "ne"]
    right: IndicatorRef
    threshold: Optional[ThresholdConfig] = None


class Group(_StrictBase):
    gid: str
    name: str
    description: str
    active: bool
    symbol_group: Optional[str] = None
    symbols: Optional[List[str]] = None
    exchange: Optional[str] = None
    interval: str
    telegram_id: Optional[Union[str, int]] = None
    single_mode: str
    deactivate_on: str
    conditions: List[Condition]


class Profile(_StrictBase):
    id: str
    name: str
    enabled: bool
    groups: List[Group]


class AlarmConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: str = "always_on"
    cooldown_sec: Optional[int] = None
    edge_only: bool = False


class EngineDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_exchange: str = "binance"
    default_interval: str = "1h"


def validate_profiles_payload(payload: Any) -> List[Profile]:
    if not isinstance(payload, list):
        raise ValidationError.from_exception_data(
            "ProfilePayloadError",
            [{"type": "value_error", "loc": ("profiles",), "msg": "profiles payload must be a list", "input": payload}],
        )
    return [Profile.model_validate(item) for item in payload]
