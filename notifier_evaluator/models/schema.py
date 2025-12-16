
# notifier_evaluator/models/schema.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Literal

# NOTE:
# - Diese Models sind NUR die Profil-Struktur (wie im JSON).
# - Keine Runtime-Keys, kein resolved context hier.


Op = Literal["gt", "gte", "lt", "lte", "eq", "ne"]
Logic = Literal["and", "or"]
DeactivateOn = Literal["always_on", "auto_off", "pre_notification"]
ThresholdMode = Literal["none", "streak", "count"]


@dataclass(frozen=True)
class ThresholdConfig:
    """
    Threshold-Konfiguration pro Group (oder optional pro Profile).
    mode:
      - none: keine Schwelle (sofort)
      - streak: N aufeinanderfolgende TRUE-Ticks
      - count: in window_ticks mindestens count_true TRUE-Ticks (Unterbrechungen ok)
    """
    mode: ThresholdMode = "none"

    # streak
    streak_needed: int = 1

    # count
    window_ticks: int = 1
    count_true: int = 1


@dataclass(frozen=True)
class ConditionSide:
    """
    Eine Seite (LEFT oder RIGHT) in einer Condition.
    """
    kind: Literal["indicator", "price", "value"] = "indicator"

    # indicator:
    name: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    output: Optional[str] = None
    count: int = 1  # wie viele Werte/rows man abruft (meist 1 oder 5)

    # price:
    source: Optional[str] = None  # Close/Open/High/Low (falls kind=price)

    # value:
    value: Optional[float] = None  # falls kind=value

    # row overrides (optional):
    symbol: Optional[str] = None
    interval: Optional[str] = None
    exchange: Optional[str] = None  # falls du das row-level erlaubst


@dataclass(frozen=True)
class Condition:
    """
    Eine Zeile:
      LEFT op RIGHT
    plus row_logic (AND/OR) als Verbindung zur VORHERIGEN Zeile.
    """
    rid: str

    left: ConditionSide
    op: Op
    right: ConditionSide

    # Verknüpfung zur vorherigen Zeile (erste Zeile kann "and" defaulten)
    row_logic: Logic = "and"

    enabled: bool = True

    # Optional: falls du pro Zeile "wichtig" markieren willst (für spätere partial_true-Varianten)
    important: bool = False


@dataclass(frozen=True)
class Group:
    gid: str
    enabled: bool = True

    # Symbole können direkt sein ODER group tags (z.B. "@top10") – Expander löst das auf
    symbols: List[str] = field(default_factory=list)

    # Gruppen-Defaults (Kontext)
    interval: Optional[str] = None
    exchange: Optional[str] = None

    # Alarm-Mode / Deactivation
    deactivate_on: DeactivateOn = "always_on"

    # Cooldown in Sekunden (Spam-Schutz)
    cooldown_s: int = 0

    # Edge gating (nur bei False->True pushen)
    edge_only: bool = True

    threshold: ThresholdConfig = field(default_factory=ThresholdConfig)

    conditions: List[Condition] = field(default_factory=list)


@dataclass(frozen=True)
class Profile:
    profile_id: str
    name: str
    enabled: bool = True

    groups: List[Group] = field(default_factory=list)

    # globale defaults (nur wenn group/row nicht gesetzt)
    default_interval: Optional[str] = None
    default_exchange: Optional[str] = None
