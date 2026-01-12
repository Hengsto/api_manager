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
AlarmMode = Literal["always_on", "auto_off", "pre_notification"]
ThresholdMode = Literal["none", "streak", "count"]


# ──────────────────────────────────────────────────────────────────────────────
# Engine Defaults (global fallback)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class EngineDefaults:
    """
    Globale Defaults, falls row/group nicht setzen.
    """
    exchange: str = ""
    interval: str = ""
    clock_interval: str = ""
    source: str = "Close"


# ──────────────────────────────────────────────────────────────────────────────
# Threshold / Alarm Configs
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ThresholdConfig:
    """
    Threshold-Konfiguration pro Group.
    mode:
      - none: keine Schwelle (sofort)
      - streak: N aufeinanderfolgende TRUE-Ticks
      - count: in window_ticks mindestens count_required TRUE-Ticks (Unterbrechungen ok)
    """
    mode: ThresholdMode = "none"

    # streak
    streak_n: int = 1

    # count
    window_ticks: int = 1
    count_required: int = 1

    # Backward-compatible alias names (wenn du das früher so hattest)
    @property
    def streak_needed(self) -> int:
        return self.streak_n

    @property
    def count_true(self) -> int:
        return self.count_required

    @staticmethod
    def from_dict(d: Any) -> "ThresholdConfig":
        if not isinstance(d, dict):
            return ThresholdConfig()
        mode = str(d.get("mode", "none")).strip().lower()
        mode = mode if mode in ("none", "streak", "count") else "none"

        # accept both naming styles:
        streak_n = d.get("streak_n", d.get("streak_needed", 1))
        window_ticks = d.get("window_ticks", 1)
        count_required = d.get("count_required", d.get("count_true", 1))

        try:
            streak_n = int(streak_n)
        except Exception:
            streak_n = 1
        try:
            window_ticks = int(window_ticks)
        except Exception:
            window_ticks = 1
        try:
            count_required = int(count_required)
        except Exception:
            count_required = 1

        # clamp
        if streak_n < 1:
            streak_n = 1
        if window_ticks < 1:
            window_ticks = 1
        if count_required < 1:
            count_required = 1

        return ThresholdConfig(
            mode=mode, streak_n=streak_n, window_ticks=window_ticks, count_required=count_required
        )


@dataclass
class AlarmConfig:
    """
    Alarm policy config:
      - always_on: push wenn final_true (+threshold passed) erfüllt
      - auto_off: push und dann deactivate (active=false)
      - pre_notification: push bei partial-change + push bei final_true, dann deactivate
    """
    mode: AlarmMode = "always_on"
    cooldown_sec: int = 0
    edge_only: bool = True

    @staticmethod
    def from_group_legacy(g: Dict[str, Any]) -> "AlarmConfig":
        """
        Mappt deine alten Group-Felder:
          deactivate_on -> mode
          cooldown_s    -> cooldown_sec
          edge_only     -> edge_only
        """
        if not isinstance(g, dict):
            return AlarmConfig()
        mode = str(g.get("mode", g.get("deactivate_on", "always_on"))).strip().lower()
        if mode not in ("always_on", "auto_off", "pre_notification"):
            mode = "always_on"
        cd = g.get("cooldown_sec", g.get("cooldown_s", 0))
        try:
            cd = int(cd)
        except Exception:
            cd = 0
        if cd < 0:
            cd = 0
        edge = g.get("edge_only", True)
        edge = bool(edge) if isinstance(edge, (bool, int)) else True
        return AlarmConfig(mode=mode, cooldown_sec=cd, edge_only=edge)

    @staticmethod
    def from_dict(d: Any) -> "AlarmConfig":
        if not isinstance(d, dict):
            return AlarmConfig()
        mode = str(d.get("mode", "always_on")).strip().lower()
        if mode not in ("always_on", "auto_off", "pre_notification"):
            mode = "always_on"
        cd = d.get("cooldown_sec", 0)
        try:
            cd = int(cd)
        except Exception:
            cd = 0
        if cd < 0:
            cd = 0
        edge = d.get("edge_only", True)
        edge = bool(edge) if isinstance(edge, (bool, int)) else True
        return AlarmConfig(mode=mode, cooldown_sec=cd, edge_only=edge)


# ──────────────────────────────────────────────────────────────────────────────
# Conditions
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ConditionSide:
    """
    Eine Seite (LEFT oder RIGHT) in einer Condition.
    """
    kind: Literal["indicator", "price", "value"] = "indicator"

    # indicator:
    name: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    output: Optional[str] = None
    count: int = 1  # meist 1 oder 5

    # price:
    source: Optional[str] = None  # Close/Open/High/Low (falls kind=price)

    # value:
    value: Optional[float] = None  # falls kind=value

    # row overrides (optional):
    symbol: Optional[str] = None
    interval: Optional[str] = None
    exchange: Optional[str] = None

    @staticmethod
    def from_dict(d: Any) -> "ConditionSide":
        if not isinstance(d, dict):
            return ConditionSide()
        kind = str(d.get("kind", "indicator")).strip().lower()
        if kind not in ("indicator", "price", "value"):
            kind = "indicator"
        params = d.get("params") if isinstance(d.get("params"), dict) else {}
        try:
            cnt = int(d.get("count", 1))
        except Exception:
            cnt = 1
        if cnt < 1:
            cnt = 1
        return ConditionSide(
            kind=kind,
            name=d.get("name"),
            params=params,
            output=d.get("output"),
            count=cnt,
            source=d.get("source"),
            value=d.get("value"),
            symbol=d.get("symbol"),
            interval=d.get("interval"),
            exchange=d.get("exchange"),
        )


@dataclass
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
    logic_to_prev: Logic = "and"

    enabled: bool = True
    important: bool = False

    @staticmethod
    def from_dict(d: Any) -> "Condition":
        if not isinstance(d, dict):
            # harte Default-Zeile ist Quatsch → rid muss existieren
            return Condition(
                rid="<missing>",
                left=ConditionSide(),
                op="eq",
                right=ConditionSide(kind="value", value=0.0),
                logic_to_prev="and",
                enabled=False,
            )
        rid = str(d.get("rid") or d.get("id") or "").strip() or "<missing>"
        op = str(d.get("op", "eq")).strip().lower()
        if op not in ("gt", "gte", "lt", "lte", "eq", "ne"):
            op = "eq"
        ltp = str(d.get("logic_to_prev", d.get("row_logic", "and"))).strip().lower()
        if ltp not in ("and", "or"):
            ltp = "and"
        return Condition(
            rid=rid,
            left=ConditionSide.from_dict(d.get("left")),
            op=op,  # type: ignore
            right=ConditionSide.from_dict(d.get("right")),
            logic_to_prev=ltp,  # type: ignore
            enabled=bool(d.get("enabled", True)),
            important=bool(d.get("important", False)),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Group / Profile
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class Group:
    gid: str
    enabled: bool = True

    # Symbole können direkt sein ODER group tags (z.B. "@top10") – Expander löst das auf
    symbols: List[str] = field(default_factory=list)

    # Gruppen-Defaults (Kontext)
    interval: Optional[str] = None
    exchange: Optional[str] = None

    # evaluator config
    threshold: ThresholdConfig = field(default_factory=ThresholdConfig)
    alarm: AlarmConfig = field(default_factory=AlarmConfig)

    # rows
    rows: List[Condition] = field(default_factory=list)

    # Backward-compatible alias: falls alte JSONs "conditions" benutzen
    @property
    def conditions(self) -> List[Condition]:
        return self.rows

    @staticmethod
    def from_dict(d: Any) -> "Group":
        if not isinstance(d, dict):
            return Group(gid="<missing>", enabled=False)

        gid = str(d.get("gid") or d.get("id") or "").strip() or "<missing>"
        enabled = bool(d.get("enabled", True))
        symbols = d.get("symbols") if isinstance(d.get("symbols"), list) else []
        symbols = [str(x) for x in symbols if str(x).strip()]

        interval = d.get("interval", None)
        exchange = d.get("exchange", None)

        threshold = ThresholdConfig.from_dict(d.get("threshold", {}))

        # Accept either explicit "alarm" dict OR legacy group fields
        if isinstance(d.get("alarm"), dict):
            alarm = AlarmConfig.from_dict(d.get("alarm"))
        else:
            alarm = AlarmConfig.from_group_legacy(d)

        # Accept either "rows" OR legacy "conditions"
        raw_rows = d.get("rows", None)
        if raw_rows is None:
            raw_rows = d.get("conditions", [])

        rows: List[Condition] = []
        if isinstance(raw_rows, list):
            for item in raw_rows:
                rows.append(Condition.from_dict(item))

        return Group(
            gid=gid,
            enabled=enabled,
            symbols=symbols,
            interval=interval,
            exchange=exchange,
            threshold=threshold,
            alarm=alarm,
            rows=rows,
        )


@dataclass
class Profile:
    profile_id: str
    name: str
    enabled: bool = True

    groups: List[Group] = field(default_factory=list)

    # globale defaults (nur wenn group/row nicht gesetzt)
    default_interval: Optional[str] = None
    default_exchange: Optional[str] = None

    @staticmethod
    def from_dict(d: Any) -> "Profile":
        if not isinstance(d, dict):
            return Profile(profile_id="<missing>", name="<missing>", enabled=False)

        pid = str(d.get("profile_id") or d.get("id") or "").strip() or "<missing>"
        name = str(d.get("name") or "").strip() or pid
        enabled = bool(d.get("enabled", True))

        default_interval = d.get("default_interval", None)
        default_exchange = d.get("default_exchange", None)

        raw_groups = d.get("groups", [])
        groups: List[Group] = []
        if isinstance(raw_groups, list):
            for g in raw_groups:
                groups.append(Group.from_dict(g))

        return Profile(
            profile_id=pid,
            name=name,
            enabled=enabled,
            groups=groups,
            default_interval=default_interval,
            default_exchange=default_exchange,
        )
