# api/notifier/registry.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional

# Harte Abhängigkeit: wenn das fehlt, ist dein Setup kaputt → Crash ist korrekt.
from notifier.indicator_registry import REGISTERED, SIMPLE_SIGNALS

log = logging.getLogger("notifier.registry")


def _as_list(v: Any) -> List[Any]:
    """Robust: None/str/tuple/etc. -> list"""
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    # Strings nicht als Iterable splitten
    if isinstance(v, str):
        return [v]
    try:
        return list(v)  # type: ignore[arg-type]
    except Exception:
        return []


def _log_summary(prefix: str, items: List[Dict[str, Any]]) -> None:
    """
    Kleine Debug-Hilfe: Anzahl + ein paar Namen loggen/printen.
    """
    try:
        names: List[str] = []
        for it in items[:5]:
            if not isinstance(it, dict):
                continue
            name = it.get("display_name") or it.get("name") or it.get("base") or ""
            try:
                names.append(str(name))
            except Exception:
                names.append("?")
        log.info("%s count=%d sample=%s", prefix, len(items), names)
        print(f"[REGISTRY] {prefix} count={len(items)} sample={names}")
    except Exception:
        # Debug darf nie crashen
        pass


# ─────────────────────────────────────────────────────────────
# Registry: alle Indikatoren (Roh-Registry)
# ─────────────────────────────────────────────────────────────

def get_registry_indicators(
    scope: Optional[str] = None,
    include_deprecated: bool = False,
    include_hidden: bool = False,
    expand_presets: bool = False,
) -> List[Dict[str, Any]]:
    """
    Entspricht grob dem alten /registry/indicators:

    - Wenn expand_presets = False:
        → gibt 1 Eintrag pro Registry-Indicator zurück (volle Specs)
    - Wenn expand_presets = True:
        → gibt 1 Eintrag pro Preset (display_name, base, params, locked_params, outputs)

    Filter:
      - scope:  None  → kein Scope-Filter
                "notifier", "chart", "backtest", ... je nach Registry-Spec
      - include_deprecated: wenn False → deprecated=True rausfiltern
      - include_hidden:     wenn False → ui_hidden=True rausfiltern
    """
    items: List[Dict[str, Any]] = []

    # Debug: grobe Registry-Größe
    try:
        log.debug("get_registry_indicators expand_presets=%s scope=%s registered=%d",
                  expand_presets, scope, len(getattr(REGISTERED, "keys", lambda: [])()))
    except Exception:
        pass

    if not expand_presets:
        # Roh-Registry
        for key, spec in REGISTERED.items():
            if not isinstance(spec, dict):
                log.warning("REGISTERED[%s] is not a dict -> skip (%r)", key, type(spec))
                continue

            s = deepcopy(spec)

            if not s.get("enabled", True):
                continue
            if scope is not None and scope not in (_as_list(s.get("scopes"))):
                continue
            if not include_deprecated and bool(s.get("deprecated", False)):
                continue
            if not include_hidden and bool(s.get("ui_hidden", False)):
                continue

            items.append(s)

        _log_summary("registry_indicators(raw)", items)
        return items

    # expand_presets=True → pro Preset ein Objekt
    for key, spec in REGISTERED.items():
        if not isinstance(spec, dict):
            log.warning("REGISTERED[%s] is not a dict -> skip (%r)", key, type(spec))
            continue

        s = spec

        if not s.get("enabled", True):
            continue
        if scope is not None and scope not in (_as_list(s.get("scopes"))):
            continue
        if not include_deprecated and bool(s.get("deprecated", False)):
            continue
        if not include_hidden and bool(s.get("ui_hidden", False)):
            continue

        base_name = s.get("name")
        outputs = [str(x) for x in _as_list(s.get("outputs"))]

        presets = s.get("presets") or []
        if not isinstance(presets, (list, tuple)):
            log.warning("Indicator '%s' presets is not list/tuple -> skip presets (%r)", base_name, type(presets))
            continue

        for p in presets:
            if not isinstance(p, dict):
                continue
            label = p.get("label")
            if not isinstance(label, str) or not label:
                continue

            items.append(
                {
                    "display_name": label,
                    "base": base_name,
                    "params": deepcopy(p.get("params", {})) if isinstance(p.get("params", {}), dict) else {},
                    "locked_params": [str(x) for x in _as_list(p.get("locked_params"))],
                    "outputs": outputs,
                }
            )

    _log_summary("registry_indicators(expanded_presets)", items)
    return items


# ─────────────────────────────────────────────────────────────
# Notifier-spezifische Presets (für deine Dash-UI)
# ─────────────────────────────────────────────────────────────

def get_notifier_indicators(
    include_deprecated: bool = False,
    include_hidden: bool = False,
) -> List[Dict[str, Any]]:
    """
    Entspricht dem alten /notifier/indicators:

    - Nur Indikatoren, deren 'scopes' 'notifier' enthalten.
    - Für jeden Preset wird ein UI-freundliches Objekt erzeugt:
        {
          "display_name": <preset label>,
          "base": <basis-indicator name>,
          "params": { ... },
          "locked_params": [...],
          "outputs": [...],
        }

    locked_params:
      - wenn der Preset eigene locked_params hat → die
      - sonst → locked_params des Basis-Indikators
    """
    items: List[Dict[str, Any]] = []

    for key, spec in REGISTERED.items():
        if not isinstance(spec, dict):
            log.warning("REGISTERED[%s] is not a dict -> skip (%r)", key, type(spec))
            continue

        s = spec

        if not s.get("enabled", True):
            continue
        if "notifier" not in _as_list(s.get("scopes")):
            continue
        if not include_deprecated and bool(s.get("deprecated", False)):
            continue
        if not include_hidden and bool(s.get("ui_hidden", False)):
            continue

        base_name = s.get("name")
        base_locked = [str(x) for x in _as_list(s.get("locked_params"))]
        outputs = [str(x) for x in _as_list(s.get("outputs"))]

        presets = s.get("presets") or []
        if not isinstance(presets, (list, tuple)):
            log.warning("Indicator '%s' presets is not list/tuple -> skip presets (%r)", base_name, type(presets))
            continue

        for p in presets:
            if not isinstance(p, dict):
                continue
            label = p.get("label")
            if not isinstance(label, str) or not label:
                continue

            preset_locked = [str(x) for x in _as_list(p.get("locked_params"))]

            params = p.get("params", {})
            if not isinstance(params, dict):
                params = {}

            items.append(
                {
                    "display_name": label,
                    "base": base_name,
                    "params": deepcopy(params),
                    "locked_params": preset_locked or base_locked,
                    "outputs": outputs,
                }
            )

    _log_summary("notifier_indicators", items)
    return items


# ─────────────────────────────────────────────────────────────
# Simple Signals (nur Namen)
# ─────────────────────────────────────────────────────────────

def get_simple_signals() -> List[str]:
    """
    Entspricht dem alten /registry/simple-signals.
    Gibt nur die Namen als Liste zurück.
    """
    signals_raw = SIMPLE_SIGNALS or []
    signals = [str(x) for x in _as_list(signals_raw)]

    try:
        log.info("simple_signals count=%d", len(signals))
        print(f"[REGISTRY] simple_signals count={len(signals)}")
    except Exception:
        pass

    return signals


# ─────────────────────────────────────────────────────────────
# Backwards-Compatible Aliasse für den API-Layer
# ─────────────────────────────────────────────────────────────

# Falls du im API-Layer kurz die alten Funktionsnamen verwenden willst:
registry_indicators = get_registry_indicators
notifier_indicators = get_notifier_indicators
registry_simple_signals = get_simple_signals
