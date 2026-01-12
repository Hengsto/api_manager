# indicators/custom_registry.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List, Tuple

DEBUG = True

"""
Lightweight Registry für:
- UI/Metadaten (display_name, summary, outputs, visibility)
- Param-Shaping (required/optional -> 'unspecified')
- Lokalen Fallback-Dispatcher: module + fn (dyn. import im Proxy)
WICHTIG: Keine Imports der Indicator-Module hier (keine harte Kopplung / kein Startup-Overhead)!
"""

CUSTOMS: Dict[str, Dict[str, Any]] = {
    "value": {
        "name": "value",
        "display_name": "📡 Value",
        "summary": "Gibt einen konstanten numerischen Wert für alle Zeilen zurück (z. B. 25).",
        "required": ["value"],
        "optional": ["unspecified"],
        "outputs": ["value"],
        "visibility": ["notifier", "screener"],
        "sort_order": 1,
        "module": "indicators.value",
        "fn": "value",  # Signatur: value(df, *, value, **kwargs)
    },
    "price": {
        "name": "price",
        "display_name": "📡 Price",
        "summary": "Gibt den Rohpreis (z. B. Close) des Basis-Datasets unverändert zurück.",
        "required": ["source"],
        "optional": ["unspecified"],
        "outputs": ["price"],
        "visibility": ["notifier", "screener"],
        "sort_order": 5,
        "module": "indicators.price",
        "fn": "price",  # Signatur: price(df, *, source='close', **kwargs)
    },
    "volume": {
        "name": "volume",
        "display_name": "📡 Volume",
        "summary": "Gibt das Volumen (z. B. 'volume') des Basis-Datasets unverändert zurück.",
        "required": ["source"],
        "optional": ["unspecified"],
        "outputs": ["volume"],
        "visibility": ["notifier", "screener"],
        "sort_order": 6,
        "module": "indicators.volume",
        "fn": "volume",  # Signatur: volume(df, *, source='volume', **kwargs)
    },
    "slope": {
        "name": "slope",
        "display_name": "📡 Slope",
        "summary": "Steigung einer Basisreihe (z. B. RSI/MACD) über N Schritte.",
        "required": ["base", "window"],
        "optional": ["input", "base_params", "unspecified"],
        "outputs": ["slope"],
        "visibility": ["notifier", "screener"],
        "sort_order": 30,
        "module": "indicators.slope",
        "fn": "slope",   # Signatur: slope(df, *, base, window, input=None, base_params=None, unspecified=None)
    },
    "change": {
        "name": "change",
        "display_name": "📡 Change",
        "summary": "Änderung einer Basisreihe seit fixem Zeitpunkt (absolut oder prozentual).",
        "required": ["base", "type", "timestamp"],
        "optional": ["input", "pct_scale", "base_params", "unspecified"],
        "outputs": ["change"],
        "visibility": ["notifier", "screener"],
        "sort_order": 40,
        "module": "indicators.change",
        "fn": "change",  # Signatur: change(df, *, base, input=None, type='percentage', pct_scale=100.0, timestamp=..., base_params=None, unspecified=None)
    },
}



def _merge_unspecified(dest: Dict[str, Any], extras: Dict[str, Any]) -> Dict[str, Any]:
    """Mergt übrige Params in 'unspecified' (flach halten, nicht verschachteln)."""
    u = dest.get("unspecified")
    if not isinstance(u, dict):
        u = {}
    for k, v in extras.items():
        if k == "unspecified" and isinstance(v, dict):
            for inner_key, inner_val in v.items():
                if inner_key not in u:
                    u[inner_key] = inner_val
        elif k not in u:
            u[k] = v
    dest["unspecified"] = u
    return dest

def normalize_params_for_proxy(name: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reines Shaping:
      - UI-Alias 'output' -> 'input'
      - bekannte required/optional Keys oben behalten
      - übrige Keys in 'unspecified'
    Keine Typvalidierung/-konvertierung; das macht Upstream oder lokale Compute-Fns.
    """
    lname = (name or "").strip().lower()
    params = dict(raw or {})

    # Alias
    if "input" not in params and "output" in params and params["output"] not in (None, ""):
        params["input"] = params.pop("output")

    spec = CUSTOMS.get(lname)
    if not spec:
        return params

    required = set(spec.get("required", []))
    optional = set(spec.get("optional", []))
    keep = required | optional

    shaped = {k: params[k] for k in params.keys() if k in keep}
    extras = {k: v for k, v in params.items() if k not in keep}

    if extras:
        shaped = _merge_unspecified(shaped, extras)

    if DEBUG:
        try:
            dropped = [k for k in params.keys() if k not in shaped and k != "unspecified"]
        except Exception:
            dropped = []
        print(
            f"[REG][shape] name={lname} keep={sorted(list(keep))} "
            f"extras->unspecified={sorted(list(extras.keys()))} dropped={dropped}"
        )

    return shaped

def list_customs_for_ui(
    *,
    visibility: List[str] | None = None,
    order_by: str = "sort_order",
    desc: bool = False,
) -> List[Dict[str, Any]]:
    """UI-Metadaten liefern (inkl. display_name, outputs)."""
    rows: List[Dict[str, Any]] = []
    vset = set([v.lower() for v in (visibility or [])])

    for key, spec in CUSTOMS.items():
        vis = list(spec.get("visibility", []))
        if vset and not any((v.lower() in vset) for v in vis):
            continue
        rows.append({
            "name": spec["name"],
            "display_name": spec.get("display_name", spec["name"]),
            "summary": spec.get("summary", ""),
            "required_params": list(spec.get("required", [])),
            "optional_params": list(spec.get("optional", [])),
            "outputs": list(spec.get("outputs", [])),
            "visibility": vis,
            "sort_order": spec.get("sort_order", 9999),
        })

    key = order_by if order_by in {"name", "display_name", "sort_order"} else "sort_order"
    rows.sort(key=lambda r: (r[key], r["name"]) if key == "sort_order" else r[key], reverse=desc)
    return rows

def get_custom_exec(name: str) -> Tuple[str, str]:
    """
    Für lokalen Dispatcher: liefert (module, fn) zu einem Custom.
    Raise KeyError wenn unbekannt / unvollständig.
    """
    lname = (name or "").strip().lower()
    spec = CUSTOMS[lname]  # KeyError gewollt → sauberer 404 im Proxy
    module = spec.get("module")
    fn = spec.get("fn")
    if not module or not fn:
        raise KeyError(f"Custom '{lname}' hat keine ausführbare Definition (module/fn fehlen).")
    return module, fn
