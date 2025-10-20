# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List

DEBUG = True

"""
Lightweight Registry NUR fürs Param-Shaping & UI-Metadaten im Proxy.
- Keine Validierung/Casting (macht die price_api).
- 'display_name' für die UI.
- 'summary' optional für Tooltips.
- 'required'/'optional' steuern das Shaping (oben halten vs. in 'unspecified').
"""

CUSTOMS: Dict[str, Dict[str, Any]] = {
    "slope": {
        "name": "slope",
        "display_name": "Slope (auf Basis)",
        "summary": "Steigung einer Basisreihe (z. B. RSI/MACD) über N Schritte.",
        "required": ["base", "window"],
        "optional": ["input"],        # alias 'output' wird -> 'input' gemappt
        "outputs": ["slope"],
        "visibility": ["notifier", "screener"],
        "sort_order": 30,
    },
    "change": {
        "name": "change",
        "display_name": "Change (vs. Timestamp)",
        "summary": "Änderung einer Basisreihe seit fixem Zeitpunkt (absolut oder prozentual).",
        "required": ["base", "type"],
        "optional": ["input", "pct_scale", "timestamp"],
        "outputs": ["change"],
        "visibility": ["notifier", "screener"],
        "sort_order": 40,
    },
    "price": {
        "name": "price",
        "display_name": "Price (Quelle)",
        "summary": "Gibt die gewählte Preisquelle als Serie zurück (Close/High/Low/Open).",
        "required": ["source"],
        "optional": [],
        "outputs": ["price"],
        "visibility": ["notifier", "screener", "source"],
        "sort_order": 10,
    },
    "value": {
        "name": "value",
        "display_name": "Konstanter Wert",
        "summary": "Konstante Zahl als Serie (für Vergleiche/Schwellen).",
        "required": ["value"],
        "optional": [],
        "outputs": ["value"],
        "visibility": ["notifier", "screener", "input"],
        "sort_order": 20,
    },
}

def _merge_unspecified(dest: Dict[str, Any], extras: Dict[str, Any]) -> Dict[str, Any]:
    u = dest.get("unspecified")
    if not isinstance(u, dict):
        u = {}
    for k, v in extras.items():
        if k not in u:
            u[k] = v
    dest["unspecified"] = u
    return dest

def normalize_params_for_proxy(name: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reines Shaping für die price_api:
      - 'output' -> 'input'
      - bekannte required/optional Keys oben behalten
      - alle übrigen Keys in 'unspecified' mergen (so kommen z. B. Base-Parameter an)
    Keine Validierung, kein Casting.
    """
    lname = (name or "").strip().lower()
    params = dict(raw or {})

    # UI-Alias
    if "input" not in params and "output" in params and params["output"] not in (None, ""):
        params["input"] = params.pop("output")

    spec = CUSTOMS.get(lname)
    if not spec:
        # Unbekannt: 1:1 durchreichen (Alias bereits angewendet)
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
    """
    Liefert UI-Metadaten (inkl. display_name) zur Anzeige/Selektion.
    """
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
