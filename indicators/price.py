# indicators/price.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import numpy as np
import json

DEBUG = True

SPEC: Dict[str, Any] = {
    "name": "price",
    "summary": "Gibt den Rohpreis (z. B. Close) des Basis-Datasets unverändert zurück.",
    "required_params": {"source": "string"},
    "default_params": {"source": "close"},
    "outputs": ["price"],
    "sort_order": 5,
}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_dict(x: Any) -> Dict[str, Any]:
    if isinstance(x, dict):
        return dict(x)
    if isinstance(x, str):
        try:
            j = json.loads(x)
            return j if isinstance(j, dict) else {}
        except Exception:
            return {}
    try:
        return dict(x or {})
    except Exception:
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# Main Logic
# ──────────────────────────────────────────────────────────────────────────────
def price(
    df: pd.DataFrame,
    source: str = "close",
    **kwargs: Any,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    Gibt 1:1 die gewählte Preisspalte aus dem Eingabe-DataFrame zurück.
    Erfüllt Proxy-Anforderung: 'Timestamp' MUSS als Spalte enthalten sein.
    Unterstützt sowohl 'close' als auch 'Close' usw.
    """
    if df is None or df.empty:
        if DEBUG:
            print("[PRICE] Eingabe-DataFrame ist leer oder None.")
        # Proxy erwartet 'Timestamp' -> leeres, aber korrektes Schema liefern
        return pd.DataFrame(columns=["Timestamp", "price"]), ["source"], ["price"]

    # Mappe 'close'/'Close' etc. robust auf vorhandene Spalten
    cols = {c.lower(): c for c in df.columns}
    # bevorzugte Schreibweisen
    candidates = [source, source.lower(), source.capitalize(), source.title()]
    resolved = None
    for cand in candidates:
        key = cand.lower()
        if key in cols:
            resolved = cols[key]
            break
        if cand in df.columns:
            resolved = cand
            break

    if resolved is None:
        raise ValueError(f"[PRICE] Spalte '{source}' nicht im DataFrame vorhanden. Spalten: {list(df.columns)}")

    # 'Timestamp' als Spalte erzwingen
    if "Timestamp" in df.columns:
        ts = df["Timestamp"]
    else:
        # Falls Timestamp im Index steckt: übernehmen
        ts = df.index
    out = pd.DataFrame({"Timestamp": ts, "price": pd.to_numeric(df[resolved], errors="coerce")}, copy=False)

    if DEBUG:
        try:
            n = len(out)
            n_nan = out["price"].isna().sum()
            print(f"[PRICE] Quelle='{resolved}' (req='{source}'), len={n}, NaN={n_nan}")
        except Exception as e:
            print(f"[PRICE] debug-failed: {type(e).__name__}: {e}")

    return out, ["source"], ["price"]


# ──────────────────────────────────────────────────────────────────────────────
# Debug Run
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import pandas as pd
    test = pd.DataFrame({
        "open": [10, 11, 12],
        "high": [11, 12, 13],
        "low": [9, 10, 11],
        "close": [10.5, 11.5, 12.5],
    })
    out, used, cols = price(test, source="close")
    print(out)
    print("used:", used, "cols:", cols)
