# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Union
import pandas as pd
import numpy as np
from datetime import datetime

DEBUG = True

SPEC: Dict[str, Any] = {
    "name": "value",
    "summary": "Gibt einen konstanten numerischen Wert als Serie 'value' zurück (z. B. für Vergleiche im Notifier).",
    "required_params": {
        "value": "number",  # Eingabewert
    },
    "default_params": {
        "value": 0.0,
    },
    "choices": {},
    "outputs": ["value"],
    "sort_order": 9,
}

# ──────────────────────────────────────────────────────────────
# Compute
# ──────────────────────────────────────────────────────────────
def compute(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    """
    Gibt eine konstante 'value'-Serie mit demselben Index/Timestamp wie df zurück.
    Ideal für manuelle Eingaben im Notifier oder zum Vergleich mit fixen Zahlenwerten.

    Params:
      - value : int|float   (Pflicht, Default 0)
    Returns:
      (DataFrame[['Timestamp','value']], used_params, ['value'])
    """
    val: Union[int, float] = params.get("value", 0.0)
    try:
        val = float(val)
    except Exception as e:
        raise ValueError(f"[value] Ungültiger Eingabewert: {val!r} ({e})")

    # Zeitachse aus df übernehmen, oder Dummy-Timestamp falls leer
    if df is not None and "Timestamp" in df.columns:
        ts = pd.to_datetime(df["Timestamp"], errors="coerce")
    else:
        ts = pd.Series([pd.Timestamp(datetime.utcnow())])

    out = pd.DataFrame({
        "Timestamp": ts,
        "value": np.full(len(ts), val, dtype="float32"),
    })

    used = {"value": val}

    if DEBUG:
        print(f"[value] FIXED VALUE={val} rows={len(out)}")

    return out, used, ["value"]
