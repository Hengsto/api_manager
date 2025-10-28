# indicators/value.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Union
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

DEBUG = True

SPEC: Dict[str, Any] = {
    "name": "value",
    "summary": "Gibt einen konstanten numerischen Wert als Serie 'value' zurück (z. B. für Vergleiche im Notifier).",
    "required_params": {"value": "number"},
    "default_params": {"value": 0.0},
    "choices": {},
    "outputs": ["value"],
    "sort_order": 9,
}

def _coerce_number(x: Any) -> Union[int, float]:
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s == "":
            raise ValueError("empty string is not a number")
        try:
            if any(c in s for c in (".", "e", "E")):
                return float(s)
            return int(s)
        except Exception as e:
            raise ValueError(f"could not parse numeric string {x!r}: {e}")
    raise ValueError(f"type {type(x).__name__} is not numeric")

def _mk_synthetic_series(n: int) -> pd.Series:
    n = max(1, int(n))
    base = datetime.now(timezone.utc) - timedelta(seconds=n)
    return pd.to_datetime([base + timedelta(seconds=i) for i in range(n)], utc=True)

def compute(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    """
    Erwartet ein bereits normalisiertes Chart-DF mit 'Timestamp' (kommt aus dem LOCAL-Fallback).
    Falls df leer ist, erzeugen wir eine synthetische Zeitachse (count_hint).
    """
    if "value" not in params:
        raise ValueError("[value] missing required param 'value'")
    val = _coerce_number(params.get("value"))

    # Zeitachse: bevorzugt df['Timestamp'] (bereits normalisiert in der API)
    ts: pd.Series
    if isinstance(df, pd.DataFrame) and "Timestamp" in df.columns and not df.empty:
        ts = pd.to_datetime(df["Timestamp"], errors="coerce")
        ts = ts[ts.notna()]
        if not ts.is_monotonic_increasing:
            ts = ts.sort_values(kind="mergesort")
        if ts.empty:
            ch = int(params.get("count_hint") or 1)
            ts = _mk_synthetic_series(ch)
    else:
        ch = int(params.get("count_hint") or 1)
        ts = _mk_synthetic_series(ch)

    out = pd.DataFrame({"Timestamp": ts})
    out["value"] = np.full(len(ts), float(val), dtype="float64")

    used = {"value": val}

    if DEBUG:
        try:
            print(f"[value] rows={len(out)} value={val} ts_first={out['Timestamp'].iloc[0]} ts_last={out['Timestamp'].iloc[-1]}")
        except Exception:
            print(f"[value] rows={len(out)} value={val}")

    return out, used, ["value"]
