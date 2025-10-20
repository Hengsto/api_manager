# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
import pandas as pd
import json

DEBUG = True

SPEC: Dict[str, Any] = {
    "name": "slope",
    "summary": "Steigung einer normierten Basisreihe (z. B. RSI) über 'window' Schritte.",
    "required_params": {"base": "string", "window": "integer"},
    "default_params": {"window": 2},
    "outputs": ["slope"],
    "sort_order": 30,
}

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

def _pick_output_column(df: pd.DataFrame, value_cols: List[str], want: Optional[str]) -> str:
    cols = [c for c in df.columns if c != "Timestamp"]
    if not cols:
        raise ValueError("[slope] keine Daten-Spalten")

    lower_map = {c.lower(): c for c in cols}
    val_map   = {c.lower(): c for c in (value_cols or [])}

    if want:
        key = str(want).strip().lower()
        if key in val_map:
            return val_map[key]
        if key in lower_map:
            return lower_map[key]
        raise ValueError(f"[slope] input='{want}' nicht in {value_cols} / {cols}")

    if isinstance(value_cols, list) and len(value_cols) == 1 and value_cols[0] in df.columns:
        return value_cols[0]

    for cand in ("rsi", "ema", "signal", "value", "close"):
        if cand in lower_map:
            return lower_map[cand]

    numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    if numeric_cols:
        return numeric_cols[0]
    return cols[0]

def _compute_base_series(
    df: pd.DataFrame,
    base_name: str,
    base_params: Dict[str, Any],
) -> Tuple[pd.DataFrame, Dict[str, Any], List[str], str]:
    name = str(base_name).strip().lower()
    from ...core_registry import get_indicator_spec, compute_indicator, validate_params as _v_ind
    from ...signals_registry import get_signal_spec, compute_signal
    from ..customs_registry import get_custom_spec, compute_custom

    if spec := get_indicator_spec(name):
        params = _v_ind(spec, dict(base_params or {}))
        out_df, used, value_cols, _ = compute_indicator(spec, df, params)
        return out_df, used, value_cols, "indicator"
    if spec := get_signal_spec(name):
        out_df, used, value_cols = compute_signal(spec, df, dict(base_params or {}))
        return out_df, used, value_cols, "signal"
    if spec := get_custom_spec(name):
        out_df, used, value_cols = compute_custom(spec, df, dict(base_params or {}))
        return out_df, used, value_cols, "custom"

    raise ValueError(f"[slope] unbekannter Basis-Indikator: {base_name}")

def slope(
    df: pd.DataFrame,
    *,
    base: str,
    window: int,
    input: Optional[str] = None,
    base_params: Optional[Dict[str, Any]] = None,
    unspecified: Optional[Dict[str, Any]] = None,   # 👈 wichtig
) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    u = _ensure_dict(unspecified)
    bp = dict(base_params or {})
    bp.update(u)  # 👈 merge: unspecified überschreibt base_params, falls doppelt

    window = int(window)
    if window < 1:
        raise ValueError("window muss >= 1 sein")

    if DEBUG:
        print(f"[slope] incoming: base={base!r} window={window} input={input!r} "
              f"base_params.keys={list((base_params or {}).keys())} unspecified.keys={list(u.keys())}")
        print(f"[slope] merged_base_keys={list(bp.keys())}")

    base_df, base_used, base_value_cols, base_kind = _compute_base_series(df, base, bp)
    pick = _pick_output_column(base_df, base_value_cols, input)
    series = pd.to_numeric(base_df[pick], errors="coerce")

    price_like = {"close", "open", "high", "low", "price", "hlc3", "ohlc4", "typical"}
    if pick.lower() in price_like and not base.lower().startswith(("rsi", "stoch", "cci", "macd")):
        raise ValueError("[slope] Für Preis-Serien bitte 'change' verwenden; slope ist für normierte Indikatoren gedacht.")

    slope_vals = (series - series.shift(window)) / float(window)
    out = pd.DataFrame({
        "Timestamp": base_df["Timestamp"].values,
        "slope": slope_vals.astype("float32").values,
    })

    used = {
        "base": base,
        "base_kind": base_kind,
        "input": pick,
        "window": int(window),
        "base_params": dict(base_used or {}),
        "merged_base_params": dict(bp),  # 👈 sichtbar machen, was effektiv benutzt wurde
    }

    if DEBUG:
        print(f"[slope] pick={pick} last3={series.tail(3).tolist()}")
        print(f"[slope] slope tail={slope_vals.tail(3).tolist()}")

    return out, used, ["slope"]
