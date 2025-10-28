# indicators/change.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import json
import importlib

DEBUG = True

SPEC: Dict[str, Any] = {
    "name": "change",
    "summary": "Änderung einer Basisreihe seit fixem Zeitpunkt (absolut oder prozentual).",
    "required_params": {"base": "string", "type": "string", "timestamp": "string"},
    "default_params": {"type": "percentage", "pct_scale": 100.0},
    "outputs": ["change"],
    "sort_order": 40,
}

# ──────────────────────────────────────────────────────────────

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

def _try_import(mod_name: str):
    try:
        return importlib.import_module(mod_name)
    except Exception as e:
        if DEBUG:
            print(f"[change] import miss: {mod_name} ({type(e).__name__}: {e})")
        return None

def _compute_base_series(
    df: pd.DataFrame,
    base_name: str,
    base_params: Dict[str, Any],
) -> Tuple[pd.DataFrame, Dict[str, Any], List[str], str]:
    name = str(base_name).strip().lower()

    # Indicator
    for mod in ("trading_indicators.core_registry", "core_registry"):
        m = _try_import(mod)
        if not m:
            continue
        if hasattr(m, "get_indicator_spec") and hasattr(m, "compute_indicator"):
            spec = m.get_indicator_spec(name)
            if spec:
                params = base_params
                if hasattr(m, "validate_params"):
                    params = m.validate_params(spec, dict(base_params or {}))
                out_df, used, value_cols, _spec = m.compute_indicator(spec, df, params)
                return out_df, used, value_cols, "indicator"

    # Signal
    for mod in ("trading_indicators.signals_registry", "signals_registry"):
        m = _try_import(mod)
        if not m:
            continue
        if hasattr(m, "get_signal_spec") and hasattr(m, "compute_signal"):
            spec = m.get_signal_spec(name)
            if spec:
                out_df, used, value_cols = m.compute_signal(spec, df, dict(base_params or {}))
                return out_df, used, value_cols, "signal"

    # Custom
    for mod in ("trading_indicators.custom_registry", "custom_registry"):
        m = _try_import(mod)
        if not m:
            continue
        if hasattr(m, "get_custom_spec") and hasattr(m, "compute_custom"):
            spec = m.get_custom_spec(name)
            if spec:
                out_df, used, value_cols = m.compute_custom(spec, df, dict(base_params or {}))
                return out_df, used, value_cols, "custom"

    raise ValueError(f"[change] unbekannter Basis-Indikator: {base_name} (keine Registry gefunden oder kein Spec)")

def _pick_output_column(df: pd.DataFrame, value_cols: List[str], want: Optional[str]) -> str:
    cols = [c for c in df.columns if c != "Timestamp"]
    lower_map = {c.lower(): c for c in cols}
    val_map   = {c.lower(): c for c in (value_cols or [])}
    if want:
        k = str(want).strip().lower()
        if k in val_map:
            return val_map[k]
        if k in lower_map:
            return lower_map[k]
        raise ValueError(f"[change] input='{want}' nicht gefunden. value_cols={value_cols} cols={cols}")
    if isinstance(value_cols, list) and len(value_cols) == 1 and value_cols[0] in df.columns:
        return value_cols[0]
    for cand in ("close", "value", "rsi", "ema", "macd", "signal"):
        if cand in lower_map:
            return lower_map[cand]
    numeric = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    return numeric[0] if numeric else cols[0]

def change(
    df: pd.DataFrame,
    *,
    base: str,
    input: Optional[str] = None,
    type: str = "percentage",           # "percentage" | "absolute"
    pct_scale: float = 100.0,
    timestamp: Optional[str] = None,    # ISO 8601
    base_params: Optional[Dict[str, Any]] = None,
    unspecified: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    u = _ensure_dict(unspecified)
    bp = dict(base_params or {})
    bp.update(u)

    if timestamp is None or str(timestamp).strip() == "":
        raise ValueError("[change] 'timestamp' ist Pflicht (ISO 8601)")

    t_anchor = pd.to_datetime(timestamp, errors="coerce", utc=True)
    if t_anchor is pd.NaT:
        raise ValueError(f"[change] 'timestamp' nicht parsebar: {timestamp!r}")

    if DEBUG:
        print(f"[change] incoming base={base!r} input={input!r} type={type!r} pct_scale={pct_scale} ts={t_anchor}")

    base_df, base_used, base_value_cols, base_kind = _compute_base_series(df, base, bp)
    if "Timestamp" not in base_df.columns:
        raise ValueError("[change] Basis-Output enthält keine 'Timestamp'-Spalte")

    # Ankerwert: nächster Timestamp <= anchor (oder nächst-über)
    ts = pd.to_datetime(base_df["Timestamp"], errors="coerce", utc=True)
    base_df = base_df.assign(Timestamp=ts).sort_values("Timestamp", kind="stable")

    pick = _pick_output_column(base_df, base_value_cols, input)
    series = pd.to_numeric(base_df[pick], errors="coerce")

    # Index der Ankerstelle finden (letzter <= t_anchor)
    idx = base_df["Timestamp"].searchsorted(t_anchor, side="right") - 1
    if idx < 0:
        # davor gibt's nix – nimm ersten Wert (brutal, aber klar)
        idx = 0
    anchor_val = float(series.iloc[idx])

    if type.lower() in ("percentage", "percent", "pct"):
        # (x/anchor - 1) * pct_scale
        out_vals = (series / anchor_val - 1.0) * float(pct_scale)
    elif type.lower() in ("absolute", "abs"):
        out_vals = series - anchor_val
    else:
        raise ValueError("[change] type muss 'percentage' oder 'absolute' sein")

    out = pd.DataFrame({
        "Timestamp": base_df["Timestamp"].values,
        "change": out_vals.astype("float32").values,
    })

    used = {
        "base": base,
        "base_kind": base_kind,
        "input": pick,
        "type": type,
        "pct_scale": float(pct_scale),
        "timestamp": str(t_anchor),
        "anchor_index": int(idx),
        "anchor_value": float(anchor_val),
        "base_params": dict(base_used or {}),
        "merged_base_params": dict(bp),
    }

    if DEBUG:
        print(f"[change] anchor idx={idx} value={anchor_val}")
        print(f"[change] tail={out['change'].tail(3).tolist()}")

    return out, used, ["change"]
