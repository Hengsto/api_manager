# indicators/slope.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
import pandas as pd
import json
import importlib

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
        k = str(want).strip().lower()
        if k in val_map: return val_map[k]
        if k in lower_map: return lower_map[k]
        raise ValueError(f"[slope] input='{want}' nicht gefunden. value_cols={value_cols} cols={cols}")
    if isinstance(value_cols, list) and len(value_cols) == 1 and value_cols[0] in df.columns:
        return value_cols[0]
    for cand in ("rsi","ema","signal","value","close","macd","price"):
        if cand in lower_map: return lower_map[cand]
    numeric = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    return numeric[0] if numeric else cols[0]

def _try_import(mod: str):
    try:
        return importlib.import_module(mod)
    except Exception as e:
        if DEBUG:
            print(f"[slope] import miss: {mod} ({type(e).__name__}: {e})")
        return None

# ---- lokale Minimal-Indikatoren (Fallback) ---------------------------------

def _ensure_price_cols(df: pd.DataFrame) -> pd.DataFrame:
    lower = {c.lower(): c for c in df.columns}
    rename = {}
    for lc, full in (("open","Open"),("high","High"),("low","Low"),("close","Close")):
        if lc in lower: rename[lower[lc]] = full
    for short, full in (("o","Open"),("h","High"),("l","Low"),("c","Close")):
        if short in lower and full not in df.columns: rename[lower[short]] = full
    if rename:
        if DEBUG: print(f"[slope] ensure_price_cols rename={rename}")
        df = df.rename(columns=rename)
    return df

def _local_price_series(df: pd.DataFrame, source: str = "Close") -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    df = _ensure_price_cols(df)
    src = {"c":"Close","close":"Close","o":"Open","open":"Open","h":"High","high":"High","l":"Low","low":"Low"}.get(str(source).lower(), "Close")
    if src not in df.columns:
        raise ValueError(f"[slope] price source '{src}' nicht im DataFrame")
    out = pd.DataFrame({"Timestamp": pd.to_datetime(df["Timestamp"], errors="coerce"), src: pd.to_numeric(df[src], errors="coerce")})
    return out, {"source": src}, [src]

def _local_rsi(df: pd.DataFrame, length: int = 14, source: str = "Close") -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    price_df, used, _ = _local_price_series(df, source)
    x = price_df[source].astype("float64")
    delta = x.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/length, adjust=False).mean()
    roll_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    out = pd.DataFrame({"Timestamp": price_df["Timestamp"], "rsi": rsi.astype("float32")})
    return out, {"length": length, **used}, ["rsi"]

def _local_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9, source: str = "Close") -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    price_df, used, _ = _local_price_series(df, source)
    x = price_df[source].astype("float64")
    ema_fast = x.ewm(span=fast, adjust=False).mean()
    ema_slow = x.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    out = pd.DataFrame({
        "Timestamp": price_df["Timestamp"],
        "macd": macd.astype("float32"),
        "signal": sig.astype("float32"),
        "hist": hist.astype("float32"),
    })
    return out, {"fast": fast, "slow": slow, "signal": signal, **used}, ["macd","signal","hist"]

def _compute_base_series(df: pd.DataFrame, base_name: str, base_params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any], List[str], str]:
    name = str(base_name).strip().lower()

    # 1) Versuche Registries (wenn vorhanden)
    for mod in ("trading_indicators.core_registry", "core_registry"):
        m = _try_import(mod)
        if m and hasattr(m, "get_indicator_spec") and hasattr(m, "compute_indicator"):
            spec = m.get_indicator_spec(name)
            if spec:
                params = base_params
                if hasattr(m, "validate_params"):
                    params = m.validate_params(spec, dict(base_params or {}))
                out_df, used, value_cols, _ = m.compute_indicator(spec, df, params)
                return out_df, used, value_cols, "indicator"
    for mod in ("trading_indicators.signals_registry", "signals_registry"):
        m = _try_import(mod)
        if m and hasattr(m, "get_signal_spec") and hasattr(m, "compute_signal"):
            spec = m.get_signal_spec(name)
            if spec:
                out_df, used, value_cols = m.compute_signal(spec, df, dict(base_params or {}))
                return out_df, used, value_cols, "signal"
    for mod in ("trading_indicators.custom_registry", "custom_registry"):
        m = _try_import(mod)
        if m and hasattr(m, "get_custom_spec") and hasattr(m, "compute_custom"):
            spec = m.get_custom_spec(name)
            if spec:
                out_df, used, value_cols = m.compute_custom(spec, df, dict(base_params or {}))
                return out_df, used, value_cols, "custom"

    # 2) Lokaler Fallback
    if name in ("price", "close", "open", "high", "low"):
        src = base_params.get("source", name)
        out_df, used, cols = _local_price_series(df, src)
        return out_df, used, cols, "local"
    if name == "rsi":
        length = int(base_params.get("length", base_params.get("period", 14)))
        source = base_params.get("source", "Close")
        out_df, used, cols = _local_rsi(df, length=length, source=source)
        return out_df, used, cols, "local"
    if name == "macd":
        fast = int(base_params.get("fast", 12))
        slow = int(base_params.get("slow", 26))
        signal = int(base_params.get("signal", 9))
        source = base_params.get("source", "Close")
        out_df, used, cols = _local_macd(df, fast=fast, slow=slow, signal=signal, source=source)
        return out_df, used, cols, "local"

    raise ValueError(f"[slope] unbekannter Basis-Indikator: {base_name} (keine Registry und kein lokaler Fallback)")

def slope(
    df: pd.DataFrame,
    *,
    base: str,
    window: int,
    input: Optional[str] = None,
    base_params: Optional[Dict[str, Any]] = None,
    unspecified: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    u = _ensure_dict(unspecified)
    bp = dict(base_params or {})
    bp.update(u)

    window = int(window)
    if window < 1:
        raise ValueError("window muss >= 1 sein")

    if DEBUG:
        print(f"[slope] incoming: base={base!r} window={window} input={input!r} "
              f"base_params.keys={list((base_params or {}).keys())} unspecified.keys={list(u.keys())}")
        print(f"[slope] merged_base_keys={list(bp.keys())}")

    base_df, base_used, base_value_cols, base_kind = _compute_base_series(df, base, bp)
    if "Timestamp" not in base_df.columns:
        raise ValueError("[slope] Basis-Output enthält keine 'Timestamp'-Spalte")

    pick = _pick_output_column(base_df, base_value_cols, input)

    price_like = {"close","open","high","low","price","hlc3","ohlc4","typical"}
    if pick.lower() in price_like and not base.lower().startswith(("rsi","stoch","cci","macd")):
        raise ValueError("[slope] Für Preis-Serien bitte 'change' verwenden; slope ist für normierte Indikatoren gedacht.")

    series = pd.to_numeric(base_df[pick], errors="coerce")
    slope_vals = (series - series.shift(window)) / float(window)

    out = pd.DataFrame({"Timestamp": base_df["Timestamp"].values, "slope": slope_vals.astype("float32").values})
    used = {
        "base": base, "base_kind": base_kind, "input": pick, "window": int(window),
        "base_params": dict(base_used or {}), "merged_base_params": dict(bp),
    }

    if DEBUG:
        print(f"[slope] pick={pick} series_tail={series.tail(3).tolist()} slope_tail={slope_vals.tail(3).tolist()}")

    return out, used, ["slope"]
