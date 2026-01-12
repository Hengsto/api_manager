# indicators/change.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import importlib
from typing import Dict, Any, Optional, List, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# Konfig
# ---------------------------------------------------------------------
DEBUG = True
PRICE_API_BASE = os.getenv("PRICE_API_BASE", "http://127.0.0.1:8000").rstrip("/")

SPEC: Dict[str, Any] = {
    "name": "change",
    "summary": "Delta/Prozentänderung einer Basisreihe (z. B. RSI oder Preis) zu Timestamp oder über N Perioden.",
    "required_params": {"base": "string"},
    "default_params": {"type": "percentage", "pct_scale": 100.0},
    "outputs": ["change"],
    "sort_order": 35,
}

# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------
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
        raise ValueError("[change] keine Daten-Spalten")
    lower_map = {c.lower(): c for c in cols}
    val_map   = {c.lower(): c for c in (value_cols or [])}
    if want:
        k = str(want).strip().lower()
        if k in val_map: return val_map[k]
        if k in lower_map: return lower_map[k]
        raise ValueError(f"[change] input='{want}' nicht gefunden. value_cols={value_cols} cols={cols}")
    if isinstance(value_cols, list) and len(value_cols) == 1 and value_cols[0] in df.columns:
        return value_cols[0]
    for cand in ("rsi","ema","value","close","macd","price","signal"):
        if cand in lower_map: return lower_map[cand]
    numeric = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    return numeric[0] if numeric else cols[0]

def _try_import(mod: str):
    try:
        return importlib.import_module(mod)
    except Exception as e:
        if DEBUG:
            print(f"[change] import miss: {mod} ({type(e).__name__}: {e})")
        return None

def _ensure_price_cols(df: pd.DataFrame) -> pd.DataFrame:
    lower = {c.lower(): c for c in df.columns}
    rename = {}
    for lc, full in (("open","Open"),("high","High"),("low","Low"),("close","Close")):
        if lc in lower: rename[lower[lc]] = full
    for short, full in (("o","Open"),("h","High"),("l","Low"),("c","Close")):
        if short in lower and full not in df.columns: rename[lower[short]] = full
    if rename:
        if DEBUG: print(f"[change] ensure_price_cols rename={rename}")
        df = df.rename(columns=rename)
    return df

def _local_price_series(df: pd.DataFrame, source: str = "Close") -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    df = _ensure_price_cols(df)
    src = {"c":"Close","close":"Close","o":"Open","open":"Open","h":"High","high":"High","l":"Low","low":"Low"}.get(str(source).lower(), "Close")
    if src not in df.columns:
        raise ValueError(f"[change] price source '{src}' nicht im DataFrame")
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

def _sanitize_params_for_http(d: Dict[str, Any]) -> Dict[str, Any]:
    """Entfernt interne Inject-Keys (beginnt mit '_')."""
    out: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        if str(k).startswith("_"):
            continue
        out[k] = v
    return out

def _rows_to_df(payload: Dict[str, Any], orig_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Unterstützt:
      - rows: list[dict]
      - rows: list[list|tuple] + columns: list[str]
    Erkennt Zeitspalten u.a.: Timestamp, timestamp, Timestamp_ISO, time, t, date, Date, index.
    Konvertiert Epochen (Sek/Millis) → UTC. Fallback: Timestamp aus orig_df, wenn Länge passt.
    """
    rows = payload.get("rows")
    cols = payload.get("columns")
    data = payload.get("data")

    df = pd.DataFrame()
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        df = pd.DataFrame(rows)
    elif isinstance(rows, list) and isinstance(cols, list) and rows and isinstance(rows[0], (list, tuple)):
        try:
            df = pd.DataFrame(rows, columns=cols)
        except Exception as e:
            if DEBUG: print(f"[change/_rows_to_df] matrix build failed: {type(e).__name__}: {e}")
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        df = pd.DataFrame(data)
    elif isinstance(data, list) and isinstance(cols, list) and data and isinstance(data[0], (list, tuple)):
        try:
            df = pd.DataFrame(data, columns=cols)
        except Exception as e:
            if DEBUG: print(f"[change/_rows_to_df] matrix(data) build failed: {type(e).__name__}: {e}")

    if df.empty:
        if DEBUG: print("[change/_rows_to_df] empty payload → empty DataFrame")
        return df

    cand_names = ["Timestamp", "timestamp", "Timestamp_ISO", "time", "t", "date", "Date", "index"]
    ts_col = None
    for c in cand_names:
        if c in df.columns:
            ts_col = c
            break

    if ts_col is None and "index" in payload and isinstance(payload["index"], list) and len(payload["index"]) == len(df):
        df["Timestamp"] = payload["index"]
        ts_col = "Timestamp"

    if ts_col is not None:
        s = df[ts_col]
        if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
            s = pd.to_datetime(s.where(s < 10**12, s/1000.0), unit="s", utc=True, errors="coerce")
        else:
            s = pd.to_datetime(s, utc=True, errors="coerce")
        df["Timestamp"] = s
    elif orig_df is not None and "Timestamp" in orig_df.columns and len(orig_df) == len(df):
        if DEBUG: print("[change/_rows_to_df] fallback Timestamp from orig_df (len match)")
        df["Timestamp"] = pd.to_datetime(orig_df["Timestamp"], utc=True, errors="coerce")
    else:
        if DEBUG:
            print(f"[change/_rows_to_df] no timestamp column detected; cols={list(df.columns)}")

    return df

# ---------------------------------------------------------------------
# Base-Resolver
# ---------------------------------------------------------------------
def _http_fetch_base(
    base_name: str,
    base_params: Dict[str, Any],
    orig_df: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any], List[str], str]:
    symbol = base_params.get("_symbol")
    c_int  = base_params.get("_chart_interval")
    i_int  = base_params.get("_indicator_interval") or c_int
    if not symbol or not c_int:
        raise ValueError("[change/http] fehlende _symbol/_chart_interval in base_params (Injection aus Proxy)")

    http_params = _sanitize_params_for_http(base_params)
    try:
        import requests
        qs = {
            "name": base_name,
            "symbol": symbol,
            "chart_interval": c_int,
            "indicator_interval": i_int,
            "params": json.dumps(http_params, separators=(",", ":"), sort_keys=True),
        }
        if DEBUG:
            print(f"[change/http] GET {PRICE_API_BASE}/indicator name={base_name} sym={symbol} chart={c_int} ind={i_int} params={qs['params']}")
        r = requests.get(f"{PRICE_API_BASE}/indicator", params=qs, timeout=20)
        if not r.ok:
            raise RuntimeError(f"[change/http] upstream status={r.status_code} body={r.text[:220]}")
        payload = r.json()
        df = _rows_to_df(payload, orig_df=orig_df)
        if "Timestamp" not in df.columns or df["Timestamp"].isna().all():
            raise RuntimeError("[change/http] Upstream lieferte keine brauchbare Zeitachse")
        value_cols = [c for c in df.columns if c != "Timestamp" and pd.api.types.is_numeric_dtype(df[c])]
        if not value_cols:
            value_cols = [c for c in df.columns if c != "Timestamp"]
        used = {"_http": True, "_endpoint": "/indicator", "_base": base_name, "_symbol": symbol, "_chart": c_int, "_ind": i_int}
        return df, used, value_cols, "http"
    except Exception as e:
        raise RuntimeError(f"[change/http] fetch failed for base={base_name}: {type(e).__name__}: {e}")

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

    # 2) Lokaler Fallback (nur das Nötigste)
    if name in ("price", "close", "open", "high", "low"):
        src = base_params.get("source", name)
        out_df, used, cols = _local_price_series(df, src)
        return out_df, used, cols, "local"
    if name == "rsi":
        length = int(base_params.get("length", base_params.get("period", 14)))
        source = base_params.get("source", "Close")
        out_df, used, cols = _local_rsi(df, length=length, source=source)
        return out_df, used, cols, "local"

    # 3) HTTP-Fallback
    return _http_fetch_base(name, base_params, orig_df=df)

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def change(
    df: pd.DataFrame,
    *,
    base: str,
    input: Optional[str] = None,
    type: str = "percentage",
    pct_scale: float = 100.0,
    timestamp: Optional[str] = None,
    base_params: Optional[Dict[str, Any]] = None,
    unspecified: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    """
    Zwei Betriebsmodi:
      - timestamp: berechnet Change relativ zu Wert am/nahe 'timestamp'
      - unspecified.length: n-Perioden-Change (shift)
    """
    u = _ensure_dict(unspecified)
    bp = dict(base_params or {})
    bp.update(u)

    if DEBUG:
        print(f"[change] incoming base={base!r} input={input!r} type={type!r} pct_scale={pct_scale} ts={timestamp}")

    # Basisreihe holen
    base_df, base_used, base_value_cols, base_kind = _compute_base_series(df, base, bp)
    if "Timestamp" not in base_df.columns:
        raise ValueError("[change] Basis-Output enthält keine 'Timestamp'-Spalte")

    base_df = base_df.sort_values("Timestamp").reset_index(drop=True)
    pick = _pick_output_column(base_df, base_value_cols, input)
    series = pd.to_numeric(base_df[pick], errors="coerce")

    t = (type or "").strip().lower()
    if t in ("percent", "percentage"):
        is_pct = True
    elif t in ("abs", "absolute", "delta"):
        is_pct = False
    else:
        is_pct = True  # Default

    out_series = pd.Series(np.nan, index=series.index, dtype="float64")

    # Modus A: timestamp-basierter Vergleich
    ts_anchor_val = None
    if timestamp:
        ts_parsed = pd.to_datetime(timestamp, utc=True, errors="coerce")
        if pd.isna(ts_parsed):
            raise ValueError(f"[change] Ungültiger timestamp: {timestamp}")
        # nächster Index <= ts
        idx = np.searchsorted(base_df["Timestamp"].values, ts_parsed.to_datetime64(), side="right") - 1
        if idx < 0:
            idx = 0
        ts_anchor_val = series.iloc[idx]
        if DEBUG:
            print(f"[change] anchor by timestamp idx={idx} ts={base_df['Timestamp'].iloc[idx]} val={ts_anchor_val}")

        if is_pct:
            out_series = ((series - ts_anchor_val) / ts_anchor_val.replace(0, np.nan)) * float(pct_scale)
        else:
            out_series = series - ts_anchor_val

    # Modus B: n-Perioden-Change
    if ts_anchor_val is None:
        n = int(u.get("length", 0) or 0)
        if n <= 0:
            # Fallback: Standard 1-Schritt
            n = 1
        if DEBUG:
            print(f"[change] length-mode n={n} is_pct={is_pct}")
        prev = series.shift(n)
        if is_pct:
            out_series = ((series - prev) / prev.replace(0, np.nan)) * float(pct_scale)
        else:
            out_series = series - prev

    out_df = pd.DataFrame({"Timestamp": base_df["Timestamp"].values, "change": out_series.astype("float32").values})

    used = {
        "base": base, "base_kind": base_kind, "input": pick,
        "type": ("percentage" if is_pct else "absolute"),
        "pct_scale": float(pct_scale),
        "anchor": (str(timestamp) if timestamp else None),
        "base_params": dict(base_used or {}), "merged_base_params": dict(bp),
    }

    if DEBUG:
        print(f"[change] tail series={series.tail(3).tolist()} change_tail={out_series.tail(3).tolist()}")

    return out_df, used, ["change"]
