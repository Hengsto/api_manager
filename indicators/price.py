# -*- coding: utf-8 -*-
# src/trading_indicators/price.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import functools
import numpy as np
import pandas as pd

DEBUG = True

SPEC: Dict[str, Any] = {
    "name": "price",
    "summary": "Gibt den OHLC-Preis (z. B. Close) als Indikator aus. Optional: Autoload mit Intervall-Fallback.",
    "required_params": {
        "source": "string",  # "Open" | "High" | "Low" | "Close"
    },
    "default_params": {
        "source": "Close",

        # OPTIONAL: nur nötig, wenn du Autoload+Fallback willst:
        "ctx_symbol": None,         # z. B. "BTCUSDT_FULL"
        "ctx_interval": None,       # bevorzugtes Intervall als String (z. B. "4h")
        "interval_policy": "as_is", # "as_is" | "smaller" | "smallest"
        "available_intervals": None,# Liste z. B. ["15m","1h","4h","1d"] – überschreibt Hook
        "max_attempts": 24,
    },
    "choices": {
        "source": ["Open","High","Low","Close"],
        "interval_policy": ["as_is","smaller","smallest"],
    },
    "outputs": ["price"],
    "sort_order": 10,
}

# ──────────────────────────────────────────────────────────────────────────────
# Optionale Loader-Hooks (kein harter Import auf data_management)
# ──────────────────────────────────────────────────────────────────────────────
_HOOKS = {}
try:
    # Erwartet Funktionen in trading_indicators/loader_hooks.py:
    #   - load_ohlc(symbol:str, interval:str) -> pd.DataFrame[Timestamp,Open,High,Low,Close,Volume]
    #   - available_intervals() -> List[str]
    from . import loader_hooks  # type: ignore
    _HOOKS["load_ohlc"] = getattr(loader_hooks, "load_ohlc", None)
    _HOOKS["available_intervals"] = getattr(loader_hooks, "available_intervals", None)
except Exception:
    # Hooks sind optional – Autoload funktioniert dann nur mit per-Param übergebener Intervallliste
    _HOOKS["load_ohlc"] = None
    _HOOKS["available_intervals"] = None

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────

_OHLC_CANDIDATES = ("Close","close","Adj Close","adj_close","c")

def _pick_price_column(df: pd.DataFrame, want: Optional[str]) -> str:
    if want and want in df.columns: return want
    if want and want.lower() in df.columns: return want.lower()
    if want and want.capitalize() in df.columns: return want.capitalize()
    for c in _OHLC_CANDIDATES:
        if c in df.columns: return c
    # erste numerische
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]): return c
    return df.columns[0]

def _interval_to_minutes(s: str) -> int:
    try:
        s = s.strip().lower()
        val, unit = int(s[:-1]), s[-1]
        mult = {"m":1, "h":60, "d":1440, "w":10080}[unit]
        return val * mult
    except Exception:
        return 10**12

def _intervals_small_to_large(intervals: List[str]) -> List[str]:
    return sorted({i for i in (intervals or []) if isinstance(i, str) and i.strip()},
                  key=_interval_to_minutes)

# kleiner Cache für Autoloads
@functools.lru_cache(maxsize=256)
def _cached_load(symbol: str, interval: str) -> pd.DataFrame:
    loader = _HOOKS.get("load_ohlc")
    if loader is None:
        raise RuntimeError("[price] Kein Loader-Hook definiert (loader_hooks.load_ohlc fehlt).")
    df = loader(symbol, interval)
    # Timestamp normalisieren
    if "Timestamp" in df.columns:
        ts = pd.to_datetime(df["Timestamp"], errors="coerce")
    else:
        ts = pd.to_datetime(df["Timestamp_ISO"], errors="coerce")
    df = df.loc[ts.notna()].copy()
    df["Timestamp"] = ts.astype("datetime64[ns]")
    return df[["Timestamp","Open","High","Low","Close","Volume"]]

def _get_available_intervals(param_list: Optional[List[str]]) -> List[str]:
    if isinstance(param_list, list) and param_list:
        return _intervals_small_to_large(param_list)
    hook = _HOOKS.get("available_intervals")
    if callable(hook):
        try:
            return _intervals_small_to_large(hook())
        except Exception:
            pass
    # Fallback: konservative Defaults
    return _intervals_small_to_large(["15m","30m","1h","2h","4h","8h","12h","1d","2d","3d","1w"])

def _autoload_with_fallback(
    symbol: str,
    prefer_interval: Optional[str],
    policy: str,
    available: List[str],
    max_attempts: int,
) -> tuple[pd.DataFrame, str]:
    ordered = _intervals_small_to_large(available)
    if not ordered:
        raise RuntimeError("[price] Keine verfügbaren Intervalle bekannt.")

    if policy == "smallest":
        candidates = ordered
    elif policy == "smaller" and prefer_interval:
        req = prefer_interval
        req_min = _interval_to_minutes(req)
        smaller = [i for i in ordered if _interval_to_minutes(i) < req_min]
        # nächst-kleiner zuerst
        candidates = [req] + list(reversed(smaller))
    else:
        # as_is oder kein prefer → nur prefer (oder kleinstes, falls None)
        candidates = [prefer_interval] if prefer_interval else [ordered[0]]

    attempts = 0
    last_err: Optional[Exception] = None
    for iv in candidates:
        if not iv: continue
        try:
            attempts += 1
            if attempts > max_attempts: break
            df = _cached_load(symbol, iv)
            if not df.empty:
                return df, iv
        except Exception as e:
            last_err = e
            if DEBUG:
                print(f"[price] Autoload fail: {symbol} {iv}: {e}")
            continue
    raise RuntimeError(f"[price] Autoload-Fallback gescheitert (symbol={symbol}, prefer={prefer_interval}, policy={policy}) – last={last_err}")

# ──────────────────────────────────────────────────────────────────────────────
# Compute
# ──────────────────────────────────────────────────────────────────────────────

def compute(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    """
    Nutzt vorrangig das übergebene DF (Standard-Indikator-Pfad).
    Falls DF leer/None und ctx_* gesetzt → optionales Autoload mit Intervall-Fallback
    über loader_hooks (oder per Param gelieferte Intervallliste).

    Params:
      - source: "Open"|"High"|"Low"|"Close" (Default "Close")
      - ctx_symbol: Optional[str]
      - ctx_interval: Optional[str]
      - interval_policy: "as_is"|"smaller"|"smallest"
      - available_intervals: Optional[List[str]]  # überschreibt Hook
      - max_attempts: int
    """
    src: str = str(params.get("source", "Close"))
    ctx_symbol: Optional[str] = params.get("ctx_symbol") or params.get("symbol")
    ctx_interval: Optional[str] = params.get("ctx_interval") or params.get("indicator_interval")
    policy: str = str(params.get("interval_policy","as_is")).strip().lower()
    avail_param = params.get("available_intervals")
    max_attempts: int = int(params.get("max_attempts", 24))

    if DEBUG:
        print(f"[price] START src={src} ctx_symbol={ctx_symbol} ctx_interval={ctx_interval} policy={policy}")

    used_interval: Optional[str] = None
    use_df: Optional[pd.DataFrame] = None

    # 1) Normaler Pfad: übergebenes DF verwenden
    if isinstance(df, pd.DataFrame) and not df.empty and "Timestamp" in df.columns:
        use_df = df
    else:
        # 2) Optionaler Autoload nur, wenn Symbol vorhanden
        if ctx_symbol:
            # ohne Loader-Hook geht Autoload nicht — dann leeres Ergebnis
            if _HOOKS.get("load_ohlc") is not None:
                try:
                    available = _get_available_intervals(avail_param)
                    use_df, used_interval = _autoload_with_fallback(
                        symbol=ctx_symbol,
                        prefer_interval=(str(ctx_interval) if ctx_interval else None),
                        policy=policy,
                        available=available,
                        max_attempts=max_attempts,
                    )
                    if DEBUG:
                        print(f"[price] Autoload SUCCESS: {ctx_symbol}@{used_interval} rows={len(use_df)}")
                except Exception as e:
                    if DEBUG:
                        print(f"[price] Autoload FAILED: {e}")
                    use_df = None
            else:
                if DEBUG:
                    print("[price] Kein Loader-Hook verfügbar – Autoload übersprungen.")
                use_df = None
        else:
            use_df = None

    if use_df is None or use_df.empty:
        out = pd.DataFrame({"Timestamp": pd.Series(dtype="datetime64[ns]"),
                            "price": pd.Series(dtype="float32")})
        used = {"source": src, "ctx_symbol": ctx_symbol, "ctx_interval": ctx_interval,
                "used_interval": used_interval}
        return out, used, ["price"]

    col = _pick_price_column(use_df, src)
    out = use_df[["Timestamp"]].copy(deep=False)
    out["price"] = pd.to_numeric(use_df[col], errors="coerce").astype("float32")

    if not out["Timestamp"].is_monotonic_increasing:
        out = out.sort_values("Timestamp", kind="stable")

    used = {
        "source": col,
        "ctx_symbol": ctx_symbol,
        "ctx_interval": ctx_interval,
        "used_interval": used_interval,
    }
    return out, used, ["price"]
