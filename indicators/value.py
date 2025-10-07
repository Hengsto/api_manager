# src/trading_indicators/value.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import numpy as np

DEBUG = True

SPEC: Dict[str, Any] = {
    "name": "value",
    "summary": "Gibt eine vorhandene Spalte aus dem DataFrame 1:1 zurück (Standard: 'Value').",
    "required_params": {
        "column": "string",   # welche Spalte aus df? (z. B. 'Value')
        "numeric": "bool",    # als float ausgeben (coerce)? Default: True
    },
    "default_params": {
        "column": "Value",
        "numeric": True,
    },
    "choices": {},
    "outputs": ["value"],     # fester Output-Name für UI/Mapping
    "sort_order": 9,
}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_ts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stellt 'Timestamp' (datetime64[ns]) sicher, entfernt unparsebare Zeilen
    und sortiert stabil aufsteigend.
    """
    if "Timestamp" in df.columns:
        ts = pd.to_datetime(df["Timestamp"], errors="coerce")
    elif "Timestamp_ISO" in df.columns:
        ts = pd.to_datetime(df["Timestamp_ISO"], errors="coerce")
    else:
        raise ValueError("[value] erwarte Spalte 'Timestamp' oder 'Timestamp_ISO'.")

    out = df.loc[ts.notna()].copy()
    out["Timestamp"] = ts.astype("datetime64[ns]")
    if not out["Timestamp"].is_monotonic_increasing:
        out = out.sort_values("Timestamp", kind="stable")
    return out


def _resolve_column(df: pd.DataFrame, want: Optional[str]) -> str:
    """
    Robuste Spaltenwahl:
    - exakter Treffer
    - case-insensitive Treffer
    - heuristische Kandidaten ('Value','Close', etc.)
    - erste numerische Spalte
    - sonst erste Spalte
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        raise ValueError("[value] DataFrame ist leer.")

    if want and want in df.columns:
        return want

    if want:
        lower_map = {c.lower(): c for c in df.columns}
        k = want.lower()
        if k in lower_map:
            return lower_map[k]

    # Heuristik
    for cand in ("Value", "value", "Close", "close", "Adj Close", "adj_close"):
        if cand in df.columns:
            return cand

    # erste numerische
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            return c

    # Fallback
    return df.columns[0]

# ──────────────────────────────────────────────────────────────────────────────
# Compute
# ──────────────────────────────────────────────────────────────────────────────

def compute(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    """
    Nimmt ein DataFrame mit Zeitspalte ('Timestamp' oder 'Timestamp_ISO') und gibt
    die gewünschte Spalte als 'value' zurück.

    Params:
      - column : str   (Standard 'Value')
      - numeric: bool  (True → zu float32 coerzen; False → Originaldtype beibehalten)
    Returns:
      (DataFrame[['Timestamp','value']], used_params, ['value'])
    """
    col_want: str = str(params.get("column", "Value"))
    as_numeric: bool = bool(params.get("numeric", True))

    if DEBUG:
        print(f"[value] START column={col_want!r} numeric={as_numeric}")

    base = _normalize_ts(df)
    col = _resolve_column(base, col_want)

    out = base[["Timestamp"]].copy(deep=False)

    series = base[col]
    if as_numeric:
        series = pd.to_numeric(series, errors="coerce").astype("float32")
    out["value"] = series

    # Ausgabe säubern
    if as_numeric:
        # unendliche Werte vermeiden
        out.replace([np.inf, -np.inf], np.nan, inplace=True)

    used = {
        "column": col,
        "numeric": as_numeric,
    }

    if DEBUG:
        n = len(out)
        n_nan = int(out["value"].isna().sum())
        print(f"[value] DONE rows={n} NaNs={n_nan} col_used={col!r}")

    return out, used, ["value"]
