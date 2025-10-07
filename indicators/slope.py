# src/trading_indicators/slope.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple

import numpy as np
import pandas as pd

# Debug-Flag (kannst du über ENV oder Config togglen)
DEBUG = True

# ────────────────────────────────────────────────────────────
# Public SPEC – wird von eurer Registry gelesen (list_indicators)
# ────────────────────────────────────────────────────────────
SPEC: Dict[str, Any] = {
    "name": "slope",
    "summary": "Lineare Least-Squares-Steigung eines Basis-Indikators (pro Bar).",
    "required_params": {               # <- für euren UI/Contract
        "base":   "string",            # z. B. 'ema', 'rsi', 'macd', ...
        "window": "int",               # >= 2
    },
    "default_params": {
        "base_params": {},             # Parameter für den Basis-Indikator
        "input": None,                 # gewünschte Output-Spalte bei Multi-Output-Indikatoren
    },
    "choices": {
        # keine festen Choices hier – UI kann Strings/Ints frei erlauben
    },
    "outputs": ["slope"],              # eine Spalte 'slope'
    "sort_order": 35,
}

# ────────────────────────────────────────────────────────────
# Rolling Least-Squares Slope (vektorisiert; äquidistante t=0..k-1)
# ────────────────────────────────────────────────────────────
def _rolling_lr_slope(y: pd.Series, window: int) -> pd.Series:
    """
    slope = (k*Σ(t*y) - Σ(t)*Σ(y)) / (k*Σ(t^2) - (Σ(t))^2),  t=0..k-1
    Vektorisiert via np.convolve. Erste (window-1) Werte -> NaN.
    """
    k = int(window)
    if k < 2:
        raise ValueError("window muss >= 2 sein")

    arr = pd.to_numeric(y, errors="coerce").to_numpy(dtype=float)
    n = arr.shape[0]

    t = np.arange(k, dtype=float)
    sum_t  = k * (k - 1) / 2.0
    sum_t2 = (k * (k - 1) * (2 * k - 1)) / 6.0
    denom = k * sum_t2 - (sum_t ** 2)
    if denom == 0.0:
        raise ZeroDivisionError("Denominator 0 – window zu klein?")

    sum_y  = np.convolve(arr, np.ones(k, dtype=float), mode="valid")
    sum_ty = np.convolve(arr, t,                      mode="valid")
    slope_valid = (k * sum_ty - sum_t * sum_y) / denom

    out = np.full(n, np.nan, dtype=float)
    out[k - 1:] = slope_valid
    return pd.Series(out, index=y.index)

# ────────────────────────────────────────────────────────────
# Spaltenwahl (robust gegenüber case & Aliases)
# ────────────────────────────────────────────────────────────
def _pick_output_column(base_df: pd.DataFrame, value_cols: List[str], want: Optional[str]) -> str:
    """
    Wähle die Spalte, auf der die Steigung berechnet werden soll.
    Präferenz:
      1) explizites 'want' (case-insensitive, inkl. Aliases)
      2) wenn base nur eine value_col hat -> diese
      3) Heuristik auf bekannten Namen ('signal','hist','macd','value', 'ema','rsi','close')
      4) erste numerische Spalte
      5) erste Spalte
    """
    cols = list(base_df.columns)
    # Timestamp raus, falls enthalten
    if "Timestamp" in cols:
        cols.remove("Timestamp")

    if not cols:
        raise ValueError("[slope] Basis-Output hat keine Daten-Spalten")

    lower_map = {c.lower(): c for c in cols}
    val_map   = {c.lower(): c for c in (value_cols or [])}

    if want:
        key = str(want).strip().lower()
        aliases = {"histogram": "hist", "signal_line": "signal", "macdline": "macd"}
        key = aliases.get(key, key)
        # erst gegen value_cols matchen (sauberer)
        if key in val_map:
            pick = val_map[key]
            if DEBUG:
                print(f"[slope] pick via input (value_cols): '{want}' -> '{pick}'")
            return pick
        # dann gegen reale df-Spalten (falls Output-Name abweicht)
        if key in lower_map:
            pick = lower_map[key]
            if DEBUG:
                print(f"[slope] pick via input (df-cols): '{want}' -> '{pick}'")
            return pick
        raise ValueError(f"[slope] input='{want}' nicht in {value_cols} / {cols}")

    # nur eine value_col → nimm die
    if isinstance(value_cols, list) and len(value_cols) == 1 and value_cols[0] in base_df.columns:
        if DEBUG:
            print(f"[slope] pick via single value_col -> '{value_cols[0]}'")
        return value_cols[0]

    # Heuristik
    for cand in ("signal", "hist", "macd", "value", "ema", "rsi", "close"):
        if cand in lower_map:
            pick = lower_map[cand]
            if DEBUG:
                print(f"[slope] pick via heuristic -> '{pick}' aus {cols}")
            return pick

    # erste numerische Spalte
    numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(base_df[c])]
    if numeric_cols:
        if DEBUG:
            print(f"[slope] pick via numeric fallback -> '{numeric_cols[0]}'")
        return numeric_cols[0]

    if DEBUG:
        print(f"[slope] WARN: keine numerische Spalte, fallback -> '{cols[0]}'")
    return cols[0]

# ────────────────────────────────────────────────────────────
# Compute – Contract-kompatibel: (DataFrame, used_params, value_cols)
# ────────────────────────────────────────────────────────────
def compute(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    """
    Berechnet die Rolling-LS-Steigung eines *Basis*-Indikators.
    Erwartet in params:
      - 'base'        : str (Name des Basis-Indikators, z. B. 'ema', 'rsi', 'macd', ...)
      - 'window'      : int >= 2
      - 'base_params' : dict (Parameter für den Basis-Indikator)
      - 'input'       : Optional[str] (welcher Output bei Multi-Output)
        (Alias akzeptiert: 'base_output')
    Rückgabe:
      - DataFrame mit Spalten ['Timestamp','slope'] (float32)
      - used_params (inkl. aufgelöster 'input'-Spalte und normalisierten base_params)
      - value_cols = ['slope']
    """
    base_name: str = params["base"]
    window: int = int(params["window"])
    if window < 2:
        raise ValueError("window muss >= 2 sein")

    base_params: Dict[str, Any] = dict(params.get("base_params") or {})
    input_col: Optional[str] = params.get("input")
    if input_col is None and "base_output" in params:
        input_col = params.get("base_output")

    # Lazy-Imports aus eurem Paket-Root
    from . import get_indicator_spec, compute_indicator, validate_params

    # Basis-Spec + Param-Validierung
    base_spec = get_indicator_spec(base_name)
    if base_spec is None:
        raise ValueError(f"[slope] unbekannter Basis-Indikator: {base_name}")

    base_params_valid = validate_params(base_spec, base_params)

    if DEBUG:
        print(f"[slope] START base={base_name} window={window} input={input_col} base_params={base_params_valid}")
        print(f"[slope] df shape={getattr(df, 'shape', None)}")

    # Basis-Indikator berechnen – nutzt euren einheitlichen Wrapper
    base_df, base_used, base_value_cols, _extras = compute_indicator(base_spec, df, base_params_valid)

    if not isinstance(base_df, pd.DataFrame) or "Timestamp" not in base_df.columns:
        raise TypeError(f"[slope] Basis '{base_name}' lieferte kein valides DataFrame mit 'Timestamp'")

    # Spalte auswählen
    pick = _pick_output_column(base_df, base_value_cols, input_col)
    series = pd.to_numeric(base_df[pick], errors="coerce")

    if DEBUG:
        nans = int(series.isna().sum())
        print(f"[slope] picked series='{pick}' len={len(series)} NaNs={nans} head={series.head(3).tolist()}")

    # Rolling-LS-Slope berechnen
    slope_series = _rolling_lr_slope(series, window)

    # Ausgabe-Frame formen (float32 wie im restlichen Stack)
    out = pd.DataFrame({
        "Timestamp": base_df["Timestamp"].values,
        "slope": slope_series.astype("float32")
    })

    used = {
        "base": base_name,
        "window": int(window),
        "input": pick,
        "base_params": base_params_valid,
    }

    if DEBUG:
        tail = out.tail(3).to_dict("records")
        print(f"[slope] DONE slope tail={tail}")

    return out, used, ["slope"]
