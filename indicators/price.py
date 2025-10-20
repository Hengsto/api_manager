# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Union

import pandas as pd
import numpy as np

DEBUG = True  # per ENV/Config überschreibbar


def price(
    df: pd.DataFrame,
    source: str = "close",
    *,
    fillna: Optional[Union[str, float, int]] = None,  # "ffill"|"bfill"|zahl|None
    dropna: bool = True,
    ensure_monotonic: bool = True,
    dedupe: Optional[str] = "keep_last",     # None|"keep_first"|"keep_last"
    tz_naive: bool = True,                   # TZ entfernen, falls vorhanden
) -> Tuple[pd.DataFrame, Dict, List[str]]:
    """
    Liefert die angeforderte Preisquelle als Serie 'price'.

    Erwartet df mit:
      - 'Timestamp'
      - und eine der Spalten: 'Open','High','Low','Close'

    Parameter:
      - source: case-insensitive; erlaubt NUR:
          Basis:  close/open/high/low  (auch c/o/h/l)
      - fillna:  "ffill" | "bfill" | numerischer Wert | None
      - dropna:  NaN-Zeilen verwerfen (nach fillna)
      - ensure_monotonic: Zeitachse sortieren
      - dedupe:  Duplikate auf 'Timestamp' entfernen (None|keep_first|keep_last)
      - tz_naive: TZ-Information aus Timestamp entfernen

    Rückgabe:
      out_df:    ['Timestamp','price'] (price=float32)
      used_params: {'source': <norm>, 'derived': False, 'fillna':..., 'dropna':..., ...}
      value_cols: ['price']
    """
    # --- Vorbedingungen ---
    if "Timestamp" not in df.columns:
        raise ValueError("price: 'Timestamp' fehlt im DataFrame.")

    # --- Timestamp aufbereiten ---
    ts = pd.to_datetime(df["Timestamp"], errors="coerce")
    if tz_naive:
        # Falls mit TZ, in naive konvertieren (lokal/UTC-agnostisch, konsistent intern)
        if getattr(ts.dt, "tz", None) is not None:
            ts = ts.dt.tz_convert(None)

    if DEBUG:
        print(f"[price] START source={source} rows={len(df)} tz_naive={tz_naive}")

    # --- Source/Alias normalisieren (NUR Basisquellen) ---
    src_raw = str(source).strip().lower()
    base_map = {
        "c": "Close", "close": "Close",
        "o": "Open",  "open":  "Open",
        "h": "High",  "high":  "High",
        "l": "Low",   "low":   "Low",
    }

    if src_raw not in base_map:
        allowed = ["close", "open", "high", "low", "c", "o", "h", "l"]
        raise ValueError(f"price: unbekannte source='{source}'. Erlaubt: {allowed}")

    src_col = base_map[src_raw]
    if src_col not in df.columns:
        raise ValueError(f"price: Spalte '{src_col}' fehlt im DataFrame.")

    series = pd.to_numeric(df[src_col], errors="coerce").astype("float64")
    if DEBUG:
        print(f"[price] using base column '{src_col}'")

    # --- FillNA optional ---
    if fillna is not None:
        if isinstance(fillna, str):
            f = fillna.lower().strip()
            if f == "ffill":
                series = series.ffill()
            elif f == "bfill":
                series = series.bfill()
            else:
                raise ValueError("price: fillna string muss 'ffill' oder 'bfill' sein")
            if DEBUG:
                print(f"[price] applied fillna='{f}'")
        elif isinstance(fillna, (int, float)) and not isinstance(fillna, bool):
            series = series.fillna(float(fillna))
            if DEBUG:
                print(f"[price] applied fillna numeric={fillna}")
        else:
            raise ValueError("price: fillna muss 'ffill'|'bfill'|zahl|None sein")

    out = pd.DataFrame({"Timestamp": ts, "price": series.astype("float32")})

    # --- Drop NaNs ---
    if dropna:
        before = len(out)
        out = out.dropna(subset=["Timestamp", "price"])
        if DEBUG:
            print(f"[price] dropna removed {before - len(out)} rows")

    # --- Sort/Monotonic ---
    if ensure_monotonic and not out["Timestamp"].is_monotonic_increasing:
        out = out.sort_values("Timestamp", kind="stable")
        if DEBUG:
            print("[price] sorted by Timestamp (monotonic increasing)")

    # --- Dedupe ---
    if dedupe:
        before = len(out)
        out = out.drop_duplicates(subset=["Timestamp"], keep=("first" if dedupe == "keep_first" else "last"))
        if DEBUG:
            print(f"[price] dedup '{dedupe}' removed {before - len(out)} rows")

    used: Dict = {
        "source": src_col,
        "derived": False,  # absichtlich immer False – keine Derived-Quellen mehr
        "fillna": fillna,
        "dropna": bool(dropna),
        "ensure_monotonic": bool(ensure_monotonic),
        "dedupe": dedupe,
        "tz_naive": bool(tz_naive),
    }

    if DEBUG:
        head = out.head(3).to_dict("records")
        tail = out.tail(3).to_dict("records")
        print(f"[price] DONE rows={len(out)} head={head} tail={tail}")

    return out, used, ["price"]
