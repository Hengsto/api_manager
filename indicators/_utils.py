# indicators/_utils.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Any, Dict, Tuple
import pandas as pd

DEBUG = True

def normalize_chart_df(raw: Any) -> pd.DataFrame:
    """
    Nimmt beliebiges Chart-JSON (dict mit rows/data/klines/candles oder list)
    und gibt ein DataFrame mit garantierter 'Timestamp'-Spalte + OHLC-Aliassen
    (Open/High/Low/Close) zurück. Wirft bei unheilbarem Input HTTP-ähnliche Fehler.
    """
    # 1) Rows extrahieren
    rows = None
    if isinstance(raw, dict):
        for key in ("rows", "data", "candles", "klines"):
            if key in raw:
                rows = raw[key]
                break
    elif isinstance(raw, list):
        rows = raw

    if rows is None:
        raise ValueError("normalize_chart_df: no rows/data found in chart payload")

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("normalize_chart_df: empty chart dataframe")

    # 2) Timestamp mappen
    if "Timestamp" not in df.columns:
        lcmap = {c.lower(): c for c in df.columns}
        picked_src = None

        # Bevorzugte Kandidaten
        for key in ("timestamp", "timestamp_iso", "timestamp_ms", "time", "ts", "date", "datetime"):
            if key in lcmap:
                picked_src = lcmap[key]
                break

        # Generische Heuristik
        if not picked_src:
            for col in df.columns:
                if str(col).lower().startswith("timestamp"):
                    picked_src = col
                    break

        if picked_src:
            df = df.rename(columns={picked_src: "Timestamp"})
        elif df.index.name:
            df = df.reset_index().rename(columns={df.index.name: "Timestamp"})

    if "Timestamp" not in df.columns:
        raise ValueError(f"normalize_chart_df: no 'Timestamp' column (cols={list(df.columns)})")

    # 3) OHLC Aliasse aufräumen
    lower = {c.lower(): c for c in df.columns}
    rename = {}
    # Kurzformen 'o/h/l/c'
    for short, full in (("o", "Open"), ("h", "High"), ("l", "Low"), ("c", "Close")):
        if short in lower and full.lower() not in lower:
            rename[lower[short]] = full
    # Klein-/Großschreibung vereinheitlichen
    for lc, full in (("open","Open"),("high","High"),("low","Low"),("close","Close")):
        if lc in lower:
            rename[lower[lc]] = full
    if rename and DEBUG:
        print(f"[UTIL] normalize_chart_df rename={rename}")
    if rename:
        df = df.rename(columns=rename)

    # 4) Timestamp in datetime umwandeln + NaT raus + stabil sortieren
    ts = pd.to_datetime(df["Timestamp"], errors="coerce")
    mask = ts.notna()
    if not mask.any():
        raise ValueError("normalize_chart_df: all timestamps are NaT")
    if (~mask).any():
        df = df.loc[mask].copy()
        ts = ts.loc[mask]
    df["Timestamp"] = ts
    if not df["Timestamp"].is_monotonic_increasing:
        df = df.sort_values("Timestamp", kind="mergesort")

    if DEBUG:
        try:
            print(f"[UTIL] normalize_chart_df ok rows={len(df)} ts_first={df['Timestamp'].iloc[0]} ts_last={df['Timestamp'].iloc[-1]}")
        except Exception:
            pass

    return df
