# indicators/volume.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
import json
import os

DEBUG = os.getenv("DEBUG", "1") not in ("0", "false", "False")

SPEC: Dict[str, Any] = {
    "name": "volume",
    "summary": "Gibt das Volumen (base/quote) des Basis-Datasets zurück (auto-detect, optional Präferenz).",
    "required_params": {},  # keine harten Requireds, um UI-Fehler (source='close') zu tolerieren
    "default_params": {"prefer": "auto"},  # 'auto' | 'base' | 'quote'
    "outputs": ["volume"],
    "sort_order": 6,
}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
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

def _resolve_volume_column(
    df: pd.DataFrame,
    requested: Optional[str],
    prefer: str = "auto",
) -> str:
    """
    Wählt eine sinnvolle Volumenspalte aus df.
    - requested: wird NUR akzeptiert, wenn es eine bekannte/validierte Volume-Spalte im DF ist.
    - prefer: 'auto' (Standard), 'base', 'quote'
    """
    cols_lower = {c.lower(): c for c in df.columns}

    # Aliase
    base_aliases = [
        "volume", "vol", "base_volume", "basevolume",
        "volume_base", "vol_base",
    ]
    quote_aliases = [
        "quote_volume", "quotevolume", "volume_quote", "vol_quote",
        "turnover",  # häufig bei Futures-Feeds
    ]
    all_aliases = base_aliases + quote_aliases

    # 1) Wenn requested gesetzt ist, NUR akzeptieren, wenn es tatsächlich eine Volume-Spalte ist
    if requested:
        r = requested.strip()
        # tolerate case versions
        for cand in (r, r.lower(), r.capitalize(), r.title()):
            key = cand.lower()
            if key in cols_lower and key in [a.lower() for a in all_aliases]:
                return cols_lower[key]

    # 2) Präferenzliste aufbauen
    preferred_scan: List[str] = []
    if prefer == "base":
        preferred_scan = base_aliases + quote_aliases
    elif prefer == "quote":
        preferred_scan = quote_aliases + base_aliases
    else:  # auto
        preferred_scan = base_aliases + quote_aliases

    # 3) Erste passende Spalte nehmen
    for a in preferred_scan:
        key = a.lower()
        if key in cols_lower:
            return cols_lower[key]

    # 4) Not found → harter Fehler mit Spaltenliste
    raise ValueError(
        f"[VOLUME] Keine Volumenspalte gefunden. Gesucht in Aliases={preferred_scan}. "
        f"Vorhandene Spalten: {list(df.columns)}"
    )

# ──────────────────────────────────────────────────────────────────────────────
# Main Logic
# ──────────────────────────────────────────────────────────────────────────────
def volume(
    df: pd.DataFrame,
    source: Optional[str] = None,      # wird nur respektiert, wenn es wirklich eine Volume-Spalte ist
    prefer: str = "auto",              # 'auto' | 'base' | 'quote'
    **kwargs: Any,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    Gibt 1:1 die Volumenspalte aus dem Eingabe-DataFrame zurück.
    - Tolerant gegenüber UI/Proxy, die fälschlich `source='close'` o.ä. übergeben.
    - 'prefer' steuert Auswahl-Reihenfolge zwischen base/quote.
    - 'Timestamp' wird als Spalte erzwungen.
    """
    if df is None or df.empty:
        if DEBUG:
            print("[VOLUME] Eingabe-DataFrame ist leer oder None.")
        return pd.DataFrame(columns=["Timestamp", "volume"]), ["prefer"], ["volume"]

    try:
        resolved = _resolve_volume_column(df, requested=source, prefer=str(prefer).lower())
    except Exception as e:
        # Zusätzlicher Kontext, wenn jemand versehentlich 'close' liefert
        if DEBUG:
            print(f"[VOLUME] Auflösung fehlgeschlagen (source={source}, prefer={prefer}): {type(e).__name__}: {e}")
        raise

    # 'Timestamp' als Spalte erzwingen
    if "Timestamp" in df.columns:
        ts = df["Timestamp"]
    else:
        ts = df.index

    out = pd.DataFrame(
        {"Timestamp": ts, "volume": pd.to_numeric(df[resolved], errors="coerce")},
        copy=False,
    )

    if DEBUG:
        try:
            n = len(out)
            n_nan = out["volume"].isna().sum()
            vmin = out["volume"].min() if n else None
            vmax = out["volume"].max() if n else None
            vsum = out["volume"].sum() if n else None
            print(f"[VOLUME] resolved='{resolved}' (requested='{source}', prefer='{prefer}'), len={n}, NaN={n_nan}")
            if n > 0:
                print(f"[VOLUME][HEAD] {out.head(3).to_dict(orient='list')}")
                print(f"[VOLUME][DESC] min={vmin} max={vmax} sum={vsum}")
        except Exception as e:
            print(f"[VOLUME] debug-failed: {type(e).__name__}: {e}")

    # 'used' spiegelt die tatsächlich relevanten Parameter wider
    used_params: List[str] = ["prefer"]
    if source:
        used_params.append("source")
    return out, used_params, ["volume"]

# Optional: einheitlicher Entry-Point für generischen Dispatcher
def compute(df: pd.DataFrame, **kwargs: Any):
    return volume(df, **kwargs)

# ──────────────────────────────────────────────────────────────────────────────
# Debug Run
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test = pd.DataFrame({
        "open": [10, 11, 12],
        "high": [11, 12, 13],
        "low": [9, 10, 11],
        "close": [10.5, 11.5, 12.5],
        "volume": [1000, 1200, 900],
        "quote_volume": [6500.0, 7200.0, 5900.0],
    })
    # 1) Auto (bevorzugt base)
    out, used, cols = volume(test)
    print(out)
    print("used:", used, "cols:", cols)

    # 2) quote bevorzugen
    out2, used2, cols2 = volume(test, prefer="quote")
    print(out2)
    print("used:", used2, "cols:", cols2)

    # 3) source='close' (soll ignoriert und trotzdem Volume liefern)
    out3, used3, cols3 = volume(test, source="close")
    print(out3)
    print("used:", used3, "cols:", cols3)
