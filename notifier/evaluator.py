# notifier/evaluator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import math
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import NOTIFIER_ENDPOINT, PRICE_API_ENDPOINT

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("notifier.evaluator")

# Laute Debugs erwünscht
DEBUG_HTTP = True
DEBUG_VALUES = True

# -----------------------------------------------------------------------------
# Operator-Mapping (mit Toleranz für eq/ne)
# -----------------------------------------------------------------------------
def _op_eq(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 1e-9) -> bool:
    try:
        return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)
    except Exception:
        return False

def _op_ne(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 1e-9) -> bool:
    try:
        return not math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)
    except Exception:
        return False

OPS = {
    "eq":  _op_eq,
    "ne":  _op_ne,
    "gt":  lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
    "lt":  lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
}

# -----------------------------------------------------------------------------
# HTTP Utils (robust, mit kleinen Retries)
# -----------------------------------------------------------------------------
def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: float = 15.0) -> Any:
    tries = 2
    last_err: Optional[Exception] = None
    for i in range(tries):
        try:
            if DEBUG_HTTP:
                log.debug(f"[HTTP] GET {url} params={params}")
            r = requests.get(url, params=params, timeout=timeout)
            if DEBUG_HTTP:
                log.debug(f"[HTTP] {r.status_code} {r.url}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            log.warning(f"[HTTP] Fehler bei GET {url}: {e} (try {i+1}/{tries})")
            time.sleep(0.25 * (i + 1))
    raise RuntimeError(f"HTTP GET fehlgeschlagen: {url} :: {last_err}")

# -----------------------------------------------------------------------------
# Profile & Indicator-Metadaten
# -----------------------------------------------------------------------------
def _load_profiles() -> List[Dict[str, Any]]:
    url = f"{NOTIFIER_ENDPOINT}/profiles"
    data = _http_get_json(url)
    if not isinstance(data, list):
        raise RuntimeError("Profiles-Endpoint lieferte kein List-JSON.")
    log.info(f"Profile geladen: {len(data)}")
    return data

def _load_indicators_meta() -> Dict[str, Dict[str, Any]]:
    """
    Holt die Metadaten von der Price-API (/indicators) und liefert ein Mapping
    {spec_name_lower: spec_dict}. spec_dict enthält u.a. 'name' und 'outputs'.
    """
    url = f"{PRICE_API_ENDPOINT}/indicators"
    items = _http_get_json(url)
    if not isinstance(items, list):
        raise RuntimeError("/indicators lieferte kein List-JSON.")
    meta: Dict[str, Dict[str, Any]] = {}
    for it in items:
        n = (it.get("name") or "").strip()
        if n:
            meta[n.lower()] = it
    log.info(f"Indikator-Metadaten: {len(meta)} Specs")
    return meta

# -----------------------------------------------------------------------------
# Backwards-Compat: alte Label-Pattern nur falls *keine* params übergeben wurden
# -----------------------------------------------------------------------------
_EMA_RE = re.compile(r"^EMA_(\d+)$", re.IGNORECASE)
_RSI_RE = re.compile(r"^RSI_(\d+)$", re.IGNORECASE)
_MACD_RE = re.compile(r"^MACD_(\d+)_(\d+)_(\d+)$", re.IGNORECASE)

def _legacy_parse_label_if_needed(label: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    s = (label or "").strip()
    if not s:
        return None
    m = _EMA_RE.match(s)
    if m:
        return "ema", {"length": int(m.group(1))}
    m = _RSI_RE.match(s)
    if m:
        return "rsi", {"length": int(m.group(1))}
    m = _MACD_RE.match(s)
    if m:
        return "macd", {"fast": int(m.group(1)), "slow": int(m.group(2)), "signal": int(m.group(3))}
    # simple signals (alt)
    s_low = s.lower()
    if s_low in {"golden_cross", "death_cross", "macd_cross"}:
        return s_low, {}
    return None

# -----------------------------------------------------------------------------
# Spec-Resolver (neu): label + params -> (mode, name, params, preferred_output)
# -----------------------------------------------------------------------------
def resolve_spec_and_params(
    label: str,
    params: Optional[Dict[str, Any]] = None,
    preferred_output: Optional[str] = None,
) -> Tuple[str, Optional[str], Dict[str, Any], Optional[str]]:
    """
    - Neue Welt: label ist Spec-Name (z.B. "price","rsi","ema","bitcoin_ahr999", ...)
      -> mode="api", name=label.lower(), params=dict(params)
    - Pseudo-Specs (ohne API):
        "value"  -> params['target'] als Konstante
        "change" -> **prozentual**: target = baseline * (1 + delta/100)
                    baseline Pflicht (Alias 'source' ok), delta in %
      -> mode="const", name=None, params={"value": float}
    - Alte Welt (nur wenn keine params übergeben wurden): EMA_50 / RSI_14 / MACD_12_26_9
      -> mode="api" mit generierten params
    """
    p = dict(params or {})
    s = (label or "").strip()
    if not s:
        return "invalid", None, {}, None

    s_low = s.lower()

    # Pseudo-Specs
    if s_low == "value":
        target = p.get("target", None)
        try:
            val = float(target)
        except Exception:
            raise RuntimeError(f"Ungültiger value.target: {target!r}")
        return "const", None, {"value": val, "target": val}, "value"

    if s_low == "change":
        # prozentuale Änderung: target = baseline * (1 + delta/100)
        base = p.get("baseline", p.get("source", None))
        if base is None:
            raise RuntimeError("change erfordert right_params.baseline (oder 'source' als Alias).")
        try:
            baseline = float(base)
        except Exception:
            raise RuntimeError(f"Ungültiger change.baseline/source: {base!r}")
        try:
            delta = float(p.get("delta", 0))
        except Exception:
            raise RuntimeError(f"Ungültiger change.delta: {p.get('delta')!r}")
        target = baseline * (1.0 + (delta / 100.0))
        return "const", None, {"value": target, "baseline": baseline, "delta": delta, "target": target}, "value"

    # Neue Welt: direkte Specs
    if params is not None:
        return "api", s_low, p, preferred_output

    # Alte Labels (nur falls KEINE params kamen)
    legacy = _legacy_parse_label_if_needed(s)
    if legacy:
        name, gen = legacy
        return "api", name, gen, preferred_output

    # Fallback: treat label as direct spec (api) ohne params
    return "api", s_low, {}, preferred_output

# -----------------------------------------------------------------------------
# Indicator-Aufruf & Value-Extraktion
# -----------------------------------------------------------------------------
_INDICATOR_CACHE: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}

def _indicator_cache_key(name: str, symbol: str, chart_interval: str, indicator_interval: str, params: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    pkey = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
    return (name.lower(), symbol, chart_interval, indicator_interval, pkey)

def _fetch_indicator_series(
    name: str,
    symbol: str,
    chart_interval: str,
    indicator_interval: str,
    params: Dict[str, Any]
) -> Dict[str, Any]:
    key = _indicator_cache_key(name, symbol, chart_interval, indicator_interval, params)
    if key in _INDICATOR_CACHE:
        return _INDICATOR_CACHE[key]

    url = f"{PRICE_API_ENDPOINT}/indicator"
    query = {
        "name": name,
        "symbol": symbol,
        "chart_interval": chart_interval,
        "indicator_interval": indicator_interval,
        "params": json.dumps(params or {}, separators=(",", ":"), sort_keys=True),
    }
    if DEBUG_VALUES:
        log.debug(f"[FETCH] {name} sym={symbol} chart_iv={chart_interval} ind_iv={indicator_interval} params={params}")
    data = _http_get_json(url, params=query)
    if not isinstance(data, dict) or "data" not in data:
        raise RuntimeError(f"/indicator lieferte kein dict mit 'data'. name={name}")
    _INDICATOR_CACHE[key] = data
    return data

def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and x is not None

def _pick_value_from_row(
    row: Dict[str, Any],
    preferred_cols: Optional[List[str]] = None
) -> Tuple[Optional[float], Optional[str]]:
    if preferred_cols:
        for c in preferred_cols:
            if isinstance(c, str) and c in row and _is_number(row[c]):
                return float(row[c]), c
    # Fallback: erste numerische Spalte (außer Timestamp_ISO)
    for k, v in row.items():
        if k == "Timestamp_ISO":
            continue
        if _is_number(v):
            return float(v), k
    return None, None

def _default_output_priority_for(name: str) -> List[str]:
    n = (name or "").lower()
    if n == "ema":
        return ["EMA", "ema", "Ema"]
    if n == "rsi":
        return ["RSI", "rsi"]
    if n == "macd":
        return ["Histogram", "hist", "MACD", "Signal", "signal"]
    if n == "price":
        return ["Price", "Close", "close"]
    if n in {"golden_cross", "death_cross", "macd_cross"}:
        return ["signal", "value"]
    return []

def _last_value_for_indicator(
    meta: Dict[str, Dict[str, Any]],
    name: str,
    symbol: str,
    chart_interval: str,
    indicator_interval: str,
    params: Dict[str, Any],
    chosen_output: Optional[str] = None,
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    series = _fetch_indicator_series(name, symbol, chart_interval, indicator_interval, params)
    rows = series.get("data") or []
    if not rows:
        return None, None, None

    last = rows[-1]

    # Prioritäten: 1) gewählter Output 2) spec.outputs 3) Default-Heuristik
    pref: List[str] = []
    if isinstance(chosen_output, str) and chosen_output:
        pref.append(chosen_output)

    spec = meta.get(name.lower())
    if spec and isinstance(spec.get("outputs"), list) and spec["outputs"]:
        pref += [str(x) for x in spec["outputs"] if isinstance(x, (str, int, float))]
    pref += _default_output_priority_for(name)

    # Dedupe
    dedup = []
    seen = set()
    for x in pref:
        if x not in seen:
            dedup.append(x)
            seen.add(x)

    val, col = _pick_value_from_row(last, dedup)
    ts = last.get("Timestamp_ISO")
    return val, col, ts

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _resolve_right_side(
    cond: Dict[str, Any],
    main_symbol: str,
    main_interval: str
) -> Tuple[str, str]:
    rsym = (cond.get("right_symbol") or "").strip() or main_symbol
    rint = (cond.get("right_interval") or "").strip() or main_interval
    return rsym, rint

def _numeric_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Condition-Evaluation (neu)
# -----------------------------------------------------------------------------
def evaluate_condition_for_symbol(
    meta: Dict[str, Dict[str, Any]],
    cond: Dict[str, Any],
    main_symbol: str,
    main_interval: str
) -> Tuple[bool, Dict[str, Any]]:
    op = (cond.get("op") or "").lower()
    if op not in OPS:
        log.warning(f"❓ Unbekannter Operator: {op}")
        return False, {"error": f"unknown_operator:{op}"}

    # --- LEFT ---------------------------------------------------------------
    left_label   = cond.get("left") or ""
    left_params  = cond.get("left_params") or {}
    left_output  = (cond.get("left_output") or "").strip() or None

    mode_l, left_name, left_p, left_out = resolve_spec_and_params(left_label, left_params, left_output)

    if mode_l == "invalid":
        log.warning(f"❓ Ungültiges left-Label: '{left_label}'")
        return False, {"error": "invalid_left_label", "left": left_label}

    left_val: Optional[float] = None
    left_col: Optional[str] = None
    left_ts: Optional[str] = None
    if mode_l == "const":
        left_val = float((left_p or {}).get("value"))
        left_col = "CONST"
        left_ts  = None
    else:
        try:
            left_val, left_col, left_ts = _last_value_for_indicator(
                meta=meta,
                name=left_name,                      # type: ignore[arg-type]
                symbol=main_symbol,
                chart_interval=main_interval,
                indicator_interval=main_interval,
                params=left_p,
                chosen_output=left_out,
            )
        except Exception as e:
            log.error(f"Left-Indikator-Fehler ({left_label}) [{main_symbol} {main_interval}]: {e}")
            return False, {"error": "left_indicator_fetch_failed", "exception": str(e)}

    if left_val is None:
        return False, {"error": "left_value_none"}

    # --- RIGHT --------------------------------------------------------------
    right_label   = cond.get("right") or ""
    right_params  = cond.get("right_params") or {}
    right_output  = (cond.get("right_output") or "").strip() or None

    right_abs_legacy = _numeric_or_none(cond.get("right_absolut"))       # legacy
    right_pct_legacy = _numeric_or_none(cond.get("right_change"))        # legacy (in %)

    r_symbol, r_interval = _resolve_right_side(cond, main_symbol, main_interval)

    right_val: Optional[float] = None
    right_col: Optional[str] = None
    right_ts: Optional[str] = None

    if right_label.strip() == "":
        # Legacy: reine absolute Schwelle (selten sinnvoll)
        base = right_abs_legacy if right_abs_legacy is not None else 0.0
        if right_pct_legacy is not None:
            right_val = base * (1.0 + (right_pct_legacy / 100.0))
            right_col = "ABS% (legacy)"
        else:
            right_val = base
            right_col = "ABS (legacy)"
        right_ts = None
    else:
        mode_r, right_name, right_p, right_out = resolve_spec_and_params(right_label, right_params, right_output)
        if mode_r == "invalid":
            return False, {"error": "invalid_right_label", "right": right_label}

        if mode_r == "const":
            right_val = float((right_p or {}).get("value"))
            right_col = "CONST"
            right_ts  = None
            # Expliziter Debug für change-Prozent
            if (right_label or "").strip().lower() == "change" and DEBUG_VALUES:
                try:
                    bl = right_p.get("baseline")
                    dl = right_p.get("delta")
                    tg = right_p.get("target")
                    log.debug(f"[EVAL] change% resolved: baseline={bl}, delta%={dl}, target={tg}")
                except Exception:
                    pass
        else:
            try:
                right_val, right_col, right_ts = _last_value_for_indicator(
                    meta=meta,
                    name=right_name,                   # type: ignore[arg-type]
                    symbol=r_symbol,
                    chart_interval=main_interval,      # immer auf Gruppen-Chart mappen
                    indicator_interval=r_interval,
                    params=right_p,
                    chosen_output=right_out,
                )
            except Exception as e:
                log.error(f"Right-Indikator-Fehler ({right_label}) [{r_symbol} {r_interval} -> chart {main_interval}]: {e}")
                return False, {"error": "right_indicator_fetch_failed", "exception": str(e)}

            if right_val is None:
                return False, {"error": "right_value_none"}

        # WICHTIG: bei right == "change" KEIN zusätzliches right_change (legacy %) anwenden
        if (right_label or "").strip().lower() != "change" and right_pct_legacy is not None and right_val is not None:
            if DEBUG_VALUES:
                log.debug(f"[EVAL] apply legacy right_change%: base={right_val} pct={right_pct_legacy}")
            right_val = right_val * (1.0 + (right_pct_legacy / 100.0))

    # --- Vergleich ----------------------------------------------------------
    try:
        result = bool(OPS[op](float(left_val), float(right_val)))  # type: ignore[arg-type]
    except Exception as e:
        log.error(f"💥 Operator-Fehler: {left_val} {op} {right_val} -> {e}")
        return False, {"error": "operator_error", "exception": str(e)}

    if DEBUG_VALUES:
        log.debug(
            f"[EVAL] L {left_label}({(left_name or 'CONST')}.{left_col})={left_val} "
            f"[{main_symbol} {main_interval} @ {left_ts}]  "
            f"{op}  R {right_label or right_abs_legacy}({right_col})={right_val} "
            f"[{r_symbol if right_label else '-'} {r_interval if right_label else '-'} @ {right_ts or '-'}]  -> {result}"
        )

    details = {
        "left": {
            "label": left_label,
            "spec": left_name if mode_l == "api" else None,
            "output": left_out,
            "col": left_col,
            "value": left_val,
            "symbol": main_symbol if mode_l == "api" else None,
            "interval": main_interval if mode_l == "api" else None,
            "ts": left_ts,
            "params": left_p,
        },
        "right": {
            "label": right_label,
            "spec": right_name if right_label and mode_r == "api" else None,
            "output": right_out if right_label else None,
            "col": right_col,
            "value": right_val,
            "symbol": r_symbol if right_label and mode_r == "api" else None,
            "interval": r_interval if right_label and mode_r == "api" else None,
            "ts": right_ts,
            "right_absolut": right_abs_legacy,
            "right_change_legacy_pct": right_pct_legacy,
            "params": right_p if right_label else {},
        },
        "op": op,
        "result": result,
    }
    return result, details

# -----------------------------------------------------------------------------
# Group/Profile Evaluation
# -----------------------------------------------------------------------------
def _eval_group_for_symbol(
    meta: Dict[str, Dict[str, Any]],
    profile: Dict[str, Any],
    group: Dict[str, Any],
    symbol: str,
    group_index: int
) -> Tuple[bool, List[Dict[str, Any]]]:
    conditions: List[Dict[str, Any]] = group.get("conditions") or []
    main_interval = (group.get("interval") or "").strip()
    if not main_interval:
        log.warning(f"Gruppe {group_index} ohne Intervall -> skip.")
        return False, []

    group_result: Optional[bool] = None
    per_details: List[Dict[str, Any]] = []

    for idx, cond in enumerate(conditions):
        res, details = evaluate_condition_for_symbol(meta, cond, symbol, main_interval)
        details["idx"] = idx
        per_details.append(details)

        if group_result is None:
            group_result = res
        else:
            logic = (cond.get("logic") or "and").strip().lower()
            group_result = (group_result or res) if logic == "or" else (group_result and res)

        log.debug(
            f"[GROUP] profile='{profile.get('name')}' group#{group_index} symbol={symbol} "
            f"cond#{idx} -> {res} (logic={cond.get('logic','and')}) group_state={group_result}"
        )

    return bool(group_result), per_details

# -----------------------------------------------------------------------------
# Top-Level: run_check
# -----------------------------------------------------------------------------
def run_check() -> List[Dict[str, Any]]:
    """
    Lädt Profile vom NOTIFIER_ENDPOINT, evaluiert jede aktive Gruppe
    gegen die Price-Chart-API und liefert eine Liste ausgelöster Ereignisse
    (eine Zeile pro erfüllter Gruppe/Symbol; inkl. Details pro Bedingung).
    """
    _INDICATOR_CACHE.clear()

    try:
        profiles = _load_profiles()
    except Exception as e:
        log.error(f"⚠️ Fehler beim Laden der Profile: {e}")
        return []

    try:
        meta = _load_indicators_meta()
    except Exception as e:
        log.error(f"⚠️ Fehler beim Laden der Indikator-Metadaten: {e}")
        return []

    triggered: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")

    for p_idx, profile in enumerate(profiles):
        if not profile.get("enabled", True):
            log.debug(f"Profil deaktiviert: {profile.get('name')}")
            continue

        groups = profile.get("condition_groups") or []
        for g_idx, group in enumerate(groups):
            if not group.get("active", True):
                log.debug(f"Gruppe deaktiviert in Profil {profile.get('name')}")
                continue

            symbols = [s for s in (group.get("symbols") or []) if s]
            main_interval = (group.get("interval") or "").strip()
            if not symbols:
                log.warning(f"Profil '{profile.get('name')}' Gruppe#{g_idx} ohne Symbole -> skip.")
                continue
            if not main_interval:
                log.warning(f"Profil '{profile.get('name')}' Gruppe#{g_idx} ohne Intervall -> skip.")
                continue

            for sym in symbols:
                group_ok, cond_details = _eval_group_for_symbol(meta, profile, group, sym, g_idx)
                if group_ok:
                    payload = {
                        "ts": now_iso,
                        "profile_id": profile.get("id"),
                        "profile_name": profile.get("name"),
                        "group_index": g_idx,
                        "group_name": group.get("name") or f"group_{g_idx}",
                        "symbol": sym,
                        "interval": main_interval,
                        "exchange": group.get("exchange") or None,
                        "telegram_bot_id": group.get("telegram_bot_id") or None,
                        "description": group.get("description") or None,
                        "conditions": cond_details,
                        "result": True,
                    }
                    triggered.append(payload)
                    log.info(f"✅ Gruppe erfüllt: {profile.get('name')} / {payload['group_name']} / {sym} ({main_interval})")

    log.info(f"Gesamt ausgelöste Gruppen: {len(triggered)}")
    return triggered

# -----------------------------------------------------------------------------
# CLI Helper
# -----------------------------------------------------------------------------
def run_evaluator() -> None:
    print("🔄 Evaluator startet …")
    try:
        res = run_check()
    except Exception as e:
        print(f"💥 Fatal: {e}")
        return
    print(f"✅ {len(res)} Gruppe(n) erfüllt.")
    if DEBUG_VALUES and res:
        print(json.dumps(res[:3], indent=2, ensure_ascii=False))
