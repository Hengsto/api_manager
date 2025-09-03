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
import hashlib
from pathlib import Path

import requests

from config import NOTIFIER_ENDPOINT, PRICE_API_ENDPOINT
# Optional: lokaler Profiles-Pfad, falls API-Write nicht geht
try:
    from config import PROFILES_NOTIFIER  # type: ignore
except Exception:
    PROFILES_NOTIFIER = None  # type: ignore[assignment]

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("notifier.evaluator")

DEBUG_HTTP = True
DEBUG_VALUES = True

# -----------------------------------------------------------------------------
# Operatoren
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
# HTTP Utils
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

def _http_put_json(url: str, payload: Dict[str, Any], timeout: float = 15.0) -> Any:
    tries = 2
    last_err: Optional[Exception] = None
    for i in range(tries):
        try:
            if DEBUG_HTTP:
                log.debug(f"[HTTP] PUT {url} json_keys={list(payload.keys())}")
            r = requests.put(url, json=payload, timeout=timeout)
            if DEBUG_HTTP:
                log.debug(f"[HTTP] {r.status_code} {r.url}")
            r.raise_for_status()
            return r.json() if r.text else {}
        except Exception as e:
            last_err = e
            log.warning(f"[HTTP] Fehler bei PUT {url}: {e} (try {i+1}/{tries})")
            time.sleep(0.25 * (i + 1))
    raise RuntimeError(f"HTTP PUT fehlgeschlagen: {url} :: {last_err}")

def _http_post_json(url: str, payload: Dict[str, Any], timeout: float = 15.0) -> Any:
    tries = 2
    last_err: Optional[Exception] = None
    for i in range(tries):
        try:
            if DEBUG_HTTP:
                log.debug(f"[HTTP] POST {url} json_keys={list(payload.keys())}")
            r = requests.post(url, json=payload, timeout=timeout)
            if DEBUG_HTTP:
                log.debug(f"[HTTP] {r.status_code} {r.url}")
            r.raise_for_status()
            return r.json() if r.text else {}
        except Exception as e:
            last_err = e
            log.warning(f"[HTTP] Fehler bei POST {url}: {e} (try {i+1}/{tries})")
            time.sleep(0.25 * (i + 1))
    raise RuntimeError(f"HTTP POST fehlgeschlagen: {url} :: {last_err}")

# -----------------------------------------------------------------------------
# Profile & Indicators-Meta
# -----------------------------------------------------------------------------
def _load_profiles() -> List[Dict[str, Any]]:
    url = f"{NOTIFIER_ENDPOINT}/profiles"
    data = _http_get_json(url)
    if not isinstance(data, list):
        raise RuntimeError("Profiles-Endpoint lieferte kein List-JSON.")
    log.info(f"Profile geladen: {len(data)}")
    return data

def _load_indicators_meta() -> Dict[str, Dict[str, Any]]:
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
# Legacy Label Parser
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
# Spec-Resolver
# -----------------------------------------------------------------------------
def resolve_spec_and_params(
    label: str,
    params: Optional[Dict[str, Any]] = None,
    preferred_output: Optional[str] = None,
) -> Tuple[str, Optional[str], Dict[str, Any], Optional[str]]:
    p = dict(params or {})
    s = (label or "").strip()
    if not s:
        return "invalid", None, {}, None

    s_low = s.lower()

    if s_low == "value":
        target = p.get("target", None)
        try:
            val = float(target)
        except Exception:
            raise RuntimeError(f"Ungültiger value.target: {target!r}")
        return "const", None, {"value": val, "target": val}, "value"

    if s_low == "change":
        base = p.get("baseline", p.get("source", None))
        if base is None:
            raise RuntimeError("change erfordert right_params.baseline (oder 'source').")
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

    if params is not None:
        return "api", s_low, p, preferred_output

    legacy = _legacy_parse_label_if_needed(s)
    if legacy:
        name, gen = legacy
        return "api", name, gen, preferred_output

    return "api", s_low, {}, preferred_output

# -----------------------------------------------------------------------------
# Indicator-Aufruf & Value-Extraktion
# -----------------------------------------------------------------------------
_INDICATOR_CACHE: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}

def _indicator_cache_key(name: str, symbol: str, chart_interval: str, indicator_interval: str, params: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    pkey = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
    return (name.lower(), symbol, chart_interval, indicator_interval, pkey)

def _fetch_indicator_series(name: str, symbol: str, chart_interval: str, indicator_interval: str, params: Dict[str, Any]) -> Dict[str, Any]:
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

def _pick_value_from_row(row: Dict[str, Any], preferred_cols: Optional[List[str]] = None) -> Tuple[Optional[float], Optional[str]]:
    if preferred_cols:
        for c in preferred_cols:
            if isinstance(c, str) and c in row and _is_number(row[c]):
                return float(row[c]), c
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

    pref: List[str] = []
    if isinstance(chosen_output, str) and chosen_output:
        pref.append(chosen_output)

    spec = meta.get(name.lower())
    if spec and isinstance(spec.get("outputs"), list) and spec["outputs"]:
        pref += [str(x) for x in spec["outputs"] if isinstance(x, (str, int, float))]
    pref += _default_output_priority_for(name)

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
def _resolve_right_side(cond: Dict[str, Any], main_symbol: str, main_interval: str) -> Tuple[str, str]:
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

def _stable_event_key(profile_id: Any, group_index: int, symbol: str, interval: str) -> str:
    payload = {"profile_id": profile_id, "group_index": group_index, "symbol": symbol, "interval": interval}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))

def _stable_event_id(profile_id: Any, group_index: int, symbol: str, interval: str) -> str:
    raw = _stable_event_key(profile_id, group_index, symbol, interval)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# ---- Auto-Deactivate ---------------------------------------------------------
def _normalize_deactivate_mode(group: Dict[str, Any]) -> Optional[str]:
    """
    Returns: "true", "any_true", or None
    Accepts aliases:
      - true:  "true", "full", "match"
      - any:   "any_true", "any", "partial"
    """
    val = group.get("deactivate_on")
    if val is None:
        # legacy boolean flag support: auto_deactivate: true  -> "true"
        if group.get("auto_deactivate") is True:
            return "true"
        return None
    s = str(val).strip().lower()
    if s in {"true", "full", "match"}:
        return "true"
    if s in {"any_true", "any", "partial"}:
        return "any_true"
    return None

def _should_deactivate_group(group: Dict[str, Any], cond_details: List[Dict[str, Any]], group_ok: bool) -> Tuple[bool, str]:
    mode = _normalize_deactivate_mode(group)
    if not mode:
        return False, ""
    if mode == "true":
        return (True, "group_true") if group_ok else (False, "")
    # any_true
    any_true = any(bool(c.get("result")) for c in cond_details)
    return (True, "any_true") if any_true else (False, "")

def _api_update_group_active(profile: Dict[str, Any], group_index: int, active: bool, reason: str) -> bool:
    """
    Versucht per API zu aktualisieren.
    Strategie:
      1) PUT /profiles/{id} mit komplettem Profile (bekannt aus _load_profiles())
         (kompatibel zur üblichen CRUD-API)
      2) Falls es eine spezielle Route gäbe (z.B. /profiles/{id}/deactivate), könnte man die hier einbauen.
    """
    try:
        pid = profile.get("id")
        if not pid:
            return False
        # Profil-Kopie modifizieren
        p2 = json.loads(json.dumps(profile))
        groups = p2.get("condition_groups") or []
        if not (0 <= group_index < len(groups)):
            return False
        g = groups[group_index]
        g["active"] = active
        g["deactivated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
        g["deactivated_reason"] = reason

        url = f"{NOTIFIER_ENDPOINT}/profiles/{pid}"
        _http_put_json(url, p2)
        return True
    except Exception as e:
        log.debug(f"[AUTO-DEACT] API-Update fehlgeschlagen: {e}")
        return False

def _local_file_update_group_active(profile_id: Any, group_index: int, active: bool, reason: str) -> bool:
    try:
        path = PROFILES_NOTIFIER  # type: ignore[name-defined]
    except Exception:
        path = None
    if not path:
        return False
    path = Path(path)
    if not path.exists():
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return False
        changed = False
        for p in data:
            if p.get("id") == profile_id:
                groups = p.get("condition_groups") or []
                if 0 <= group_index < len(groups):
                    g = groups[group_index]
                    g["active"] = active
                    g["deactivated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
                    g["deactivated_reason"] = reason
                    changed = True
                break
        if not changed:
            return False
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
        return True
    except Exception as e:
        log.debug(f"[AUTO-DEACT] Local file update failed: {e}")
        return False

def _deactivate_group(profile: Dict[str, Any], group_index: int, reason: str) -> bool:
    # bevorzugt API, dann lokal
    ok = _api_update_group_active(profile, group_index, active=False, reason=reason)
    if ok:
        log.info(f"[AUTO-DEACT] Gruppe deaktiviert (API): profile={profile.get('name')} group_index={group_index} reason={reason}")
        return True
    ok = _local_file_update_group_active(profile.get("id"), group_index, active=False, reason=reason)
    if ok:
        log.info(f"[AUTO-DEACT] Gruppe deaktiviert (local file): profile={profile.get('name')} group_index={group_index} reason={reason}")
        return True
    log.warning(f"[AUTO-DEACT] Deaktivierung fehlgeschlagen: profile={profile.get('name')} group_index={group_index}")
    return False

# -----------------------------------------------------------------------------
# Condition-Evaluation
# -----------------------------------------------------------------------------
def evaluate_condition_for_symbol(
    meta: Dict[str, Dict[str, Any]],
    cond: Dict[str, Any],
    main_symbol: str,
    main_interval: str
) -> Tuple[bool, Dict[str, Any]]:
    op = (cond.get("op") or "").lower()
    if op not in OPS:
        return False, {"error": f"unknown_operator:{op}"}

    # LEFT
    left_label   = cond.get("left") or ""
    left_params  = cond.get("left_params") or {}
    left_output  = (cond.get("left_output") or "").strip() or None

    mode_l, left_name, left_p, left_out = resolve_spec_and_params(left_label, left_params, left_output)
    if mode_l == "invalid":
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
            return False, {"error": "left_indicator_fetch_failed", "exception": str(e)}

    if left_val is None:
        return False, {"error": "left_value_none"}

    # RIGHT
    right_label   = cond.get("right") or ""
    right_params  = cond.get("right_params") or {}
    right_output  = (cond.get("right_output") or "").strip() or None

    right_abs_legacy = _numeric_or_none(cond.get("right_absolut"))
    right_pct_legacy = _numeric_or_none(cond.get("right_change"))

    r_symbol, r_interval = _resolve_right_side(cond, main_symbol, main_interval)

    right_val: Optional[float] = None
    right_col: Optional[str] = None
    right_ts: Optional[str] = None

    if right_label.strip() == "":
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
        else:
            try:
                right_val, right_col, right_ts = _last_value_for_indicator(
                    meta=meta,
                    name=right_name,                   # type: ignore[arg-type]
                    symbol=r_symbol,
                    chart_interval=main_interval,
                    indicator_interval=r_interval,
                    params=right_p,
                    chosen_output=right_out,
                )
            except Exception as e:
                return False, {"error": "right_indicator_fetch_failed", "exception": str(e)}

            if right_val is None:
                return False, {"error": "right_value_none"}

        if (right_label or "").strip().lower() != "change" and right_pct_legacy is not None and right_val is not None:
            right_val = right_val * (1.0 + (right_pct_legacy / 100.0))

    # Vergleich
    try:
        result = bool(OPS[op](float(left_val), float(right_val)))  # type: ignore[arg-type]
    except Exception as e:
        return False, {"error": "operator_error", "exception": str(e)}

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
        "op": (cond.get("op") or "").lower(),
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

    return bool(group_result), per_details

# -----------------------------------------------------------------------------
# Top-Level: run_check
# -----------------------------------------------------------------------------
def run_check() -> List[Dict[str, Any]]:
    """
    Evaluator mit optionalem Auto-Deaktivieren je Gruppe:
      - deactivate_on: "true"|"any_true" (oder Aliase)
      - bei Erfolg: Gruppe -> active:false (per API oder lokal), Break weiterer Symbole
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
    seen_run_keys: set[str] = set()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")

    for p_idx, profile in enumerate(profiles):
        if not profile.get("enabled", True):
            continue

        groups = profile.get("condition_groups") or []
        for g_idx, group in enumerate(groups):
            if not group.get("active", True):
                continue

            raw_symbols = [s for s in (group.get("symbols") or []) if s]
            symbols = list(dict.fromkeys(raw_symbols))
            main_interval = (group.get("interval") or "").strip()
            if not symbols or not main_interval:
                continue

            group_deactivated = False  # Break-Schutz nach Deaktivierung

            for sym in symbols:
                if group_deactivated:
                    break

                group_ok, cond_details = _eval_group_for_symbol(meta, profile, group, sym, g_idx)

                # Bar TS bestimmen
                bar_ts: Optional[str] = None
                try:
                    left_ts = cond_details[0].get("left", {}).get("ts") if cond_details else None
                    ts_candidates = [left_ts] if left_ts else []
                    for d in cond_details:
                        rts = d.get("right", {}).get("ts")
                        if rts:
                            ts_candidates.append(rts)
                    bar_ts = max([t for t in ts_candidates if isinstance(t, str)], default=None)
                except Exception:
                    bar_ts = None
                if not bar_ts:
                    bar_ts = now_iso

                # Trigger handling
                if group_ok:
                    key_raw = _stable_event_key(profile.get("id"), g_idx, sym, main_interval)
                    if key_raw in seen_run_keys:
                        continue
                    seen_run_keys.add(key_raw)

                    alarm_id = _stable_event_id(profile.get("id"), g_idx, sym, main_interval)
                    payload = {
                        "alarm_id": alarm_id,
                        "ts": now_iso,
                        "bar_ts": bar_ts,
                        "profile_id": profile.get("id"),
                        "profile_name": profile.get("name"),
                        "group_index": g_idx,
                        "group_name": group.get("name") or f"group_{g_idx}",
                        "symbol": sym,
                        "interval": main_interval,
                        "exchange": group.get("exchange") or None,
                        "telegram_bot_id": group.get("telegram_bot_id") or None,
                        "telegram_bot_token": group.get("telegram_bot_token") or None,
                        "telegram_chat_id": group.get("telegram_chat_id") or None,
                        "description": group.get("description") or None,
                        "conditions": cond_details,
                        "result": True,
                    }
                    triggered.append(payload)
                    log.info(f"✅ Gruppe erfüllt: {profile.get('name')} / {payload['group_name']} / {sym} ({main_interval}) [alarm_id={alarm_id[:12]}]")

                # Auto-Deactivate?
                should_deact, reason = _should_deactivate_group(group, cond_details, group_ok)
                if should_deact:
                    ok = _deactivate_group(profile, g_idx, reason=reason)
                    if ok:
                        group_deactivated = True  # abbrechen, keine weiteren Symbole
                        # Bonus: kein weiterer Trigger für diese Gruppe/Symbolkombination
                    else:
                        log.warning(f"[AUTO-DEACT] Konnte Gruppe NICHT deaktivieren: profile={profile.get('name')} group_index={g_idx}")

    log.info(f"Gesamt ausgelöste Gruppen (nach per-Run Dedupe): {len(triggered)}")
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
