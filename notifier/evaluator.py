# notifier/evaluator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import logging
import math
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Set
import hashlib
from pathlib import Path
from collections import OrderedDict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import NOTIFIER_ENDPOINT, PRICE_API_ENDPOINT
# Optional: lokaler Profiles-Pfad, falls API-Write nicht geht
try:
    from config import PROFILES_NOTIFIER  # type: ignore
except Exception:
    PROFILES_NOTIFIER = None  # type: ignore[assignment]

# -----------------------------------------------------------------------------
# Logging & ENV
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("notifier.evaluator")

ENV = os.getenv
def _bool_env(key: str, default: bool) -> bool:
    v = ENV(key)
    if v is None: return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}

def _int_env(key: str, default: int) -> int:
    try:
        v = int(ENV(key, str(default)))
        return v
    except Exception:
        return default

DEBUG_HTTP   = _bool_env("EVAL_DEBUG_HTTP", True)
DEBUG_VALUES = _bool_env("EVAL_DEBUG_VALUES", True)

HTTP_TIMEOUT = float(_int_env("EVAL_HTTP_TIMEOUT", 15))
HTTP_RETRIES = _int_env("EVAL_HTTP_RETRIES", 3)
CACHE_MAX    = _int_env("EVAL_CACHE_MAX", 256)

# -----------------------------------------------------------------------------
# Global HTTP Session mit Retries
# -----------------------------------------------------------------------------
_SESSION: Optional[requests.Session] = None

def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    s = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        read=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    _SESSION = s
    return s

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

ALIASES = {
    "==": "eq",
    "=":  "eq",
    "!=": "ne",
    "<>": "ne",
    ">=": "gte",
    "≤":  "lte",
    "<=": "lte",
    "≥":  "gte",
}

def _normalize_op(op: str) -> str:
    s = (op or "").strip().lower()
    return ALIASES.get(s, s)

# -----------------------------------------------------------------------------
# HTTP Utils
# -----------------------------------------------------------------------------
def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: float = HTTP_TIMEOUT) -> Any:
    tries = max(1, HTTP_RETRIES)
    last_err: Optional[Exception] = None
    sess = _get_session()
    for i in range(tries):
        try:
            if DEBUG_HTTP:
                log.debug(f"[HTTP] GET {url} params={params}")
            r = sess.get(url, params=params, timeout=timeout)
            if DEBUG_HTTP:
                log.debug(f"[HTTP] {r.status_code} {r.url}")
            r.raise_for_status()
            try:
                return r.json()
            except Exception as je:
                last_err = je
                log.warning(f"[HTTP] JSON-Decode-Fehler bei GET {url}: {je} (try {i+1}/{tries})")
        except Exception as e:
            last_err = e
            log.warning(f"[HTTP] Fehler bei GET {url}: {e} (try {i+1}/{tries})")
        time.sleep(0.25 * (i + 1))
    raise RuntimeError(f"HTTP GET fehlgeschlagen: {url} :: {last_err}")

def _http_put_json(url: str, payload: Dict[str, Any], timeout: float = HTTP_TIMEOUT) -> Any:
    tries = max(1, HTTP_RETRIES)
    last_err: Optional[Exception] = None
    sess = _get_session()
    for i in range(tries):
        try:
            if DEBUG_HTTP:
                log.debug(f"[HTTP] PUT {url} json_keys={list(payload.keys())}")
            r = sess.put(url, json=payload, timeout=timeout)
            if DEBUG_HTTP:
                log.debug(f"[HTTP] {r.status_code} {r.url}")
            r.raise_for_status()
            return r.json() if r.text else {}
        except Exception as e:
            last_err = e
            log.warning(f"[HTTP] Fehler bei PUT {url}: {e} (try {i+1}/{tries})")
        time.sleep(0.25 * (i + 1))
    raise RuntimeError(f"HTTP PUT fehlgeschlagen: {url} :: {last_err}")

def _http_post_json(url: str, payload: Dict[str, Any], timeout: float = HTTP_TIMEOUT) -> Any:
    tries = max(1, HTTP_RETRIES)
    last_err: Optional[Exception] = None
    sess = _get_session()
    for i in range(tries):
        try:
            if DEBUG_HTTP:
                log.debug(f"[HTTP] POST {url} json_keys={list(payload.keys())}")
            r = sess.post(url, json=payload, timeout=timeout)
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
# Indicator-Aufruf & Value-Extraktion (LRU-Cache)
# -----------------------------------------------------------------------------
_INDICATOR_CACHE: "OrderedDict[Tuple[str, str, str, str, str], Dict[str, Any]]" = OrderedDict()

def _indicator_cache_key(name: str, symbol: str, chart_interval: str, indicator_interval: str, params: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    pkey = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
    return (name.lower(), symbol, chart_interval, indicator_interval, pkey)

def _cache_get(key: Tuple[str, str, str, str, str]) -> Optional[Dict[str, Any]]:
    val = _INDICATOR_CACHE.get(key)
    if val is not None:
        # move-to-end for LRU
        _INDICATOR_CACHE.move_to_end(key)
    return val

def _cache_put(key: Tuple[str, str, str, str, str], value: Dict[str, Any]) -> None:
    _INDICATOR_CACHE[key] = value
    _INDICATOR_CACHE.move_to_end(key)
    if len(_INDICATOR_CACHE) > max(8, CACHE_MAX):
        _INDICATOR_CACHE.popitem(last=False)

def _fetch_indicator_series(name: str, symbol: str, chart_interval: str, indicator_interval: str, params: Dict[str, Any]) -> Dict[str, Any]:
    key = _indicator_cache_key(name, symbol, chart_interval, indicator_interval, params)
    cached = _cache_get(key)
    if cached is not None:
        return cached

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
    t0 = time.perf_counter()
    data = _http_get_json(url, params=query)
    dt = (time.perf_counter() - t0) * 1000
    if DEBUG_VALUES:
        log.debug(f"[FETCH] done {name} in {dt:.1f} ms")
    if not isinstance(data, dict) or "data" not in data:
        raise RuntimeError(f"/indicator lieferte kein dict mit 'data'. name={name}")
    _cache_put(key, data)
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
        return ["EMA", "ema", "Ema", "value"]
    if n == "rsi":
        return ["RSI", "rsi", "value"]
    if n == "macd":
        return ["Histogram", "hist", "MACD", "Signal", "signal", "value"]
    if n == "price":
        return ["Price", "Close", "close", "value"]
    if n in {"golden_cross", "death_cross", "macd_cross"}:
        return ["signal", "value"]
    return ["value"]

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

    dedup: List[str] = []
    seen: Set[str] = set()
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

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")

def _safe_max_iso(ts_list: List[Optional[str]], fallback: Optional[str] = None) -> Optional[str]:
    vals = [t for t in ts_list if isinstance(t, str) and t]
    if not vals:
        return fallback
    # ISO-8601 ist lexikographisch vergleichbar, wenn Format konsistent ist
    try:
        return max(vals)
    except Exception:
        return fallback

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
        g["deactivated_at"] = _now_iso()
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
                    g["deactivated_at"] = _now_iso()
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
    t0 = time.perf_counter()

    op_raw = (cond.get("op") or "")
    op = _normalize_op(op_raw)
    if op not in OPS:
        return False, {"error": f"unknown_operator:{op_raw}", "normalized": op}

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
            return False, {"error": "left_indicator_fetch_failed", "exception": str(e), "left": left_label}

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
        # Legacy-Modus
        if right_pct_legacy is not None and right_abs_legacy is None:
            return False, {"error": "right_change_without_base", "hint": "right_absolut erforderlich oder right_label='change' verwenden"}
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
                return False, {"error": "right_indicator_fetch_failed", "exception": str(e), "right": right_label}

            if right_val is None:
                return False, {"error": "right_value_none"}

        if (right_label or "").strip().lower() != "change" and right_pct_legacy is not None and right_val is not None:
            right_val = right_val * (1.0 + (right_pct_legacy / 100.0))

    # Vergleich
    try:
        result = bool(OPS[op](float(left_val), float(right_val)))  # type: ignore[arg-type]
    except Exception as e:
        return False, {"error": "operator_error", "exception": str(e)}

    dt = (time.perf_counter() - t0) * 1000.0

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
        "op_norm": op,
        "result": result,
        "duration_ms": round(dt, 2),
    }
    if DEBUG_VALUES:
        log.debug(f"[EVAL] {main_symbol}@{main_interval} {left_label} {op} {right_label} -> {result} ({dt:.1f} ms)")
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
    t0 = time.perf_counter()
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

    dt = (time.perf_counter() - t0) * 1000.0
    if DEBUG_VALUES:
        log.debug(f"[GROUP] {profile.get('name')}[{group_index}] {symbol}@{main_interval} -> {bool(group_result)} "
                  f"(conds={len(conditions)}, {dt:.1f} ms)")
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

    t_start = time.perf_counter()
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
    seen_run_keys: Set[str] = set()
    now_iso = _now_iso()

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
                ts_candidates: List[Optional[str]] = []
                try:
                    left_ts = cond_details[0].get("left", {}).get("ts") if cond_details else None
                    if left_ts:
                        ts_candidates.append(left_ts)
                    for d in cond_details:
                        rts = d.get("right", {}).get("ts")
                        if rts:
                            ts_candidates.append(rts)
                except Exception:
                    pass
                bar_ts = _safe_max_iso(ts_candidates, fallback=now_iso) or now_iso

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
                        group_deactivated = True
                    else:
                        log.warning(f"[AUTO-DEACT] Konnte Gruppe NICHT deaktivieren: profile={profile.get('name')} group_index={g_idx}")

    dt_total = (time.perf_counter() - t_start) * 1000.0
    log.info(f"Gesamt ausgelöste Gruppen (nach per-Run Dedupe): {len(triggered)} — Laufzeit: {dt_total:.1f} ms")
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
