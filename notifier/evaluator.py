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

# WICHTIG: Gate importieren UND BENUTZEN
from .gate import gate_and_build_triggers

# ─────────────────────────────────────────────────────────────
# Persistenz/Endpoints aus config
# ─────────────────────────────────────────────────────────────
from config import NOTIFIER_ENDPOINT, PRICE_API_ENDPOINT, CHART_API_ENDPOINT  # noqa: F401

# Optional: lokale Pfade
try:
    from config import PROFILES_NOTIFIER  # type: ignore
except Exception:
    PROFILES_NOTIFIER = None  # type: ignore[assignment]

try:
    from config import STATUS_NOTIFIER  # type: ignore
except Exception:
    STATUS_NOTIFIER = None  # type: ignore[assignment]
try:
    from config import OVERRIDES_NOTIFIER  # type: ignore
except Exception:
    OVERRIDES_NOTIFIER = None  # type: ignore[assignment]
try:
    from config import COMMANDS_NOTIFIER  # type: ignore
except Exception:
    COMMANDS_NOTIFIER = None  # type: ignore[assignment]

# ─────────────────────────────────────────────────────────────
# Logging & ENV
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("notifier.evaluator")

ENV = os.getenv
def _bool_env(key: str, default: bool) -> bool:
    v = ENV(key)
    return default if v is None else str(v).strip().lower() in {"1","true","yes","y","on"}

def _int_env(key: str, default: int) -> int:
    try:
        return int(ENV(key, str(default)))
    except Exception:
        return default

DEBUG_HTTP   = _bool_env("EVAL_DEBUG_HTTP", True)
DEBUG_VALUES = _bool_env("EVAL_DEBUG_VALUES", True)

HTTP_TIMEOUT = float(_int_env("EVAL_HTTP_TIMEOUT", 15))
HTTP_RETRIES = _int_env("EVAL_HTTP_RETRIES", 3)
CACHE_MAX    = _int_env("EVAL_CACHE_MAX", 256)

# ─────────────────────────────────────────────────────────────
# Pfade & Locks
# ─────────────────────────────────────────────────────────────
def _to_path(p: Any | None) -> Optional[Path]:
    if p is None: return None
    return p if isinstance(p, Path) else Path(str(p)).expanduser().resolve()

_BASE_DIR = _to_path(PROFILES_NOTIFIER).parent if _to_path(PROFILES_NOTIFIER) else Path(os.getcwd())

_STATUS_PATH    = _to_path(STATUS_NOTIFIER)    or (_BASE_DIR / "notifier_status.json")
_OVERRIDES_PATH = _to_path(OVERRIDES_NOTIFIER) or (_BASE_DIR / "notifier_overrides.json")
_COMMANDS_PATH  = _to_path(COMMANDS_NOTIFIER)  or (_BASE_DIR / "notifier_commands.json")

for _p in (_STATUS_PATH, _OVERRIDES_PATH, _COMMANDS_PATH):
    try:
        _p.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.warning(f"[DEBUG] mkdir failed for {_p.parent}: {e}")

_LOCK_DIR = Path(os.environ.get("NOTIFIER_LOCK_DIR", "") or (Path(os.getenv("TMPDIR", "/tmp")) / "notifier_locks"))
_LOCK_DIR.mkdir(parents=True, exist_ok=True)
if DEBUG_VALUES:
    log.info(f"[DEBUG] Using lock dir: {_LOCK_DIR}")

def _lock_path(path: Path) -> Path:
    return _LOCK_DIR / (Path(path).name + ".lock")

class FileLock:
    def __init__(self, path: Path, timeout: float = 10.0, poll: float = 0.05):
        self.lockfile = _lock_path(path)
        self.timeout = timeout
        self.poll = poll
        self._acq = False
    def acquire(self):
        start = time.time()
        while True:
            try:
                fd = os.open(str(self.lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd); self._acq = True
                if DEBUG_VALUES: log.debug(f"[LOCK] acquired {self.lockfile}")
                return
            except FileExistsError:
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lockfile}")
                time.sleep(self.poll)
    def release(self):
        if self._acq:
            try:
                os.unlink(self.lockfile)
                if DEBUG_VALUES: log.debug(f"[LOCK] released {self.lockfile}")
            except FileNotFoundError:
                pass
            finally:
                self._acq = False
    def __enter__(self): self.acquire(); return self
    def __exit__(self, exc_type, exc, tb): self.release()

def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def _json_load_any(path: Path, fallback: Any) -> Any:
    if not path.exists():
        if DEBUG_VALUES: log.debug(f"[IO] load {path} -> fallback")
        return json.loads(json.dumps(fallback))
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"[IO] read error {path}: {e} -> fallback")
        return json.loads(json.dumps(fallback))

def _json_save_any(path: Path, data: Any) -> None:
    payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    with FileLock(path):
        try:
            if path.exists():
                cur = path.read_bytes()
                if len(cur) == len(payload) and _sha256_bytes(cur) == _sha256_bytes(payload):
                    if DEBUG_VALUES: log.debug(f"[IO] save {path} -> SKIP (unchanged)")
                    return
        except Exception as e:
            log.debug(f"[IO] compare failed for {path}: {e}")
        with open(tmp, "wb") as f:
            f.write(payload); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
        try:
            if hasattr(os, "O_DIRECTORY"):
                dfd = os.open(str(path.parent), os.O_DIRECTORY)
                try: os.fsync(dfd)
                finally: os.close(dfd)
        except Exception:
            pass
    if DEBUG_VALUES: log.debug(f"[IO] save {path} ok")

# ─────────────────────────────────────────────────────────────
# HTTP Session + unified JSON call
# ─────────────────────────────────────────────────────────────
_SESSION: Optional[requests.Session] = None
def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        retry = Retry(
            total=HTTP_RETRIES, read=HTTP_RETRIES, connect=HTTP_RETRIES,
            backoff_factor=0.3, status_forcelist=[429,500,502,503,504],
            allowed_methods=["GET","POST","PUT"], raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
        s.mount("http://", adapter); s.mount("https://", adapter)
        _SESSION = s
    return _SESSION

def _http_json(method: str, url: str, *, params: Dict[str, Any] | None = None,
               json_body: Dict[str, Any] | None = None, timeout: float = HTTP_TIMEOUT) -> Any:
    tries = max(1, HTTP_RETRIES)
    last_err: Optional[Exception] = None
    sess = _get_session()
    for i in range(tries):
        try:
            if DEBUG_HTTP:
                log.debug(f"[HTTP] {method} {url} params={params} json_keys={list((json_body or {}).keys())}")
            r = sess.request(method, url, params=params, json=json_body, timeout=timeout)
            if DEBUG_HTTP: log.debug(f"[HTTP] {r.status_code} {r.url}")
            # treat 400/422 from /indicator gracefully to empty payload
            if url.endswith("/indicator") and r.status_code in (400, 422):
                return {"data": []}
            r.raise_for_status()
            return r.json() if r.text else {}
        except Exception as e:
            last_err = e
            log.warning(f"[HTTP] {method} {url} failed: {e} (try {i+1}/{tries})")
            time.sleep(0.25 * (i + 1))
    raise RuntimeError(f"HTTP {method} failed: {url} :: {last_err}")

# ─────────────────────────────────────────────────────────────
# Operatoren
# ─────────────────────────────────────────────────────────────
def _op_eq(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 1e-9) -> bool:
    try: return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)
    except Exception: return False

def _op_ne(a: float, b: float, rel_tol: float = 1e-6, abs_tol: float = 1e-9) -> bool:
    try: return not math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)
    except Exception: return False

OPS = {
    "eq":  _op_eq,
    "ne":  _op_ne,
    "gt":  lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
    "lt":  lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
}
ALIASES = {"==":"eq","=":"eq","!=":"ne","<>":"ne",">=":"gte","≤":"lte","<=":"lte","≥":"gte"}

def _normalize_op(op: str) -> str:
    return ALIASES.get((op or "").strip().lower(), (op or "").strip().lower())

# ─────────────────────────────────────────────────────────────
# Profiles & Indicators meta
# ─────────────────────────────────────────────────────────────
def _load_profiles() -> List[Dict[str, Any]]:
    data = _http_json("GET", f"{NOTIFIER_ENDPOINT}/profiles")
    if not isinstance(data, list):
        raise RuntimeError("Profiles-Endpoint lieferte kein List-JSON.")
    log.info(f"Profile geladen: {len(data)}")
    return data

def _load_indicators_meta() -> Dict[str, Dict[str, Any]]:
    items = _http_json("GET", f"{PRICE_API_ENDPOINT}/indicators")
    if not isinstance(items, list):
        raise RuntimeError("/indicators lieferte kein List-JSON.")
    meta: Dict[str, Dict[str, Any]] = {}
    for it in items:
        n = (it.get("name") or "").strip()
        if n: meta[n.lower()] = it
    log.info(f"Indikator-Metadaten: {len(meta)} Specs")
    return meta

# ─────────────────────────────────────────────────────────────
# Legacy label parser
# ─────────────────────────────────────────────────────────────
_EMA_RE = re.compile(r"^EMA_(\d+)$", re.IGNORECASE)
_RSI_RE = re.compile(r"^RSI_(\d+)$", re.IGNORECASE)
_MACD_RE = re.compile(r"^MACD_(\d+)_(\d+)_(\d+)$", re.IGNORECASE)

def _legacy_parse_label_if_needed(label: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    s = (label or "").strip()
    if not s: return None
    if (m := _EMA_RE.match(s)):  return "ema",  {"length": int(m.group(1))}
    if (m := _RSI_RE.match(s)):  return "rsi",  {"length": int(m.group(1))}
    if (m := _MACD_RE.match(s)): return "macd", {"fast": int(m.group(1)), "slow": int(m.group(2)), "signal": int(m.group(3))}
    s_low = s.lower()
    if s_low in {"golden_cross", "death_cross", "macd_cross"}:
        return s_low, {}
    return None

# ─────────────────────────────────────────────────────────────
# Spec resolver
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# Indicator fetching + caching
# ─────────────────────────────────────────────────────────────
_INDICATOR_CACHE: "OrderedDict[Tuple[str,str,str,str,str], Dict[str, Any]]" = OrderedDict()

def _indicator_cache_key(name: str, symbol: str, chart_iv: str, ind_iv: str, params: Dict[str, Any]) -> Tuple[str,str,str,str,str]:
    pkey = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
    return (name.lower(), symbol, chart_iv, ind_iv, pkey)

def _cache_get(key: Tuple[str,str,str,str,str]) -> Optional[Dict[str, Any]]:
    val = _INDICATOR_CACHE.get(key)
    if val is not None: _INDICATOR_CACHE.move_to_end(key)
    return val

def _cache_put(key: Tuple[str,str,str,str,str], value: Dict[str, Any]) -> None:
    _INDICATOR_CACHE[key] = value
    _INDICATOR_CACHE.move_to_end(key)
    if len(_INDICATOR_CACHE) > max(8, CACHE_MAX):
        _INDICATOR_CACHE.popitem(last=False)

def _clean_params_for_api(params: Dict[str, Any] | None) -> Dict[str, Any]:
    p = {k: v for k, v in (params or {}).items() if v not in (None, "", [], {})}
    p.pop("symbol", None)
    if not p.get("source"):
        p.pop("source", None)
    return p

def _stable_params_json(params: Dict[str, Any]) -> str:
    return json.dumps(params, separators=(",", ":"), sort_keys=True)

def _effective_intervals(chart_iv: Optional[str], ind_iv: Optional[str]) -> Tuple[str, str]:
    ci = (chart_iv or "").strip(); ii = (ind_iv or "").strip()
    if ci and not ii: ii = ci
    elif ii and not ci: ci = ii
    if not ci and not ii: ci = ii = "1d"
    return ci, ii

def _fetch_indicator_series(name: str, symbol: str, chart_iv: str, ind_iv: str, params: Dict[str, Any]) -> Dict[str, Any]:
    eff_ci, eff_ii = _effective_intervals(chart_iv, ind_iv)
    clean = _clean_params_for_api(params)
    params_json = _stable_params_json(clean)
    key = _indicator_cache_key(name, symbol, eff_ci, eff_ii, clean)
    if (cached := _cache_get(key)) is not None:
        return cached

    query = {
        "name": name, "symbol": symbol,
        "chart_interval": eff_ci, "indicator_interval": eff_ii,
        "params": params_json, "count": 5,
    }
    if DEBUG_VALUES:
        log.debug(f"[FETCH] {name} sym={symbol} chart_iv={eff_ci} ind_iv={eff_ii} params={clean} count=5")

    t0 = time.perf_counter()
    try:
        data = _http_json("GET", f"{CHART_API_ENDPOINT}/indicator", params=query)
    except Exception as e:
        # treat any hard error as empty data; evaluator shows it as error later
        log.warning(f"[HTTP] /indicator hard fail for {name}@{symbol}: {e}")
        data = {"data": []}
    dt = (time.perf_counter() - t0) * 1000
    if DEBUG_VALUES:
        log.debug(f"[FETCH] done {name} in {dt:.1f} ms")

    if not isinstance(data, dict) or "data" not in data:
        raise RuntimeError(f"/indicator lieferte kein dict mit 'data'. name={name}")

    _cache_put(key, data)
    return data

def _is_number(x: Any) -> bool:
    try:
        if isinstance(x, bool): return False
        if isinstance(x, (int, float)): return math.isfinite(float(x))
        return False
    except Exception:
        return False

def _pick_value_from_row(row: Dict[str, Any], preferred_cols: Optional[List[str]] = None) -> Tuple[Optional[float], Optional[str]]:
    if preferred_cols:
        for c in preferred_cols:
            if isinstance(c, str) and c in row and _is_number(row[c]):
                return float(row[c]), c
    for k, v in row.items():
        if k == "Timestamp_ISO": continue
        if _is_number(v): return float(v), k
    return None, None

def _default_output_priority_for(name: str) -> List[str]:
    n = (name or "").lower()
    if n == "ema":  return ["EMA","ema","Ema","value"]
    if n == "rsi":  return ["RSI","rsi","value"]
    if n == "macd": return ["Histogram","hist","MACD","Signal","signal","value"]
    if n == "price":return ["Price","Close","close","value"]
    if n in {"golden_cross","death_cross","macd_cross"}: return ["signal","value"]
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
    if not rows:  # empty = cannot compute
        return None, None, None
    last = rows[-1]

    pref: List[str] = []
    if isinstance(chosen_output, str) and chosen_output: pref.append(chosen_output)
    spec = meta.get(name.lower())
    if spec and isinstance(spec.get("outputs"), list) and spec["outputs"]:
        pref += [str(x) for x in spec["outputs"] if isinstance(x, (str,int,float))]
    pref += _default_output_priority_for(name)

    # dedup
    seen: Set[str] = set(); dedup: List[str] = []
    for x in pref:
        if x not in seen:
            seen.add(x); dedup.append(x)

    ts = last.get("Timestamp_ISO")
    val, col = _pick_value_from_row(last, dedup)
    return val, col, ts

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _resolve_right_side(cond: Dict[str, Any], main_symbol: str, main_interval: str) -> Tuple[str, str]:
    return ((cond.get("right_symbol") or "").strip() or main_symbol,
            (cond.get("right_interval") or "").strip() or main_interval)

def _numeric_or_none(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None
    
def _to_int_or_none(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(str(v).strip())
    except Exception as e:
        log.warning(f"[EVAL] bad telegram_bot_id={v!r} -> using None ({e})")
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")

def _safe_max_iso(ts_list: List[Optional[str]], fallback: Optional[str] = None) -> Optional[str]:
    vals = [t for t in ts_list if isinstance(t, str) and t]
    if not vals: return fallback
    try:
        def _parse(s: str) -> datetime:
            if s.endswith("Z"): s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        return max(vals, key=_parse)
    except Exception:
        try: return max(vals)
        except Exception: return fallback

def _normalize_notify_mode(group: Dict[str, Any]) -> str:
    val = group.get("deactivate_on")
    if val is None:
        return "true" if group.get("auto_deactivate") else "always"
    s = str(val).strip().lower()
    if s in {"always"}: return "always"
    if s in {"true","full","match"}: return "true"
    if s in {"any_true","any","partial"}: return "any_true"
    return "always"

def _min_true_ticks_of(group: Dict[str, Any]) -> Optional[int]:
    try:
        v = group.get("min_true_ticks")
        if v in (None, "", "null"): return None
        i = int(v); return i if i >= 1 else 1
    except Exception:
        return None

def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def _iso_now_dt() -> datetime:
    return datetime.now(timezone.utc)

def _ensure_required_params(meta: Dict[str, Dict[str, Any]], name: Optional[str], p: Dict[str, Any]) -> Dict[str, Any]:
    if not name: return p
    out = dict(p or {})
    spec = meta.get(str(name).lower()) or {}
    req  = spec.get("required_params") or {}
    dfl  = spec.get("default_params") or {}
    if "source" in req and not out.get("source"):
        out["source"] = dfl.get("source") or "Close"
    return out

def _label_only_conditions(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in (group.get("conditions") or []):
        if not isinstance(c, dict): continue
        left  = (c.get("left") or "").strip() or "—"
        right = (c.get("right") or "").strip()
        if not right:
            rsym = (c.get("right_symbol") or "").strip()
            rinv = (c.get("right_interval") or "").strip()
            rout = (c.get("right_output") or "").strip()
            if rsym:
                parts = [rsym]; 
                if rinv: parts.append(f"@{rinv}")
                if rout: parts.append(f":{rout}")
                right = "".join(parts)
        right = right or "—"
        op = (c.get("op") or "gt").strip().lower()
        out.append({
            "left": left, "right": right,
            "left_spec": None, "right_spec": None,
            "left_output": None, "right_output": None,
            "left_col": None, "right_col": None,
            "op": op, "passed": False,
            "left_value": None, "right_value": None,
            "left_ts": None, "right_ts": None,
            "eval_ms": None, "error": None,
        })
    return out

# ─────────────────────────────────────────────────────────────
# Condition Evaluation
# ─────────────────────────────────────────────────────────────
def evaluate_condition_for_symbol(
    meta: Dict[str, Dict[str, Any]],
    cond: Dict[str, Any],
    main_symbol: str,
    main_interval: str
) -> Tuple[bool, Dict[str, Any]]:
    t0 = time.perf_counter()
    op_raw = (cond.get("op") or ""); op = _normalize_op(op_raw)
    if op not in OPS:
        return False, {"error": f"unknown_operator:{op_raw}", "normalized": op}

    # LEFT
    left_label   = cond.get("left") or ""
    left_params  = cond.get("left_params") or {}
    left_output  = (cond.get("left_output") or "").strip() or None
    mode_l, left_name, left_p, left_out = resolve_spec_and_params(left_label, left_params, left_output)
    if mode_l == "invalid":
        return False, {"error": "invalid_left_label", "left": left_label}

    left_val: Optional[float]; left_col: Optional[str]; left_ts: Optional[str]
    if mode_l == "const":
        left_val = float((left_p or {}).get("value")); left_col = "CONST"; left_ts = None
    else:
        try:
            left_p = _ensure_required_params(meta, left_name, left_p)
            left_val, left_col, left_ts = _last_value_for_indicator(
                meta=meta, name=left_name, symbol=main_symbol,
                chart_interval=main_interval, indicator_interval=main_interval,
                params=left_p, chosen_output=left_out,
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

    right_val: Optional[float]; right_col: Optional[str]; right_ts: Optional[str]
    if right_label.strip() == "":
        # Legacy ABS / ABS% mode
        if right_pct_legacy is not None and right_abs_legacy is None:
            return False, {"error": "right_change_without_base", "hint": "right_absolut erforderlich oder right_label='change' verwenden"}
        base = right_abs_legacy if right_abs_legacy is not None else 0.0
        if right_pct_legacy is not None:
            right_val = base * (1.0 + (right_pct_legacy / 100.0)); right_col = "ABS% (legacy)"
        else:
            right_val = base; right_col = "ABS (legacy)"
        right_ts = None
    else:
        mode_r, right_name, right_p, right_out = resolve_spec_and_params(right_label, right_params, right_output)
        if mode_r == "invalid":
            return False, {"error": "invalid_right_label", "right": right_label}
        if mode_r == "const":
            right_val = float((right_p or {}).get("value")); right_col = "CONST"; right_ts = None
        else:
            try:
                right_p = _ensure_required_params(meta, right_name, right_p)
                right_val, right_col, right_ts = _last_value_for_indicator(
                    meta=meta, name=right_name, symbol=r_symbol,
                    chart_interval=main_interval, indicator_interval=r_interval,
                    params=right_p, chosen_output=right_out,
                )
            except Exception as e:
                return False, {"error": "right_indicator_fetch_failed", "exception": str(e), "right": right_label}
            if right_val is None:
                return False, {"error": "right_value_none"}
        if (right_label or "").strip().lower() != "change" and right_pct_legacy is not None and right_val is not None:
            right_val = right_val * (1.0 + (right_pct_legacy / 100.0))

    # Compare
    try:
        result = bool(OPS[op](float(left_val), float(right_val)))  # type: ignore[arg-type]
    except Exception as e:
        return False, {"error": "operator_error", "exception": str(e)}

    dt = (time.perf_counter() - t0) * 1000.0
    details = {
        "left": {
            "label": left_label, "spec": left_name if mode_l == "api" else None,
            "output": left_out, "col": left_col, "value": left_val,
            "symbol": main_symbol if mode_l == "api" else None,
            "interval": main_interval if mode_l == "api" else None,
            "ts": left_ts, "params": left_p,
        },
        "right": {
            "label": right_label, "spec": right_name if right_label and mode_r == "api" else None,
            "output": right_out if right_label else None, "col": right_col, "value": right_val,
            "symbol": r_symbol if right_label and mode_r == "api" else None,
            "interval": r_interval if right_label and mode_r == "api" else None,
            "ts": right_ts, "right_absolut": right_abs_legacy, "right_change_legacy_pct": right_pct_legacy,
            "params": right_p if right_label else {},
        },
        "op": (cond.get("op") or "").lower(),
        "op_norm": _normalize_op(cond.get("op") or ""),
        "result": result, "duration_ms": round(dt, 2),
    }
    if DEBUG_VALUES:
        log.debug(f"[EVAL] {main_symbol}@{main_interval} {left_label} {details['op_norm']} {right_label} -> {result} ({dt:.1f} ms)")
    return result, details

# ─────────────────────────────────────────────────────────────
# Group/Profile Evaluation
# ─────────────────────────────────────────────────────────────
def _eval_group_for_symbol(
    meta: Dict[str, Dict[str, Any]],
    profile: Dict[str, Any],
    group: Dict[str, Any],
    symbol: str,
    group_index: int
) -> Tuple[str, List[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Returns:
      status ∈ {"FULL","PARTIAL","NONE"}
      cond_details
      bar_ts (max Timestamp_ISO)
      error_str (falls harter Fehler)
    """
    t0 = time.perf_counter()
    conditions: List[Dict[str, Any]] = group.get("conditions") or []
    main_interval = (group.get("interval") or "").strip()
    if not main_interval:
        return "NONE", [], None, "missing_interval"

    group_result: Optional[bool] = None
    any_true = False
    per_details: List[Dict[str, Any]] = []
    hard_error: Optional[str] = None

    for idx, cond in enumerate(conditions):
        try:
            res, details = evaluate_condition_for_symbol(meta, cond, symbol, main_interval)
        except Exception as e:
            res, details = False, {"error": "eval_exception", "exception": str(e)}
        details["idx"] = idx
        details["rid"] = (cond.get("rid") or None)

        per_details.append(details)

        if details.get("error"): hard_error = details.get("error")
        any_true = any_true or bool(res)
        group_result = res if group_result is None else ((group_result or res) if (cond.get("logic") or "and").strip().lower() == "or" else (group_result and res))

    status = "FULL" if bool(group_result) else ("PARTIAL" if any_true else "NONE")

    ts_candidates: List[Optional[str]] = []
    for d in per_details:
        try:
            lts = (d.get("left") or {}).get("ts"); rts = (d.get("right") or {}).get("ts")
            if lts: ts_candidates.append(lts)
            if rts: ts_candidates.append(rts)
        except Exception:
            pass
    bar_ts = _safe_max_iso(ts_candidates, fallback=None)

    dt = (time.perf_counter() - t0) * 1000.0
    if DEBUG_VALUES:
        log.debug(f"[GROUP] {profile.get('name')}[{group_index}] {symbol}@{main_interval} -> {status} (conds={len(conditions)}, {dt:.1f} ms)")
    return status, per_details, bar_ts, hard_error

# ─────────────────────────────────────────────────────────────
# Status/Commands/Overrides
# ─────────────────────────────────────────────────────────────
_STATUS_TEMPLATE: Dict[str, Any] = {"version": 0, "updated_ts": None, "profiles": {}}
_OVR_TEMPLATE: Dict[str, Any]    = {"overrides": {}, "updated_ts": None}
_CMD_TEMPLATE: Dict[str, Any]    = {"queue": []}

def _load_status() -> Dict[str, Any]:   return _json_load_any(_STATUS_PATH, _STATUS_TEMPLATE)
def _save_status(st: Dict[str, Any]) -> None: st["updated_ts"] = _now_iso(); _json_save_any(_STATUS_PATH, st)

def _load_overrides() -> Dict[str, Any]:
    d = _json_load_any(_OVERRIDES_PATH, _OVR_TEMPLATE)
    return d if isinstance(d, dict) and "overrides" in d else {"overrides": {}, "updated_ts": None}

def _ensure_ovr_slot(ovr: Dict[str, Any], pid: str, gid: str) -> Dict[str, Any]:
    ovr.setdefault("overrides", {}); ovr["overrides"].setdefault(pid, {})
    ovr["overrides"][pid].setdefault(gid, {"forced_off": False, "snooze_until": None, "note": None})
    return ovr["overrides"][pid][gid]

def _load_commands() -> Dict[str, Any]:
    d = _json_load_any(_COMMANDS_PATH, _CMD_TEMPLATE)
    return d if isinstance(d, dict) and "queue" in d else {"queue": []}

def _save_commands(d: Dict[str, Any]) -> None: _json_save_any(_COMMANDS_PATH, d)

def _prune_status_and_overrides(
    status: Dict[str, Any],
    overrides: Dict[str, Any],
    profiles: List[Dict[str, Any]],
) -> Tuple[bool, bool]:
    """
    Entfernt nicht mehr existierende Profile/Gruppen aus status & overrides.
    Rückgabe: (status_changed, overrides_changed)
    """
    # Soll-Zustand aus aktuellen Profiles aufbauen
    wanted_pids: Set[str] = set()
    wanted_pg: Set[Tuple[str, str]] = set()
    for p in profiles or []:
        pid = str(p.get("id") or "").strip()
        if not pid:
            continue
        wanted_pids.add(pid)
        for g in (p.get("condition_groups") or []):
            gid = str(g.get("gid") or "").strip()
            if not gid:
                # Fallback: index-based gid, aber UI/API sollte gid liefern
                # Trotzdem defensiv: überspringen statt falsche Keys zu erzeugen
                continue
            wanted_pg.add((pid, gid))

    # --- Status prune ---
    st_changed = False
    status.setdefault("profiles", {})
    cur_profiles = status["profiles"]

    # Profile löschen, die es nicht mehr gibt
    for pid in list(cur_profiles.keys()):
        if pid not in wanted_pids:
            if DEBUG_VALUES:
                log.debug(f"[PRUNE][status] drop profile pid={pid} (no longer in /profiles)")
            del cur_profiles[pid]
            st_changed = True
            continue
        # Gruppen in bestehenden Profilen prunen
        gmap = cur_profiles[pid].setdefault("groups", {})
        for gid in list(gmap.keys()):
            if (pid, gid) not in wanted_pg:
                if DEBUG_VALUES:
                    log.debug(f"[PRUNE][status] drop group pid={pid} gid={gid} (no longer in /profiles)")
                del gmap[gid]
                st_changed = True

    # --- Overrides prune ---
    ovr_changed = False
    overrides.setdefault("overrides", {})
    ovr_profiles = overrides["overrides"]

    for pid in list(ovr_profiles.keys()):
        if pid not in wanted_pids:
            if DEBUG_VALUES:
                log.debug(f"[PRUNE][ovr] drop profile pid={pid} (no longer in /profiles)")
            del ovr_profiles[pid]
            ovr_changed = True
            continue
        gmap = ovr_profiles[pid]
        for gid in list(gmap.keys()):
            if (pid, gid) not in wanted_pg:
                if DEBUG_VALUES:
                    log.debug(f"[PRUNE][ovr] drop group pid={pid} gid={gid} (no longer in /profiles)")
                del gmap[gid]
                ovr_changed = True

    return st_changed, ovr_changed


def _skeleton_group_from_def(group: Dict[str, Any], g_idx: int) -> Dict[str, Any]:
    name = group.get("name") or f"group_{g_idx}"
    interval = (group.get("interval") or "").strip()
    symbols = list(dict.fromkeys([s for s in (group.get("symbols") or []) if s]))
    misconfigured = (not interval) or (not symbols)
    notify_mode = _normalize_notify_mode(group)
    min_ticks   = _min_true_ticks_of(group) or 1
    return {
        "group_active": bool(group.get("active", True)),
        "last_eval_ts": None,
        "effective_active": False,
        "blockers": (["misconfigured"] if misconfigured else []),
        "auto_disabled": False,
        "cooldown_until": None,
        "fresh": True,
        "name": name,
        "aggregate": {
            "logic": "AND",
            "passed": False,
            "notify_mode": notify_mode,
            "min_true_ticks": min_ticks
        },
        "conditions": _label_only_conditions(group),
        # 👉 details als leeres Array vorhanden, damit UI nie auf Fallback muss
        "runtime": {"met": 0, "total": len(group.get("conditions") or []), "true_ticks": None, "details": []},
    }


def sync_status_from_profiles(profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
    st = _load_status()
    st.setdefault("profiles", {})

    # Vor dem Auffüllen: prunen
    dummy_overrides = _load_overrides()
    _prune_status_and_overrides(st, dummy_overrides, profiles)  # overrides hier nur zum konsistenten Verhalten
    cur_profiles = st["profiles"]

    # Jetzt (re)anlegen/auffüllen
    for p in profiles or []:
        pid = str(p.get("id") or "").strip()
        if not pid:
            continue
        pobj = cur_profiles.setdefault(pid, {})
        pobj["profile_active"] = bool(p.get("enabled", True))
        pobj["id"] = pid
        pobj["name"] = p.get("name") or ""
        gmap = pobj.setdefault("groups", {})
        for g_idx, g in enumerate(p.get("condition_groups") or []):
            gid = str(g.get("gid") or "").strip() or f"g{g_idx}"
            gmap[gid] = {**gmap.get(gid, {}), **_skeleton_group_from_def(g, g_idx)}

    st["version"] = int(st.get("version", 0)) + 1
    _save_status(st)
    return st


# ─────────────────────────────────────────────────────────────
# Top-Level: run_check
# ─────────────────────────────────────────────────────────────
def run_check() -> List[Dict[str, Any]]:
    """
    Evaluator ohne Auto-Deaktivierung.
    - Ermittelt je Gruppe+Symbol den Status: FULL / PARTIAL / NONE
    - Baut EVAL-Events (inkl. deactivate_on, min_true_ticks)
    - Übergibt an gate_and_build_triggers (Streak-Gate pro Modus)
    - Schreibt Status & konsumiert Commands
    """
    _INDICATOR_CACHE.clear()

    status = _load_status()
    overrides = _load_overrides()
    commands = _load_commands()

    # Commands indexieren
    cmds_by_pg: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for it in list(commands.get("queue") or []):
        pid = str(it.get("profile_id") or ""); gid = str(it.get("group_id") or "")
        if pid and gid: cmds_by_pg.setdefault((pid, gid), []).append(it)

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

    # 🔧 PRUNE: Entferne alte Profile/Gruppen aus Status & Overrides
    st_changed, ovr_changed = _prune_status_and_overrides(status, overrides, profiles)
    if st_changed:
        status["version"] = int(status.get("version", 0)) + 1
        _save_status(status)
        if DEBUG_VALUES:
            log.debug("[PRUNE] status pruned & saved")
    if ovr_changed:
        overrides["updated_ts"] = _now_iso()
        _json_save_any(_OVERRIDES_PATH, overrides)
        if DEBUG_VALUES:
            log.debug("[PRUNE] overrides pruned & saved")


    evals: List[Dict[str, Any]] = []
    now_iso = _now_iso(); now_dt = _iso_now_dt()

    status.setdefault("profiles", {})
    prev_version = int(status.get("version", 0))
    cur_profiles = status["profiles"]

    consumed_cmd_ids: Set[str] = set()

    for p_idx, profile in enumerate(profiles):
        pid = str(profile.get("id") or "")
        if not pid: continue

        prof_st = cur_profiles.setdefault(pid, {})
        prof_st["profile_active"] = bool(profile.get("enabled", True))
        prof_st["id"] = pid; prof_st["name"] = profile.get("name") or ""
        gmap = prof_st.setdefault("groups", {})

        if not profile.get("enabled", True):
            if DEBUG_VALUES: log.debug(f"[ACTIVE] skip profile pid={pid} (enabled=0)")
            continue

        for g_idx, group in enumerate(profile.get("condition_groups") or []):
            gid = str(group.get("gid") or f"g{g_idx}") or f"g{g_idx}"
            grp_st = gmap.setdefault(gid, {})
            grp_st["group_active"] = bool(group.get("active", True))
            grp_st["last_eval_ts"] = now_iso

            blockers: List[str] = []
            auto_disabled = bool(grp_st.get("auto_disabled", False))
            cooldown_until_iso = grp_st.get("cooldown_until")
            cooldown_until_dt  = _parse_iso(cooldown_until_iso)
            fresh = True
            hard_error: Optional[str] = None

            # Overrides
            ov_slot = _ensure_ovr_slot(overrides, pid, gid)
            forced_off = bool(ov_slot.get("forced_off", False))
            snooze_until_dt = _parse_iso(ov_slot.get("snooze_until"))

            # Commands anwenden
            for cmd in cmds_by_pg.get((pid, gid), []):
                cmd_id = str(cmd.get("id") or "")
                if cmd_id in consumed_cmd_ids: continue
                rearm = bool(cmd.get("rearm", True)); rebaseline = bool(cmd.get("rebaseline", False))
                if rearm:
                    auto_disabled = False
                    cooldown_until_dt = now_dt
                    cooldown_until_iso = now_iso
                    if DEBUG_VALUES: log.debug(f"[COMMAND] REARM pid={pid} gid={gid}")
                if rebaseline and DEBUG_VALUES:
                    log.debug(f"[COMMAND] REBASELINE pid={pid} gid={gid} (handled down the chain)")
                consumed_cmd_ids.add(cmd_id)

            # Inaktive Gruppe?
            if not group.get("active", True): blockers.append("group_inactive")
            if forced_off: blockers.append("forced_off")
            if snooze_until_dt and now_dt < snooze_until_dt: blockers.append("snooze")
            if auto_disabled: blockers.append("auto_disabled")
            if cooldown_until_dt and now_dt < cooldown_until_dt: blockers.append("cooldown")

            symbols = list(dict.fromkeys([s for s in (group.get("symbols") or []) if s]))
            main_interval = (group.get("interval") or "").strip()
            if (not symbols) or (not main_interval):
                blockers.append("misconfigured")
                grp_st.update({
                    "name": (group.get("name") or f"group_{g_idx}"),
                    "effective_active": False, "blockers": blockers,
                    "auto_disabled": auto_disabled, "cooldown_until": cooldown_until_iso,
                    "fresh": fresh,
                    "aggregate": {
                        "logic": "AND", "passed": False,
                        "notify_mode": _normalize_notify_mode(group),
                        "min_true_ticks": _min_true_ticks_of(group) or 1,
                    },
                    "conditions": _label_only_conditions(group),
                    "runtime": {"met": 0, "total": len(group.get("conditions") or []), "true_ticks": None},
                })
                continue

            effective_active = bool(profile.get("enabled", True)) \
                               and bool(group.get("active", True)) \
                               and (not forced_off) \
                               and (not (snooze_until_dt and now_dt < snooze_until_dt)) \
                               and (not auto_disabled) \
                               and (not (cooldown_until_dt and now_dt < cooldown_until_dt))

            notify_mode = _normalize_notify_mode(group)
            min_ticks = _min_true_ticks_of(group)
            per_symbol_evals: List[Dict[str, Any]] = []
            ts_list: List[Optional[str]] = []

            if effective_active:
                for sym in symbols:
                    status_str, cond_details, bar_ts, err = _eval_group_for_symbol(meta, profile, group, sym, g_idx)
                    if err:
                        hard_error = err

                    # 🔧 NEU: single_mode + tick_id + Vereinheitlichung auf notify_mode
                    single_mode = (group.get("single_mode") or "symbol").strip().lower()  # "symbol" | "group" | "everything"
                    tick_id = f"{main_interval}:{(bar_ts or now_iso)}"

                    ev = {
                        "profile_id": pid, "profile_name": profile.get("name"),
                        "group_id": gid, "group_index": g_idx, "group_name": group.get("name") or f"group_{g_idx}",
                        "symbol": sym, "interval": main_interval, "exchange": group.get("exchange") or None,
                        "telegram_bot_id": _to_int_or_none(group.get("telegram_bot_id")),
                        "telegram_bot_token": group.get("telegram_bot_token") or None,
                        "telegram_chat_id": group.get("telegram_chat_id") or None,

                        "description": group.get("description") or None,
                        "ts": now_iso, "bar_ts": bar_ts or now_iso, "tick_id": tick_id,
                        "status": status_str, "notify_mode": notify_mode, "min_true_ticks": min_ticks,
                        "single_mode": single_mode,
                        "conditions": cond_details,
                    }

                    # ✅ WICHTIG: Events sammeln (du hattest das vergessen)
                    per_symbol_evals.append(ev)
                    evals.append(ev)
                    if bar_ts:
                        ts_list.append(bar_ts)

                    if DEBUG_VALUES:
                        log.debug(
                            f"[EVAL-EVENT] pid={pid} gid={gid} sym={sym} "
                            f"status={status_str} mode={notify_mode} single={single_mode} tick={tick_id}"
                        )

            else:
                if DEBUG_VALUES: log.debug(f"[ACTIVE] skip evaluation pid={pid} gid={gid} due blockers={blockers}")
                grp_st["conditions"] = _label_only_conditions(group)

            if hard_error:
                blockers.append("error"); fresh = False

            effective_active = effective_active and fresh and not any(b in ("forced_off","snooze","auto_disabled","cooldown","group_inactive","misconfigured") for b in blockers)

            met = total = 0
            if per_symbol_evals:
                try:
                    sample_conds = per_symbol_evals[0].get("conditions") or []
                    total = len(sample_conds); met = sum(1 for c in sample_conds if c and bool(c.get("result")))
                except Exception:
                    pass
            else:
                total = len(group.get("conditions") or []); met = 0

            # 👉 Bedingungen + Details aus erstem Symbol (repräsentativ) ableiten
            conditions_list: List[Dict[str, Any]] = []
            details_list: List[Dict[str, Any]] = []
            if per_symbol_evals:
                try:
                    sample = per_symbol_evals[0]
                    for cd in (sample.get("conditions") or []):
                        left  = cd.get("left")  if isinstance(cd.get("left"), dict)  else {}
                        right = cd.get("right") if isinstance(cd.get("right"), dict) else {}

                        # UI-freundliche Kurzform (conditions) – wie bisher
                        conditions_list.append({
                            "left": left.get("label"),
                            "right": right.get("label"),
                            "left_spec": left.get("spec"),
                            "right_spec": right.get("spec"),
                            "left_output": left.get("output"),
                            "right_output": right.get("output"),
                            "left_col": left.get("col"),
                            "right_col": right.get("col"),
                            "op": cd.get("op_norm") or cd.get("op"),
                            "passed": bool(cd.get("result")),
                            "left_value": left.get("value"),
                            "right_value": right.get("value"),
                            "left_ts": left.get("ts"),
                            "right_ts": right.get("ts"),
                            "eval_ms": cd.get("duration_ms"),
                            "error": cd.get("error"),
                        })

                        # 🔥 Vollständige Details (runtime.details) – damit UI echte Werte hat
                        details_list.append({
                            "rid": cd.get("rid") or None,
                            "op": cd.get("op_norm") or cd.get("op"),
                            "result": bool(cd.get("result")),
                            "left": {
                                "label":  left.get("label"),
                                "spec":   left.get("spec"),
                                "output": left.get("output"),
                                "col":    left.get("col"),
                                "value":  left.get("value"),
                                "symbol": left.get("symbol"),
                                "interval": left.get("interval"),
                                "ts":     left.get("ts"),
                                "params": left.get("params") or {},
                            },
                            "right": {
                                "label":  right.get("label"),
                                "spec":   right.get("spec"),
                                "output": right.get("output"),
                                "col":    right.get("col"),
                                "value":  right.get("value"),
                                "symbol": right.get("symbol"),
                                "interval": right.get("interval"),
                                "ts":     right.get("ts"),
                                "params": right.get("params") or {},
                                "right_absolut": right.get("right_absolut"),
                                "right_change_legacy_pct": right.get("right_change_legacy_pct"),
                            },
                            "duration_ms": cd.get("duration_ms"),
                            "error": cd.get("error"),
                        })
                except Exception as e:
                    log.debug(f"[STATUS] details build failed pid={pid} gid={gid}: {e}")

            # Vorhandene true_ticks erhalten (Gate setzt die später evtl. noch)
            prev_true_ticks = None
            try:
                prev_true_ticks = (grp_st.get("runtime") or {}).get("true_ticks")
            except Exception:
                prev_true_ticks = None

            grp_st.update({
                "name": (group.get("name") or f"group_{g_idx}"),
                "effective_active": bool(effective_active),
                "blockers": blockers,
                "auto_disabled": bool(auto_disabled),
                "cooldown_until": cooldown_until_iso,
                "fresh": bool(fresh),
                "last_eval_ts": now_iso,
                "last_bar_ts": _safe_max_iso(ts_list, None),
                "aggregate": {
                    "logic": "AND",
                    "passed": any(
                        (ev["status"] == "FULL")
                        or (notify_mode == "any_true" and ev["status"] in ("FULL","PARTIAL"))
                        for ev in per_symbol_evals
                    ),
                    "notify_mode": notify_mode,
                    "min_true_ticks": (min_ticks if min_ticks is not None else 1),
                },
                "conditions": conditions_list if conditions_list else _label_only_conditions(group),
                "runtime": {
                    "met": met,
                    "total": total,
                    "true_ticks": prev_true_ticks,
                    "details": details_list,  # 👉 das will die UI sehen
                },
            })

            if DEBUG_VALUES:
                log.debug(f"[STATUS] pid={pid} gid={gid} conds={len(conditions_list)} details={len(details_list)}")

    triggered = gate_and_build_triggers(evals)

    if cmds := {str(it.get("id") or "") for it in (commands.get("queue") or [])}:
        if consumed_cmd_ids:
            commands["queue"] = [it for it in (commands.get("queue") or []) if str(it.get("id") or "") not in consumed_cmd_ids]
            _save_commands(commands)
            if DEBUG_VALUES: log.debug(f"[COMMAND] consumed={len(consumed_cmd_ids)} left={len(commands['queue'])}")

    status["version"] = int(prev_version) + 1
    _save_status(status)

    dt_total = (time.perf_counter() - t_start) * 1000.0
    log.info(f"Evals={len(evals)} → Trigger={len(triggered)} — Status v{status['version']} geschrieben — Laufzeit: {dt_total:.1f} ms")
    if triggered and DEBUG_VALUES:
        try: log.debug(json.dumps(triggered[:2], ensure_ascii=False, indent=2))
        except Exception: pass
    return triggered

# ─────────────────────────────────────────────────────────────
# CLI Helper
# ─────────────────────────────────────────────────────────────
def run_evaluator() -> None:
    print("🔄 Evaluator startet …")
    try:
        res = run_check()
    except Exception as e:
        print(f"💥 Fatal: {e}")
        return
    print(f"✅ {len(res)} Trigger(s) generiert.")
    if DEBUG_VALUES and res:
        try:
            print(json.dumps(res[:3], indent=2, ensure_ascii=False))
        except Exception:
            pass
