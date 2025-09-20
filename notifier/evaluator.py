# notifier/evaluator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import logging
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Set
import hashlib
from pathlib import Path
from collections import OrderedDict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Gate: baut Trigger inkl. Streak/Gating
from .gate import gate_and_build_triggers

# ─────────────────────────────────────────────────────────────
# Persistenz/Endpoints aus config (mit robusten Fallbacks)
# ─────────────────────────────────────────────────────────────
from config import (
    NOTIFIER_ENDPOINT,      # z.B. http://127.0.0.1:8099/notifier
    CHART_API_ENDPOINT,     # z.B. http://127.0.0.1:7004
)

# Optional verfügbare Pfade/Konfigurationen
try:
    from config import PROFILES_NOTIFIER  # Basis, um Sibling-Files abzuleiten
except Exception:
    PROFILES_NOTIFIER = None  # type: ignore[assignment]

try:
    from config import STATUS_NOTIFIER     # expliziter Status-Pfad
except Exception:
    STATUS_NOTIFIER = None  # type: ignore[assignment]
try:
    from config import OVERRIDES_NOTIFIER  # expliziter Overrides-Pfad
except Exception:
    OVERRIDES_NOTIFIER = None  # type: ignore[assignment]
try:
    from config import COMMANDS_NOTIFIER   # expliziter Commands-Pfad
except Exception:
    COMMANDS_NOTIFIER = None  # type: ignore[assignment]

# Unified-Konfiguration (optional): dient nur als weiterer Commands-Bridge/Fallback
try:
    from config import NOTIFIER_UNIFIED
except Exception:
    NOTIFIER_UNIFIED = None  # type: ignore[assignment]

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

# NEW: Default für min_tick (Bars-Bestätigung) pro Gruppe
DEFAULT_MIN_TICK = _int_env("EVAL_GROUP_MIN_TICK", 1)

# NEW: konfigurierbare Gleichheitstoleranzen für eq/ne
REL_TOL = float(os.getenv("EVAL_REL_TOL", "1e-6"))
ABS_TOL = float(os.getenv("EVAL_ABS_TOL", "1e-9"))

# NEW: /indicator Abrufgröße
FETCH_COUNT = _int_env("EVAL_FETCH_COUNT", 5)

# ─────────────────────────────────────────────────────────────
# Pfade & Locks (mit sinnvollen Fallbacks)
# ─────────────────────────────────────────────────────────────
def _to_path(p: Any | None) -> Optional[Path]:
    if p is None: return None
    return p if isinstance(p, Path) else Path(str(p)).expanduser().resolve()

# Bestmögliche Basis für Siblings bestimmen
def _base_dir() -> Path:
    if _to_path(STATUS_NOTIFIER):
        return _to_path(STATUS_NOTIFIER).parent  # type: ignore[return-value]
    if _to_path(OVERRIDES_NOTIFIER):
        return _to_path(OVERRIDES_NOTIFIER).parent  # type: ignore[return-value]
    if _to_path(COMMANDS_NOTIFIER):
        return _to_path(COMMANDS_NOTIFIER).parent  # type: ignore[return-value]
    if _to_path(NOTIFIER_UNIFIED):
        return _to_path(NOTIFIER_UNIFIED).parent  # type: ignore[return-value]
    if _to_path(PROFILES_NOTIFIER):
        return _to_path(PROFILES_NOTIFIER).parent  # type: ignore[return-value]
    return Path(os.getcwd())

_BASE_DIR = _base_dir()

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
    # Kollisionssicher: Hash über den kanonischen Pfad (bytes!), basename egal
    p = str(Path(path).expanduser().resolve())
    h = hashlib.sha256(p.encode("utf-8")).hexdigest()
    return _LOCK_DIR / f"{h[:16]}.lock"

class FileLock:
    def __init__(self, path: Path, timeout: float = 10.0, poll: float = 0.05, stale_after: float = 300.0):
        self.lockfile = _lock_path(path)
        self.timeout = timeout
        self.poll = poll
        self.stale_after = stale_after
        self._acq = False
    def _is_stale(self) -> bool:
        try:
            st = self.lockfile.stat()
            return (time.time() - st.st_mtime) > self.stale_after
        except FileNotFoundError:
            return False
    def acquire(self):
        start = time.time()
        while True:
            try:
                fd = os.open(str(self.lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd); self._acq = True
                if DEBUG_VALUES: log.debug(f"[LOCK] acquired {self.lockfile}")
                return
            except FileExistsError:
                if self._is_stale():
                    try:
                        os.unlink(self.lockfile)
                        log.warning(f"[LOCK] stale removed {self.lockfile}")
                    except FileNotFoundError:
                        pass
                    continue
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

# ─────────────────────────────────────────────────────────────
# JSON IO (atomar, write-on-change via lock)
# ─────────────────────────────────────────────────────────────
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
            if DEBUG_VALUES: log.debug(f"[IO] compare failed for {path}: {e}")
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
# HTTP Session + robustes JSON
# ─────────────────────────────────────────────────────────────
_SESSION: Optional[requests.Session] = None
def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        retry = Retry(
            total=HTTP_RETRIES,
            read=HTTP_RETRIES,
            connect=HTTP_RETRIES,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset({"GET","POST","PUT","PATCH","DELETE"}),
            raise_on_status=False,
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
            # treat /indicator 400/422 gracefully to empty payload
            if url.endswith("/indicator") and r.status_code in (400, 422):
                return {"data": []}
            r.raise_for_status()
            if not r.text:
                return {}
            try:
                return r.json()
            except ValueError:
                if url.endswith("/indicator"):
                    return {"data": []}
                return {}
        except Exception as e:
            last_err = e
            log.warning(f"[HTTP] {method} {url} failed: {e} (try {i+1}/{tries})")
            time.sleep(0.25 * (i + 1))
    raise RuntimeError(f"HTTP {method} failed: {url} :: {last_err}")

# ─────────────────────────────────────────────────────────────
# Operatoren
# ─────────────────────────────────────────────────────────────
def _op_eq(a: float, b: float, rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL) -> bool:
    try:
        return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)
    except Exception:
        return False

def _op_ne(a: float, b: float, rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL) -> bool:
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
    # korrekt: aus CHART_API_ENDPOINT/indicators
    items = _http_json("GET", f"{CHART_API_ENDPOINT}/indicators")
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
import re as _re
_EMA_RE = _re.compile(r"^EMA_(\d+)$", _re.IGNORECASE)
_RSI_RE = _re.compile(r"^RSI_(\d+)$", _re.IGNORECASE)
_MACD_RE = _re.compile(r"^MACD_(\d+)_(\d+)_(\d+)$", _re.IGNORECASE)

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
_CACHE_HIT = 0
_CACHE_MISS = 0

def _indicator_cache_key(name: str, symbol: str, chart_iv: str, ind_iv: str, params: Dict[str, Any]) -> Tuple[str,str,str,str,str]:
    pkey = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
    return (name.lower(), symbol, chart_iv, ind_iv, pkey)

def _cache_get(key: Tuple[str,str,str,str,str]) -> Optional[Dict[str, Any]]:
    global _CACHE_HIT, _CACHE_MISS
    val = _INDICATOR_CACHE.get(key)
    if val is not None:
        _CACHE_HIT += 1
        _INDICATOR_CACHE.move_to_end(key)
        return val
    _CACHE_MISS += 1
    return None

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
    key = _indicator_cache_key(name, symbol, eff_ci, eff_ii, clean)
    if (cached := _cache_get(key)) is not None:
        return cached

    query = {
        "name": name, "symbol": symbol,
        "chart_interval": eff_ci, "indicator_interval": eff_ii,
        "params": _stable_params_json(clean), "count": max(2, FETCH_COUNT),
    }
    if DEBUG_VALUES:
        log.debug(f"[FETCH] {name} sym={symbol} chart_iv={eff_ci} ind_iv={eff_ii} params={clean} count={query['count']}")
        print(f"[DBG] fetch name={name} sym={symbol} ci={eff_ci} ii={eff_ii}")

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
def _profiles_fingerprint(profiles: List[Dict[str, Any]]) -> str:
    """
    Stabile Fingerprint-Funktion analog zur API-Logik:
    - JSON dump mit sort_keys=True
    - sha256 über UTF-8 Bytes
    """
    try:
        payload = json.dumps(profiles or [], sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    except Exception:
        return ""

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

def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    """Akzeptiert ISO-Strings (mit/ohne Z) sowie Unix-Epoch in Sekunden oder Millisekunden."""
    if s is None or s == "":
        return None
    try:
        # numeric epoch? accept seconds or ms
        if isinstance(s, (int, float)) or (isinstance(s, str) and str(s).isdigit()):
            v = int(s)
            if v > 10_000_000_000:  # ms
                v = v // 1000
            return datetime.fromtimestamp(v, tz=timezone.utc)
        # string ISO with optional Z
        s2 = str(s)
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        return datetime.fromisoformat(s2).astimezone(timezone.utc)
    except Exception:
        return None

def _safe_max_iso(ts_list: List[Optional[str]], fallback: Optional[str] = None) -> Optional[str]:
    vals = [t for t in ts_list if t not in (None, "")]
    if not vals:
        return fallback
    try:
        def _norm_to_dt(x: Any) -> datetime:
            dt = _parse_iso(x)
            return dt if dt else datetime.min.replace(tzinfo=timezone.utc)
        best = max(vals, key=_norm_to_dt)
        return str(best)
    except Exception:
        try:
            return max(map(str, vals))
        except Exception:
            return fallback

def _iso_now_dt() -> datetime:
    return datetime.now(timezone.utc)

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
                parts = [rsym]
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

# NEW: min_tick und Bar-Close-Heuristik
def _group_min_tick(group: Dict[str, Any]) -> int:
    try:
        raw = group.get("min_tick", DEFAULT_MIN_TICK)
        if raw in (None, "", "null"):
            return DEFAULT_MIN_TICK
        iv = int(raw)
        return iv if iv >= 1 else 1
    except Exception:
        return max(1, DEFAULT_MIN_TICK)

def _bar_close_info(bar_ts: Optional[str], interval: str, now_dt: datetime) -> Tuple[Optional[datetime], Optional[bool]]:
    """
    Sehr einfache Heuristik: wenn eine Timestamp vorhanden ist, betrachten wir die letzte Bar als "geschlossen".
    Gating/Streak (gate.py) kümmert sich um echte Bestätigungen über mehrere Ticks.
    """
    if not bar_ts:
        return None, None
    try:
        s = str(bar_ts)
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        bar_dt = datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None, None
    return bar_dt, True

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
        return False, {"error": f"unknown_operator:{op_raw}", "normalized": op, "rid": cond.get("rid")}

    # LEFT
    left_label   = cond.get("left") or ""
    left_params  = cond.get("left_params") or {}
    left_output  = (cond.get("left_output") or "").strip() or None
    mode_l, left_name, left_p, left_out = resolve_spec_and_params(left_label, left_params, left_output)
    if mode_l == "invalid":
        return False, {"error": "invalid_left_label", "left": left_label, "rid": cond.get("rid")}

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
            return False, {"error": "left_indicator_fetch_failed", "exception": str(e), "left": left_label, "rid": cond.get("rid")}
    if left_val is None:
        return False, {"error": "left_value_none", "rid": cond.get("rid")}

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
            return False, {
                "error": "right_change_without_base",
                "hint": "right_absolut erforderlich oder right_label='change' verwenden",
                "rid": cond.get("rid"),
            }
        base = right_abs_legacy if right_abs_legacy is not None else 0.0
        if right_pct_legacy is not None:
            right_val = base * (1.0 + (right_pct_legacy / 100.0)); right_col = "ABS% (legacy)"
        else:
            right_val = base; right_col = "ABS (legacy)"
        right_ts = None
    else:
        mode_r, right_name, right_p, right_out = resolve_spec_and_params(right_label, right_params, right_output)
        if mode_r == "invalid":
            return False, {"error": "invalid_right_label", "right": right_label, "rid": cond.get("rid")}
        # Guard: nicht beides kombinieren
        if (right_label or "").strip().lower() == "change" and _numeric_or_none(cond.get("right_change")) is not None:
            return False, {"error": "mixed_change_modes", "hint": "Entweder right='change' (mit baseline/delta) ODER legacy right_absolut/right_change, nicht beides.", "rid": cond.get("rid")}
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
                return False, {"error": "right_indicator_fetch_failed", "exception": str(e), "right": right_label, "rid": cond.get("rid")}
            if right_val is None:
                return False, {"error": "right_value_none", "rid": cond.get("rid")}

        if (right_label or "").strip().lower() != "change" and right_pct_legacy is not None and right_val is not None:
            right_val = right_val * (1.0 + (right_pct_legacy / 100.0))

    # Compare
    try:
        result = bool(OPS[op](float(left_val), float(right_val)))  # type: ignore[arg-type]
    except Exception as e:
        return False, {"error": "operator_error", "exception": str(e), "rid": cond.get("rid")}

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
        "rid": (cond.get("rid") or None),
    }
    if DEBUG_VALUES:
        log.debug(f"[EVAL] {main_symbol}@{main_interval} {left_label} {details['op_norm']} {right_label} -> {result} ({dt:.1f} ms)")
        print(f"[DBG] cond rid={details.get('rid')} res={result}")
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
) -> Tuple[str, List[Dict[str, Any]], Optional[str], Optional[str], bool]:
    conditions: List[Dict[str, Any]] = group.get("conditions") or []
    main_interval = (group.get("interval") or "").strip()
    if not main_interval:
        return "NONE", [], None, "missing_interval", False

    # per-group min_tick
    min_tick = _group_min_tick(group)

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
        per_details.append(details)

        if details.get("error"): hard_error = details.get("error")
        any_true = any_true or bool(res)
        group_result = res if group_result is None else (
            (group_result or res) if (cond.get("logic") or "and").strip().lower() == "or" else (group_result and res)
        )

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

    # tick confirmation (soft) – Gate/Streak kümmert sich um "mehrere nacheinander"
    now_dt = _iso_now_dt()
    _, confirmed_soft = _bar_close_info(bar_ts, main_interval, now_dt)
    tick_confirmed = (confirmed_soft is True and min_tick <= 1)

    # details fertig
    return status, per_details, bar_ts, hard_error, tick_confirmed

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

def _save_commands(d: Dict[str, Any]) -> None:
    q = list(d.get("queue") or [])
    if len(q) > 5000:  # Sicherheitslimit
        q = q[-5000:]
        d["queue"] = q
    _json_save_any(_COMMANDS_PATH, d)

# Unified bridge (optional)
def _load_commands_unified() -> Dict[str, Any]:
    if NOTIFIER_UNIFIED is None: return {"queue": []}
    try:
        u = _json_load_any(_to_path(NOTIFIER_UNIFIED), {})  # type: ignore[arg-type]
        q = list(((u.get("commands") or {}).get("queue") or []))
        return {"queue": q}
    except Exception:
        return {"queue": []}

def _consume_commands_unified(consumed_ids: Set[str]) -> None:
    if NOTIFIER_UNIFIED is None or not consumed_ids:
        return
    try:
        path = _to_path(NOTIFIER_UNIFIED)  # type: ignore[arg-type]
        u = _json_load_any(path, {})
        cmds = (u.get("commands") or {})
        q = list(cmds.get("queue") or [])
        if not q:
            return
        q2 = [it for it in q if str(it.get("id") or "") not in consumed_ids]
        u.setdefault("commands", {})["queue"] = q2
        _json_save_any(path, u)
    except Exception as e:
        if DEBUG_VALUES:
            log.debug(f"[BRIDGE] consume unified failed: {e}")

def _prune_status_and_overrides(
    status: Dict[str, Any],
    overrides: Dict[str, Any],
    profiles: List[Dict[str, Any]],
) -> Tuple[bool, bool]:
    """
    Entfernt nicht mehr existierende Profile/Gruppen aus status & overrides.
    Rückgabe: (status_changed, overrides_changed)
    """
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
                continue
            wanted_pg.add((pid, gid))

    # --- Status prune ---
    st_changed = False
    status.setdefault("profiles", {})
    cur_profiles = status["profiles"]

    for pid in list(cur_profiles.keys()):
        if pid not in wanted_pids:
            if DEBUG_VALUES:
                log.debug(f"[PRUNE][status] drop profile pid={pid} (no longer in /profiles)")
            del cur_profiles[pid]
            st_changed = True
            continue
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
        # Details initial leer, damit UI nie auf Fallback muss
        "runtime": {"met": 0, "total": len(group.get("conditions") or []), "true_ticks": None, "details": []},
    }

def sync_status_from_profiles(profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
    st = _load_status()
    st.setdefault("profiles", {})

    # Vor dem Auffüllen: prunen
    dummy_overrides = _load_overrides()
    _prune_status_and_overrides(st, dummy_overrides, profiles)
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
# Single-Symbol Values: Helpers (origin/latest je rid)
# ─────────────────────────────────────────────────────────────
def _ensure_values_slot(grp_st: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """
    Sorgt dafür, dass grp_st['runtime']['values'][symbol] existiert und liefert das dict zurück.
    Struktur:
      runtime.values[<symbol>][rid].left/right = {
         value_origin, ts_origin, value_latest, ts_latest, col, output
      }
    """
    rt = grp_st.setdefault("runtime", {})
    vals = rt.setdefault("values", {})
    return vals.setdefault(symbol, {})

def _persist_single_symbol_values(grp_st: Dict[str, Any], symbol: str, per_symbol_evals: List[Dict[str, Any]]) -> None:
    """
    Persistiert für den Single-Symbol-Fall die Werte (origin/latest) je rid.
    Nimmt das erste Event (repräsentativ) und schreibt left/right-Werte.
    """
    if not per_symbol_evals:
        return
    vals_sym = _ensure_values_slot(grp_st, symbol)
    sample = per_symbol_evals[0]
    for cd in (sample.get("conditions") or []):
        rid = cd.get("rid") or "no_rid"

        # LEFT
        l = cd.get("left") or {}
        lv, lts = l.get("value"), l.get("ts")
        if lv is not None:
            slot = vals_sym.setdefault(rid, {}).setdefault("left", {
                "value_origin": None, "ts_origin": None,
                "value_latest": None, "ts_latest": None,
                "col": l.get("col"), "output": l.get("output"),
            })
            if slot["value_origin"] is None:
                slot["value_origin"], slot["ts_origin"] = float(lv), lts
            slot["value_latest"], slot["ts_latest"] = float(lv), lts
            slot["col"], slot["output"] = l.get("col"), l.get("output")

        # RIGHT
        r = cd.get("right") or {}
        rv, rts = r.get("value"), r.get("ts")
        if rv is not None:
            slot = vals_sym.setdefault(rid, {}).setdefault("right", {
                "value_origin": None, "ts_origin": None,
                "value_latest": None, "ts_latest": None,
                "col": r.get("col"), "output": r.get("output"),
            })
            if slot["value_origin"] is None:
                slot["value_origin"], slot["ts_origin"] = float(rv), rts
            slot["value_latest"], slot["ts_latest"] = float(rv), rts
            slot["col"], slot["output"] = r.get("col"), r.get("output")

def _profiles_quick_summary(profiles: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for p in profiles or []:
        pid = str(p.get("id") or "")
        pname = p.get("name") or ""
        groups = p.get("condition_groups") or []
        out.append(f"[PROF] id={pid} name={pname!r} groups={len(groups)} enabled={bool(p.get('enabled', True))}")
        for gi, g in enumerate(groups):
            gid = str(g.get("gid") or f"g{gi}")
            iv  = (g.get("interval") or "").strip()
            syms = [s for s in (g.get("symbols") or []) if isinstance(s, str) and s.strip()]
            conds = g.get("conditions") or []
            out.append(
                f"  └─[GRP] gid={gid} name={g.get('name') or ''!r} interval={iv or '—'} symbols={len(syms)} "
                f"conditions={len(conds)} active={bool(g.get('active', True))}"
            )
            if not iv or not syms:
                out.append("     ⚠ misconfigured: interval und/oder symbols fehlen")
            # Zeig die ersten zwei Conditions kurz an
            for ci, c in enumerate(conds[:2]):
                out.append(
                    f"     · cond#{ci}: left={c.get('left')!r} op={c.get('op')!r} right={c.get('right')!r} "
                    f"right_symbol={c.get('right_symbol')!r} right_interval={c.get('right_interval')!r}"
                )
    if not out:
        out.append("[PROF] keine Profile vom Endpoint erhalten")
    return out

# ─────────────────────────────────────────────────────────────
# /profiles payload-Check
# ─────────────────────────────────────────────────────────────
def _validate_profiles_payload(profiles: Any) -> List[Dict[str, Any]]:
    if not isinstance(profiles, list):
        raise RuntimeError("Profiles payload is not a list.")
    out: List[Dict[str, Any]] = []
    for i, p in enumerate(profiles):
        if not isinstance(p, dict):
            raise RuntimeError(f"Profile #{i} is not an object.")
        cg = p.get("condition_groups")
        if cg is None:
            p = {**p, "condition_groups": []}
        elif not isinstance(cg, list):
            raise RuntimeError(f"Profile #{i} .condition_groups is not a list.")
        out.append(p)
    return out

# ─────────────────────────────────────────────────────────────
# Top-Level: run_check
# ─────────────────────────────────────────────────────────────
def run_check() -> List[Dict[str, Any]]:
    """
    Evaluator ohne Auto-Deaktivierung.
    - Ermittelt je Gruppe+Symbol den Status: FULL / PARTIAL / NONE
    - Baut EVAL-Events (inkl. deactivate_on, min_true_ticks)
    - Übergibt an gate_and_build_triggers (Streak-Gate pro Modus)
    - Schreibt Status & konsumiert Commands (Datei + optional Unified)
    """
    global _CACHE_HIT, _CACHE_MISS
    _INDICATOR_CACHE.clear()
    _CACHE_HIT = 0
    _CACHE_MISS = 0

    status = _load_status()
    overrides = _load_overrides()
    commands = _load_commands()
    unified_cmds = _load_commands_unified()

    # Commands indexieren (Merge: Datei + Unified)
    cmds_by_pg: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for it in list((commands.get("queue") or [])) + list((unified_cmds.get("queue") or [])):
        pid = str(it.get("profile_id") or ""); gid = str(it.get("group_id") or "")
        if pid and gid:
            cmds_by_pg.setdefault((pid, gid), []).append(it)

    t_start = time.perf_counter()
    try:
        profiles = _load_profiles()
    except Exception as e:
        log.error(f"⚠️ Fehler beim Laden der Profile: {e}")
        print(f"[FATAL] profiles load failed: {e}")
        return []

    # Validierung
    try:
        profiles = _validate_profiles_payload(profiles)
    except Exception as e:
        log.error(f"⚠️ /profiles validation failed: {e}")
        print(f"[FATAL] profiles validation failed: {e}")
        return []

    try:
        summary_lines = _profiles_quick_summary(profiles)
        for line in summary_lines:
            log.info(line)
            print(line)
        # Harter Hinweis, wenn 0 Gruppen:
        total_groups = sum(len(p.get("condition_groups") or []) for p in profiles or [])
        if total_groups == 0:
            log.warning("❌ /notifier/profiles enthält KEINE condition_groups -> Status.groups bleibt leer.")
            print("❌ HINWEIS: /notifier/profiles enthält KEINE condition_groups -> der Status kann keine groups haben.")
    except Exception as e:
        log.warning(f"[DBG] profiles summary failed: {e}")

    try:
        meta = _load_indicators_meta()
    except Exception as e:
        log.error(f"⚠️ Fehler beim Laden der Indikator-Metadaten: {e}")
        print(f"[FATAL] indicators meta load failed: {e}")
        return []

    print(f"[EVAL] profiles={len(profiles)} ts={_now_iso()}")

    # PRUNE
    st_changed, ovr_changed = _prune_status_and_overrides(status, overrides, profiles)
    if st_changed:
        status["version"] = int(status.get("version", 0)) + 1
        _save_status(status)
        if DEBUG_VALUES:
            log.debug("[PRUNE] status pruned & saved")
            print("[DBG] pruned status")
    if ovr_changed:
        overrides["updated_ts"] = _now_iso()
        _json_save_any(_OVERRIDES_PATH, overrides)
        if DEBUG_VALUES:
            log.debug("[PRUNE] overrides pruned & saved")
            print("[DBG] pruned overrides")

    evals: List[Dict[str, Any]] = []
    now_iso = _now_iso(); now_dt = _iso_now_dt()

    status.setdefault("profiles", {})
    prev_version = int(status.get("version", 0))
    cur_profiles = status["profiles"]

    consumed_cmd_ids: Set[str] = set()

    for p_idx, profile in enumerate(profiles):
        pid = str(profile.get("id") or "")
        if not pid:
            continue

        prof_st = cur_profiles.setdefault(pid, {})
        prof_st["profile_active"] = bool(profile.get("enabled", True))
        prof_st["id"] = pid
        prof_st["name"] = profile.get("name") or ""
        gmap = prof_st.setdefault("groups", {})

        if not profile.get("enabled", True):
            if DEBUG_VALUES:
                log.debug(f"[ACTIVE] skip profile pid={pid} (enabled=0)")
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

            # Commands anwenden (aus beiden Quellen)
            rebaseline_requested = False
            for cmd in cmds_by_pg.get((pid, gid), []):
                cmd_id = str(cmd.get("id") or "")
                if cmd_id in consumed_cmd_ids:
                    continue
                rearm = bool(cmd.get("rearm", True))
                rebaseline = bool(cmd.get("rebaseline", False))
                if rearm:
                    auto_disabled = False
                    # kein künstlicher Cooldown-Zeitpunkt setzen (intuitiveres Rearm)
                    if DEBUG_VALUES: log.debug(f"[COMMAND] REARM pid={pid} gid={gid}")
                    print(f"[CMD] REARM pid={pid} gid={gid}")
                if rebaseline:
                    rebaseline_requested = True
                    if DEBUG_VALUES: log.debug(f"[COMMAND] REBASELINE pid={pid} gid={gid}")
                    print(f"[CMD] REBASELINE pid={pid} gid={gid}")
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
                # bestehenden runtime.values nicht verlieren
                rt_prev = grp_st.get("runtime") or {}
                grp_st.update({
                    "name": (group.get("name") or f"group_{g_idx}"),
                    "effective_active": False,
                    "blockers": blockers,
                    "auto_disabled": auto_disabled,
                    "cooldown_until": cooldown_until_iso,
                    "fresh": fresh,
                    "aggregate": {
                        "logic": "AND",
                        "passed": False,
                        "notify_mode": _normalize_notify_mode(group),
                        "min_true_ticks": _min_true_ticks_of(group) or 1,
                    },
                    "conditions": _label_only_conditions(group),
                    "runtime": {**{
                        "met": 0,
                        "total": len(group.get("conditions") or []),
                        "true_ticks": None,
                        "details": []
                    }, **({"values": rt_prev.get("values")} if "values" in rt_prev else {})},
                })
                continue

            effective_active = (
                bool(profile.get("enabled", True))
                and bool(group.get("active", True))
                and (not forced_off)
                and (not (snooze_until_dt and now_dt < snooze_until_dt))
                and (not auto_disabled)
                and (not (cooldown_until_dt and now_dt < cooldown_until_dt))
            )

            notify_mode = _normalize_notify_mode(group)
            min_ticks = _min_true_ticks_of(group)
            per_symbol_evals: List[Dict[str, Any]] = []
            ts_list: List[Optional[str]] = []

            if effective_active:
                for sym in symbols:
                    status_str, cond_details, bar_ts, err, tick_confirmed = _eval_group_for_symbol(meta, profile, group, sym, g_idx)
                    if err:
                        hard_error = err

                    # single_mode optional (kompatibel; default "symbol")
                    single_mode = (group.get("single_mode") or "symbol").strip().lower()  # "symbol" | "group" | "everything"
                    tick_id = f"{main_interval}:{(bar_ts or now_iso)}"

                    ev = {
                        "profile_id": pid,
                        "profile_name": profile.get("name"),
                        "group_id": gid,
                        "group_index": g_idx,
                        "group_name": group.get("name") or f"group_{g_idx}",
                        "symbol": sym,
                        "interval": main_interval,
                        "exchange": group.get("exchange") or None,
                        "telegram_bot_id": _to_int_or_none(group.get("telegram_bot_id")),
                        "telegram_bot_token": group.get("telegram_bot_token") or None,
                        "telegram_chat_id": group.get("telegram_chat_id") or None,
                        "description": group.get("description") or None,
                        "ts": now_iso,
                        "bar_ts": bar_ts or now_iso,
                        "tick_id": tick_id,
                        "status": status_str,
                        "notify_mode": notify_mode,
                        "min_true_ticks": min_ticks,
                        "min_tick": _group_min_tick(group),
                        "tick_confirmed": tick_confirmed,
                        "single_mode": single_mode,
                        "conditions": cond_details,
                    }

                    per_symbol_evals.append(ev)
                    evals.append(ev)
                    if bar_ts:
                        ts_list.append(bar_ts)

                    if DEBUG_VALUES:
                        log.debug(
                            f"[EVAL-EVENT] pid={pid} gid={gid} sym={sym} "
                            f"status={status_str} mode={notify_mode} single={single_mode} tick={tick_id}"
                        )
                        print(f"[DBG] event pid={pid} gid={gid} sym={sym} status={status_str}")
            else:
                if DEBUG_VALUES:
                    log.debug(f"[ACTIVE] skip evaluation pid={pid} gid={gid} due blockers={blockers}")
                    print(f"[DBG] skip pid={pid} gid={gid} blockers={blockers}")
                grp_st["conditions"] = _label_only_conditions(group)

            if hard_error:
                blockers.append("error")
                fresh = False

            effective_active = effective_active and fresh and not any(
                b in ("forced_off", "snooze", "auto_disabled", "cooldown", "group_inactive", "misconfigured")
                for b in blockers
            )

            met = total = 0
            if per_symbol_evals:
                try:
                    sample_conds = per_symbol_evals[0].get("conditions") or []
                    total = len(sample_conds)
                    met = sum(1 for c in sample_conds if c and bool(c.get("result")))
                except Exception:
                    pass
            else:
                total = len(group.get("conditions") or [])
                met = 0

            # Bedingungen + Details aus erstem Symbol (repräsentativ)
            conditions_list: List[Dict[str, Any]] = []
            details_list: List[Dict[str, Any]] = []
            if per_symbol_evals:
                try:
                    sample = per_symbol_evals[0]
                    for cd in (sample.get("conditions") or []):
                        left = cd.get("left") if isinstance(cd.get("left"), dict) else {}
                        right = cd.get("right") if isinstance(cd.get("right"), dict) else {}

                        # UI-Kurzform (conditions)
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

                        # Tiefe Details (runtime.details)
                        details_list.append({
                            "rid": cd.get("rid") or None,
                            "op": cd.get("op_norm") or cd.get("op"),
                            "result": bool(cd.get("result")),
                            "left": {
                                "label": left.get("label"),
                                "spec": left.get("spec"),
                                "output": left.get("output"),
                                "col": left.get("col"),
                                "value": left.get("value"),
                                "symbol": left.get("symbol"),
                                "interval": left.get("interval"),
                                "ts": left.get("ts"),
                                "params": left.get("params") or {},
                            },
                            "right": {
                                "label": right.get("label"),
                                "spec": right.get("spec"),
                                "output": right.get("output"),
                                "col": right.get("col"),
                                "value": right.get("value"),
                                "symbol": right.get("symbol"),
                                "interval": right.get("interval"),
                                "ts": right.get("ts"),
                                "params": right.get("params") or {},
                                "right_absolut": right.get("right_absolut"),
                                "right_change_legacy_pct": right.get("right_change_legacy_pct"),
                            },
                            "duration_ms": cd.get("duration_ms"),
                            "error": cd.get("error"),
                        })
                except Exception as e:
                    log.debug(f"[STATUS] details build failed pid={pid} gid={gid}: {e}")

            # Single-Symbol-Werte speichern (origin/latest) je rid
            if len(symbols) == 1 and per_symbol_evals:
                try:
                    _persist_single_symbol_values(grp_st, symbols[0], per_symbol_evals)
                except Exception as e:
                    if DEBUG_VALUES:
                        log.debug(f"[VALUES] persist failed pid={pid} gid={gid}: {e}")

            # Rebaseline anwenden (origin := latest) — nur Single-Symbol-Gruppen
            if len(symbols) == 1 and rebaseline_requested:
                try:
                    rt_vals = (grp_st.get("runtime") or {}).get("values") or {}
                    vals_sym = rt_vals.get(symbols[0]) or {}
                    for rid, sides in vals_sym.items():
                        for side in ("left", "right"):
                            slot = sides.get(side)
                            if not isinstance(slot, dict):
                                continue
                            if slot.get("value_latest") is not None:
                                slot["value_origin"] = slot["value_latest"]
                                slot["ts_origin"] = slot.get("ts_latest")
                    if DEBUG_VALUES:
                        log.debug(f"[VALUES] rebaseline applied pid={pid} gid={gid} sym={symbols[0]}")
                        print(f"[DBG] rebaseline applied pid={pid} gid={gid} sym={symbols[0]}")
                except Exception as e:
                    if DEBUG_VALUES:
                        log.debug(f"[VALUES] rebaseline failed pid={pid} gid={gid}: {e}")

            # Vorhandene true_ticks und values erhalten
            rt_prev = grp_st.get("runtime") or {}
            prev_true_ticks = rt_prev.get("true_ticks")
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
                        or (_normalize_notify_mode(group) == "any_true" and ev["status"] in ("FULL", "PARTIAL"))
                        for ev in per_symbol_evals
                    ),
                    "notify_mode": _normalize_notify_mode(group),
                    "min_true_ticks": (min_ticks if min_ticks is not None else 1),
                },
                # Klar im Status hinterlegen
                "min_tick": _group_min_tick(group),
                # Conditions Digest (wie gehabt):
                "conditions": conditions_list if conditions_list else _label_only_conditions(group),
                "runtime": {
                    "met": met,
                    "total": total,
                    "true_ticks": prev_true_ticks,
                    "details": details_list,
                    **({"values": rt_prev.get("values")} if "values" in rt_prev else {}),
                    # runtime timestamps for auditability
                    "created_ts": (rt_prev.get("created_ts") or now_iso),
                    "updated_ts": now_iso,
                    # dedupe / alarm bookkeeping hooks (Gate/Alarm kann sie setzen)
                    "last_alarm_tick_id": (rt_prev.get("last_alarm_tick_id") if isinstance(rt_prev, dict) else None),
                    "last_alarm_ts": (rt_prev.get("last_alarm_ts") if isinstance(rt_prev, dict) else None),
                },
                # Storage-Block Layout Marker (Selbstbeschreibung)
                "storage_layout": "unified-config-runtime",
            })

            if DEBUG_VALUES:
                log.debug(f"[STATUS] pid={pid} gid={gid} conds={len(conditions_list)} details={len(details_list)}")

    # Gating + Trigger bauen
    triggered = gate_and_build_triggers(evals)

    # Commands konsumieren (Datei)
    if consumed_cmd_ids:
        # Datei-Commands
        if (commands.get("queue")):
            commands["queue"] = [it for it in (commands.get("queue") or []) if str(it.get("id") or "") not in consumed_cmd_ids]
            _save_commands(commands)
            if DEBUG_VALUES: log.debug(f"[COMMAND] consumed(file)={len(consumed_cmd_ids)} left={len(commands.get('queue') or [])}")
            print(f"[DBG] consumed(file)={len(consumed_cmd_ids)}")
        # Unified-Commands
        _consume_commands_unified(consumed_cmd_ids)

    # API-kompatible Metadaten setzen, damit /notifier/status nicht "autofixed"
    # und unsere Gruppen/Conditions nicht überschreibt.
    try:
        fp = _profiles_fingerprint(profiles)
    except Exception:
        fp = ""
    status["flavor"] = "notifier-api"
    status["profiles_fp"] = fp
    status["version"] = int(prev_version) + 1
    _save_status(status)

    dt_total = (time.perf_counter() - t_start) * 1000.0
    log.info(f"Evals={len(evals)} → Trigger={len(triggered)} — Status v{status['version']} geschrieben — Laufzeit: {dt_total:.1f} ms")
    if triggered and DEBUG_VALUES:
        try:
            log.debug(json.dumps(triggered[:2], ensure_ascii=False, indent=2))
        except Exception:
            pass

    # Cache-Stats
    try:
        log.info(f"cache: hit={_CACHE_HIT} miss={_CACHE_MISS} size={len(_INDICATOR_CACHE)}")
        print(f"[DBG] cache hit={_CACHE_HIT} miss={_CACHE_MISS} size={len(_INDICATOR_CACHE)}")
    except Exception:
        pass

    return triggered

# ─────────────────────────────────────────────────────────────
# CLI Helper
# ─────────────────────────────────────────────────────────────
def run_evaluator_once() -> int:
    try:
        events = run_check()
        return len(events)
    except Exception as e:
        log.exception("run_evaluator_once failed: %s", e)
        return -1

def run_evaluator_loop(period_seconds: int = 60):
    """
    Einfacher Loop ohne Async – ruft alle X Sekunden run_check() auf.
    Bricht nur bei KeyboardInterrupt ab.
    """
    log.info("⏱️ evaluator loop started period=%ss", period_seconds)
    try:
        while True:
            n = run_evaluator_once()
            log.info("tick done: events=%s", n)
            time.sleep(max(5, int(period_seconds)))
    except KeyboardInterrupt:
        log.info("evaluator loop stopped by user")

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
