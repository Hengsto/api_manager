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
# ✨ NEU: Persistenz (Status/Overrides/Commands) aus config
# ─────────────────────────────────────────────────────────────
from config import NOTIFIER_ENDPOINT, PRICE_API_ENDPOINT, CHART_API_ENDPOINT

# Optional: lokaler Profiles-Pfad, falls API-Write nicht geht (hier ungenutzt)
try:
    from config import PROFILES_NOTIFIER  # type: ignore
except Exception:
    PROFILES_NOTIFIER = None  # type: ignore[assignment]

# Optional/fallback: Status/Overrides/Commands Pfade für Persistenz
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

# ─────────────────────────────────────────────────────────────
# ✨ NEU: Pfade & Locks
# ─────────────────────────────────────────────────────────────
def _to_path(p: Any | None) -> Optional[Path]:
    if p is None: return None
    if isinstance(p, Path): return p
    return Path(str(p)).expanduser().resolve()

# Fallback-Basis für JSON-Dateien
_BASE_DIR = _to_path(PROFILES_NOTIFIER).parent if _to_path(PROFILES_NOTIFIER) else Path(os.getcwd())

_STATUS_PATH    = _to_path(STATUS_NOTIFIER)    or (_BASE_DIR / "notifier_status.json")
_OVERRIDES_PATH = _to_path(OVERRIDES_NOTIFIER) or (_BASE_DIR / "notifier_overrides.json")
_COMMANDS_PATH  = _to_path(COMMANDS_NOTIFIER)  or (_BASE_DIR / "notifier_commands.json")

for _p in (_STATUS_PATH, _OVERRIDES_PATH, _COMMANDS_PATH):
    try:
        _p.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.warning(f"[DEBUG] mkdir failed for {_p.parent}: {e}")

# Lock-Verzeichnis
_LOCK_DIR = Path(os.environ.get("NOTIFIER_LOCK_DIR", "") or (Path(os.getenv("TMPDIR", "/tmp")) / "notifier_locks"))
_LOCK_DIR.mkdir(parents=True, exist_ok=True)
log.info(f"[DEBUG] Using lock dir: {_LOCK_DIR}")

def _lock_path(path: Path) -> Path:
    try:
        name = Path(path).name
    except Exception:
        name = str(path)
    return _LOCK_DIR / (name + ".lock")

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
                os.close(fd)
                self._acq = True
                if DEBUG_VALUES:
                    log.debug(f"[LOCK] acquired {self.lockfile}")
                return
            except FileExistsError:
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lockfile}")
                time.sleep(self.poll)
    def release(self):
        if self._acq:
            try:
                os.unlink(self.lockfile)
                if DEBUG_VALUES:
                    log.debug(f"[LOCK] released {self.lockfile}")
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
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        if DEBUG_VALUES: log.debug(f"[IO] load {path} ok (type={type(data).__name__})")
        return data
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
        # fsync parent dir (best effort)
        try:
            if hasattr(os, "O_DIRECTORY"):
                dfd = os.open(str(path.parent), os.O_DIRECTORY)
                try: os.fsync(dfd)
                finally: os.close(dfd)
        except Exception: pass
    if DEBUG_VALUES: log.debug(f"[IO] save {path} ok")

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
            log.warning(f"[HTTP] Fehler beim POST {url}: {e} (try {i+1}/{tries})")
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

# ✨ UI-kompatible Helpers
def _clean_params_for_api(params: Dict[str, Any] | None) -> Dict[str, Any]:
    """Leere/irrelevante Keys entfernen; 'source' nur senden, wenn gesetzt; niemals 'symbol' als param senden."""
    p = {k: v for k, v in (params or {}).items() if v not in (None, "", [], {})}
    p.pop("symbol", None)
    if not p.get("source"):
        p.pop("source", None)
    return p

def _stable_params_json(params: Dict[str, Any]) -> str:
    """Stabile JSON-Serialisierung (Schlüssel sortiert)."""
    return json.dumps(params, separators=(",", ":"), sort_keys=True)

def _effective_intervals(chart_interval: Optional[str], indicator_interval: Optional[str]) -> tuple[str, str]:
    """Spiegelt Intervalle. Wenn beide fehlen → '1d'/'1d'."""
    ci = (chart_interval or "").strip()
    ii = (indicator_interval or "").strip()
    if ci and not ii:
        ii = ci
    elif ii and not ci:
        ci = ii
    if not ci and not ii:
        ci = ii = "1d"
    return ci, ii

def _fetch_indicator_series(name: str, symbol: str, chart_interval: str, indicator_interval: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # Intervalle wie in der UI spiegeln
    eff_ci, eff_ii = _effective_intervals(chart_interval, indicator_interval)
    # Params wie in der UI säubern + stabil serialisieren
    clean = _clean_params_for_api(params)
    params_json = _stable_params_json(clean)

    # Cache-Key mit effektiven Werten
    key = _indicator_cache_key(name, symbol, eff_ci, eff_ii, clean)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    # UI-kompatibler Endpoint
    url = f"{CHART_API_ENDPOINT}/indicator"
    query = {
        "name": name,
        "symbol": symbol,
        "chart_interval": eff_ci,
        "indicator_interval": eff_ii,
        "params": params_json,
        "count": 5,  # wichtig: min. Tail
    }
    if DEBUG_VALUES:
        log.debug(f"[FETCH] {name} sym={symbol} chart_iv={eff_ci} ind_iv={eff_ii} params={clean} count=5")

    t0 = time.perf_counter()
    sess = _get_session()
    r = sess.get(url, params=query, timeout=HTTP_TIMEOUT)
    # 400/422 tolerieren → leeres data-Array
    if r.status_code in (400, 422):
        if DEBUG_VALUES:
            snippet = (r.text or "")[:200].replace("\n", " ")
            log.debug(f"[FETCH] {r.url} -> {r.status_code} (treated as empty). body[:200]={snippet}")
        data = {"data": []}
    else:
        if r.status_code >= 400:
            snippet = (r.text or "")[:300].replace("\n", " ")
            log.warning(f"[HTTP] GET {r.url} -> {r.status_code} body[:300]={snippet}")
        r.raise_for_status()
        try:
            data = r.json()
        except Exception as je:
            # Wenn JSON kaputt ist, leer liefern statt crashen
            log.warning(f"[HTTP] JSON decode failed on {r.url}: {je}")
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
        # bool ausschließen; NaN/Inf verwerfen
        if isinstance(x, bool):
            return False
        if isinstance(x, (int, float)):
            return math.isfinite(float(x))
        return False
    except Exception:
        return False


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

    # Timestamp holen, aber Wert aus "last"
    ts = last.get("Timestamp_ISO")

    # Kolumnen nach Priorität durchsuchen
    val, col = _pick_value_from_row(last, dedup)
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

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")

def _safe_max_iso(ts_list: List[Optional[str]], fallback: Optional[str] = None) -> Optional[str]:
    vals = [t for t in ts_list if isinstance(t, str) and t]
    if not vals:
        return fallback
    try:
        pairs = []
        for t in vals:
            dt = _parse_iso(t) or datetime.min.replace(tzinfo=timezone.utc)
            pairs.append((dt, t))
        pairs.sort(key=lambda x: x[0])
        return pairs[-1][1]
    except Exception:
        # Fallback: lexikalisch
        try:
            return max(vals)
        except Exception:
            return fallback


def _normalize_notify_mode(group: Dict[str, Any]) -> str:
    """
    Mappt UI-Feld deactivate_on auf Notify-Modus.
    Semantik:
      - "always"  => wie "true" behandeln (nur FULL zählt), aber Gruppe bleibt aktiv (wir deaktivieren nie)
      - "true"    => nur FULL benachrichtigen
      - "any_true"=> bereits bei PARTIAL (mind. 1 Bedingung) benachrichtigen
    Rückgabe: "always" | "true" | "any_true"
    """
    val = group.get("deactivate_on")
    if val is None:
        # Legacy: auto_deactivate == True ⇒ "true", sonst "always"
        if group.get("auto_deactivate"):
            return "true"
        return "always"
    s = str(val).strip().lower()
    if s in {"always"}:
        return "always"
    if s in {"true", "full", "match"}:
        return "true"
    if s in {"any_true", "any", "partial"}:
        return "any_true"
    # Fallback sicher: always
    return "always"

def _min_true_ticks_of(group: Dict[str, Any]) -> Optional[int]:
    v = group.get("min_true_ticks")
    if v in (None, "", "null"):
        return None
    try:
        i = int(v)
        return i if i >= 1 else 1
    except Exception:
        return None

# ✨ NEU: ISO-Zeit utils
def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    try:
        # tolerant: Z / ohne Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def _iso_now_dt() -> datetime:
    return datetime.now(timezone.utc)

# ✨ NEU: Required-Params absichern (insb. source)
def _ensure_required_params(meta: Dict[str, Dict[str, Any]], name: Optional[str], p: Dict[str, Any]) -> Dict[str, Any]:
    if not name:
        return p
    out = dict(p or {})
    spec = meta.get(str(name).lower()) or {}
    req  = spec.get("required_params") or {}
    dfl  = spec.get("default_params") or {}
    # 'source' injizieren, wenn required & nicht gesetzt
    if "source" in req and not out.get("source"):
        out["source"] = dfl.get("source") or "Close"
    return out

# ✨ NEU: Label-only-Conditions für Status (wenn nicht evaluiert wird)
def _label_only_conditions(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in (group.get("conditions") or []):
        if not isinstance(c, dict):
            continue
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
        if not right:
            right = "—"
        op = (c.get("op") or "gt").strip().lower()
        out.append({
            # Anzeige / Meta
            "left":         left,
            "right":        right,
            "left_spec":    None,
            "right_spec":   None,
            "left_output":  None,
            "right_output": None,
            "left_col":     None,
            "right_col":    None,
            # Operator-Ergebnis
            "op":     op,
            "passed": False,
            # Werte & Timing
            "left_value":  None,
            "right_value": None,
            "left_ts":     None,
            "right_ts":    None,
            "eval_ms":     None,
            # Fehler (falls vorhanden)
            "error": None,
        })
    return out

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
            # fehlende required params (z.B. source) ergänzen
            left_p = _ensure_required_params(meta, left_name, left_p)
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
                # fehlende required params (z.B. source) ergänzen
                right_p = _ensure_required_params(meta, right_name, right_p)
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
        "op_norm": _normalize_op(cond.get("op") or ""),
        "result": result,
        "duration_ms": round(dt, 2),
    }
    if DEBUG_VALUES:
        log.debug(f"[EVAL] {main_symbol}@{main_interval} {left_label} {details['op_norm']} {right_label} -> {result} ({dt:.1f} ms)")
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
) -> Tuple[str, List[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Returns:
      status ∈ {"FULL","PARTIAL","NONE"}
      cond_details
      bar_ts (max Timestamp_ISO aus den beteiligten Reihen)
      error_str (falls harter Fehler für Blocker)
    """
    t0 = time.perf_counter()
    conditions: List[Dict[str, Any]] = group.get("conditions") or []
    main_interval = (group.get("interval") or "").strip()
    if not main_interval:
        return "NONE", [], None, "missing_interval"

    group_result: Optional[bool] = None
    any_true: bool = False
    per_details: List[Dict[str, Any]] = []
    hard_error: Optional[str] = None

    for idx, cond in enumerate(conditions):
        try:
            res, details = evaluate_condition_for_symbol(meta, cond, symbol, main_interval)
        except Exception as e:
            res, details = False, {"error": "eval_exception", "exception": str(e)}
        details["idx"] = idx
        per_details.append(details)

        # Fehler markieren, aber weiterrechnen (zeigt sich als blocker=error)
        if details.get("error"):
            hard_error = details.get("error")

        any_true = any_true or bool(res)
        if group_result is None:
            group_result = res
        else:
            logic = (cond.get("logic") or "and").strip().lower()
            group_result = (group_result or res) if logic == "or" else (group_result and res)

    # Status ableiten
    if bool(group_result):
        status = "FULL"
    elif any_true:
        status = "PARTIAL"
    else:
        status = "NONE"

    # Bar-TS aus Details ziehen
    ts_candidates: List[Optional[str]] = []
    try:
        for d in per_details:
            lts = (d.get("left") or {}).get("ts")
            rts = (d.get("right") or {}).get("ts")
            if lts:
                ts_candidates.append(lts)
            if rts:
                ts_candidates.append(rts)
    except Exception:
        pass
    bar_ts = _safe_max_iso(ts_candidates, fallback=None)

    dt = (time.perf_counter() - t0) * 1000.0
    if DEBUG_VALUES:
        log.debug(f"[GROUP] {profile.get('name')}[{group_index}] {symbol}@{main_interval} -> {status} "
                  f"(conds={len(conditions)}, {dt:.1f} ms)")
    return status, per_details, bar_ts, hard_error

# ─────────────────────────────────────────────────────────────
# ✨ NEU: Status laden/schreiben + Commands/Overrides verarbeiten
# ─────────────────────────────────────────────────────────────
_STATUS_TEMPLATE: Dict[str, Any] = {"version": 0, "updated_ts": None, "profiles": {}}
_OVR_TEMPLATE: Dict[str, Any]    = {"overrides": {}, "updated_ts": None}
_CMD_TEMPLATE: Dict[str, Any]    = {"queue": []}

def _load_status() -> Dict[str, Any]:
    return _json_load_any(_STATUS_PATH, _STATUS_TEMPLATE)

def _save_status(st: Dict[str, Any]) -> None:
    st["updated_ts"] = _now_iso()
    _json_save_any(_STATUS_PATH, st)

def _load_overrides() -> Dict[str, Any]:
    d = _json_load_any(_OVERRIDES_PATH, _OVR_TEMPLATE)
    if not isinstance(d, dict) or "overrides" not in d:
        d = {"overrides": {}, "updated_ts": None}
    return d

def _ensure_ovr_slot(ovr: Dict[str, Any], pid: str, gid: str) -> Dict[str, Any]:
    ovr.setdefault("overrides", {})
    ovr["overrides"].setdefault(pid, {})
    ovr["overrides"][pid].setdefault(gid, {"forced_off": False, "snooze_until": None, "note": None})
    return ovr["overrides"][pid][gid]

def _load_commands() -> Dict[str, Any]:
    d = _json_load_any(_COMMANDS_PATH, _CMD_TEMPLATE)
    if not isinstance(d, dict) or "queue" not in d:
        d = {"queue": []}
    return d

def _save_commands(d: Dict[str, Any]) -> None:
    _json_save_any(_COMMANDS_PATH, d)

# --- Status-Sync aus Profilen (Skeleton für UI schreiben) --------------------

def _skeleton_group_from_def(group: Dict[str, Any], g_idx: int) -> Dict[str, Any]:
    name = group.get("name") or f"group_{g_idx}"
    interval = (group.get("interval") or "").strip()
    raw_symbols = [s for s in (group.get("symbols") or []) if s]
    symbols = list(dict.fromkeys(raw_symbols))
    misconfigured = (not interval) or (not symbols)

    notify_mode = _normalize_notify_mode(group)
    min_ticks   = _min_true_ticks_of(group) or 1

    return {
        "group_active": bool(group.get("active", True)),
        "last_eval_ts": None,     # noch nichts evaluiert
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
            "min_true_ticks": min_ticks,
        },
        "conditions": _label_only_conditions(group),  # nur Labels
        "runtime": {
            "met": 0,
            "total": len(group.get("conditions") or []),
            "true_ticks": None,
        },
    }


def sync_status_from_profiles(profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Schreibt notifier_status.json minimal anhand der Profile,
    damit das UI direkt nach Speichern schon Inhalte sieht.
    """
    st = _load_status()
    if "profiles" not in st or not isinstance(st["profiles"], dict):
        st["profiles"] = {}
    cur_profiles = st["profiles"]

    for p in profiles or []:
        pid = str(p.get("id") or "")
        if not pid:
            continue
        pobj = cur_profiles.setdefault(pid, {})
        pobj["profile_active"] = bool(p.get("enabled", True))
        pobj["id"]   = pid
        pobj["name"] = p.get("name") or ""

        gmap = pobj.setdefault("groups", {})
        groups = p.get("condition_groups") or []
        for g_idx, g in enumerate(groups):
            gid = str(g.get("gid") or f"g{g_idx}") or f"g{g_idx}"
            gmap[gid] = {**gmap.get(gid, {}), **_skeleton_group_from_def(g, g_idx)}

    st["version"] = int(st.get("version", 0)) + 1
    _save_status(st)
    return st



# -----------------------------------------------------------------------------
# Top-Level: run_check
# -----------------------------------------------------------------------------
def run_check() -> List[Dict[str, Any]]:
    """
    Evaluator ohne Auto-Deaktivierung.
    - Ermittelt je Gruppe+Symbol den Status: FULL / PARTIAL / NONE
    - Baut EVAL-Events (inkl. deactivate_on, min_true_ticks)
    - Übergibt an gate_and_build_triggers (Streak-Gate pro Modus)
    - Gibt die fertigen Trigger-Payloads zurück (für Alarm-Checker)

    ✨ NEU:
    - Lädt & schreibt status.json (effective_active, blockers, cooldown, auto_disabled, …)
    - Konsumiert commands (rearm/rebaseline) einmalig
    - Berücksichtigt overrides (forced_off/snooze)
    """
    _INDICATOR_CACHE.clear()

    # Voriger Status + Queue + Overrides
    status = _load_status()
    overrides = _load_overrides()
    commands = _load_commands()

    # Indexiere Commands pro (pid,gid) und konsumiere am Ende
    cmds_by_pg: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for it in list(commands.get("queue") or []):
        pid = str(it.get("profile_id") or "")
        gid = str(it.get("group_id") or "")
        if not pid or not gid:
            continue
        cmds_by_pg.setdefault((pid, gid), []).append(it)

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

    evals: List[Dict[str, Any]] = []
    now_iso = _now_iso()
    now_dt  = _iso_now_dt()

    # Status-Struktur vorbereiten
    if "profiles" not in status or not isinstance(status["profiles"], dict):
        status["profiles"] = {}
    prev_version = int(status.get("version", 0))
    cur_profiles = status["profiles"]

    # Durch Profile/Groups iterieren
    consumed_cmd_ids: Set[str] = set()

    for p_idx, profile in enumerate(profiles):
        pid = str(profile.get("id") or "")
        if not pid:
            continue

        # Stelle Profile-Container im Status sicher
        prof_st = cur_profiles.setdefault(pid, {})
        prof_st["profile_active"] = bool(profile.get("enabled", True))
        # 👇 NEU: für UI/normalize_status()
        prof_st["id"]   = pid
        prof_st["name"] = profile.get("name") or ""

        prof_st["profile_active"] = bool(profile.get("enabled", True))
        gmap = prof_st.setdefault("groups", {})

        if not profile.get("enabled", True):
            if DEBUG_VALUES:
                log.debug(f"[ACTIVE] skip profile pid={pid} (enabled=0)")
            continue

        groups = profile.get("condition_groups") or []
        for g_idx, group in enumerate(groups):
            gid = str(group.get("gid") or f"g{g_idx}")
            if not gid:
                gid = f"g{g_idx}"

            grp_st = gmap.setdefault(gid, {})
            grp_st["group_active"] = bool(group.get("active", True))
            grp_st["last_eval_ts"] = now_iso
            blockers: List[str] = []
            auto_disabled = bool(grp_st.get("auto_disabled", False))
            cooldown_until_iso = grp_st.get("cooldown_until")
            cooldown_until_dt  = _parse_iso(cooldown_until_iso)
            fresh = True  # wir setzen fresh=true; „stale“ markieren wir nur bei harten Fetch-Fehlern
            hard_error: Optional[str] = None

            # Overrides lesen
            ov_slot = _ensure_ovr_slot(overrides, pid, gid)
            forced_off = bool(ov_slot.get("forced_off", False))
            snooze_until_str = ov_slot.get("snooze_until")
            snooze_until_dt = _parse_iso(snooze_until_str)

            # Commands anwenden (einmalig)
            for cmd in cmds_by_pg.get((pid, gid), []):
                cmd_id = str(cmd.get("id") or "")
                if cmd_id in consumed_cmd_ids:
                    continue
                rearm = bool(cmd.get("rearm", True))
                rebaseline = bool(cmd.get("rebaseline", False))
                if rearm:
                    auto_disabled = False
                    cooldown_until_dt = now_dt  # sofort wieder scharf (Cooldown vorbei)
                    cooldown_until_iso = _now_iso()
                    if DEBUG_VALUES:
                        log.debug(f"[COMMAND] REARM applied pid={pid} gid={gid} -> auto_disabled=0, cooldown=now")
                if rebaseline:
                    # Evaluator kann hier rebaseline-hinweise weiterreichen (Baselines pflegst du in deiner Logik)
                    if DEBUG_VALUES:
                        log.debug(f"[COMMAND] REBASELINE requested pid={pid} gid={gid} (handled in indicator logic if applicable)")
                consumed_cmd_ids.add(cmd_id)

            # Skip inaktive Gruppen
            if not group.get("active", True):
                blockers.append("group_inactive")

            # Snooze/Forced-Off Blocker
            if forced_off:
                blockers.append("forced_off")
            if snooze_until_dt and now_dt < snooze_until_dt:
                blockers.append("snooze")

            # Cooldown/AutoDisabled Blocker
            if auto_disabled:
                blockers.append("auto_disabled")
            if cooldown_until_dt and now_dt < cooldown_until_dt:
                blockers.append("cooldown")

            # Symbole & Intervalle prüfen
            raw_symbols = [s for s in (group.get("symbols") or []) if s]
            symbols = list(dict.fromkeys(raw_symbols))
            main_interval = (group.get("interval") or "").strip()
            if (not symbols) or (not main_interval):
                blockers.append("misconfigured")
                # 👇 WICHTIG: auch im Fehlerfall ausreichende Infos bereitstellen
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
                    "runtime": {
                        "met": 0,
                        "total": len(group.get("conditions") or []),
                        "true_ticks": None,
                    },
                })
                continue

            # effective_active Vorprüfung (ohne stale/error)
            effective_active = bool(profile.get("enabled", True)) \
                               and bool(group.get("active", True)) \
                               and (not forced_off) \
                               and (not (snooze_until_dt and now_dt < snooze_until_dt)) \
                               and (not auto_disabled) \
                               and (not (cooldown_until_dt and now_dt < cooldown_until_dt))
            # Evaluate nur wenn effective prelim aktiv
            group_aggregate_passed = False
            notify_mode = _normalize_notify_mode(group)  # "always" | "true" | "any_true"
            min_ticks = _min_true_ticks_of(group)        # None => Gate nutzt 1

   

            per_symbol_evals: List[Dict[str, Any]] = []
            ts_list: List[Optional[str]] = []   # ← NEU

            if effective_active:
                for sym in symbols:
                    status_str, cond_details, bar_ts, err = _eval_group_for_symbol(meta, profile, group, sym, g_idx)
                    if err:
                        hard_error = err

                    # Live-Event bauen (geht an Gate)
                    ev = {
                        "profile_id":   pid,
                        "profile_name": profile.get("name"),
                        "group_id":     gid,
                        "group_index":  g_idx,
                        "group_name":   group.get("name") or f"group_{g_idx}",
                        "symbol":       sym,
                        "interval":     main_interval,
                        "exchange":     group.get("exchange") or None,
                        "telegram_bot_id": group.get("telegram_bot_id") or None,
                        "telegram_bot_token": group.get("telegram_bot_token") or None,
                        "telegram_chat_id": group.get("telegram_chat_id") or None,
                        "description":  group.get("description") or None,
                        "ts":           now_iso,
                        "bar_ts":       bar_ts or now_iso,
                        "status":       status_str,
                        "deactivate_on": notify_mode,
                        "min_true_ticks": min_ticks,
                        "conditions":   cond_details,
                    }
                    per_symbol_evals.append(ev)
                    evals.append(ev)
                    if bar_ts:
                        ts_list.append(bar_ts)  # ← NEU


                # Aggregate-Passed (wenn irgendein Symbol FULL/PARTIAL je nach Modus) – nur Info für Status
                group_aggregate_passed = any(
                    (ev["status"] == "FULL") or (notify_mode == "any_true" and ev["status"] in ("FULL", "PARTIAL"))
                    for ev in per_symbol_evals
                )
            else:
                if DEBUG_VALUES:
                    log.debug(f"[ACTIVE] skip evaluation pid={pid} gid={gid} due blockers={blockers}")
                # Label-only conditions in den Status (damit das UI etwas anzeigen kann)
                grp_st["conditions"] = _label_only_conditions(group)

            # stale/error Blocker berücksichtigen
            if hard_error:
                blockers.append("error")
                fresh = False

            # finale effective_active inkl. stale/error
            effective_active = effective_active and fresh and (len([b for b in blockers if b in ("forced_off","snooze","auto_disabled","cooldown","group_inactive","misconfigured")]) == 0)

            # met/total für UI bestimmen (Sample: erstes Symbol – reicht für Übersicht)
            met = total = 0
            if per_symbol_evals:
                try:
                    sample_conds = per_symbol_evals[0].get("conditions") or []
                    total = len(sample_conds)
                    met = sum(1 for c in sample_conds if c and bool(c.get("result")))
                except Exception:
                    pass
            else:
                # falls nicht evaluiert: total aus Definition
                total = len(group.get("conditions") or [])
                met = 0

            # true_ticks: Platzhalter, bis Gate den Streak-State bereitstellt
            true_ticks = None

            # Status der Gruppe schreiben
            grp_st.update({
                "name": (group.get("name") or f"group_{g_idx}"),
                "effective_active": bool(effective_active),
                "blockers": blockers,
                "auto_disabled": bool(auto_disabled),
                "cooldown_until": cooldown_until_iso,
                "fresh": bool(fresh),
                "last_eval_ts": now_iso,                         # bleibt als „Evaluationszeit“
                "last_bar_ts": _safe_max_iso(ts_list, None),     # ← NEU: Kerzenzeit für Anzeige
                "aggregate": {
                    "logic": "AND",
                    "passed": bool(group_aggregate_passed),
                    "notify_mode": notify_mode,
                    "min_true_ticks": min_ticks if min_ticks is not None else 1,
                },
                "runtime": {
                    "met": met,
                    "total": total,
                    "true_ticks": true_ticks,
                },
            })

            # ✨ NEU: Conditions-Zusammenfassung (inkl. Labels/Werten), wenn wir evaluiert haben
            try:
                if per_symbol_evals:
                    # nimm die erste Symbol-Row als sample
                    sample = per_symbol_evals[0]
                    conds: List[Dict[str, Any]] = []
                    for cd in (sample.get("conditions") or []):
                        left  = cd.get("left")  if isinstance(cd.get("left"), dict)  else {}
                        right = cd.get("right") if isinstance(cd.get("right"), dict) else {}
                        left  = left  or {}
                        right = right or {}

                        conds.append({
                            # Anzeige / Meta
                            "left":         left.get("label"),
                            "right":        right.get("label"),
                            "left_spec":    left.get("spec"),
                            "right_spec":   right.get("spec"),
                            "left_output":  left.get("output"),
                            "right_output": right.get("output"),
                            "left_col":     left.get("col"),
                            "right_col":    right.get("col"),

                            # Operator-Ergebnis
                            "op":     cd.get("op_norm") or cd.get("op"),
                            "passed": bool(cd.get("result")),

                            # Werte & Timing
                            "left_value":  left.get("value"),
                            "right_value": right.get("value"),
                            "left_ts":     left.get("ts"),
                            "right_ts":    right.get("ts"),
                            "eval_ms":     cd.get("duration_ms"),

                            # Fehler (falls vorhanden)
                            "error": cd.get("error"),
                        })
                    grp_st["conditions"] = conds
            except Exception as e:
                log.debug(f"[STATUS] conditions summary failed pid={pid} gid={gid}: {e}")

    # Gate anwenden (Streak + Modus-Logik)
    triggered = gate_and_build_triggers(evals)

    # Commands-Queue um konsumierte Items bereinigen
    if consumed_cmd_ids:
        new_queue = [it for it in (commands.get("queue") or []) if str(it.get("id") or "") not in consumed_cmd_ids]
        commands["queue"] = new_queue
        _save_commands(commands)
        if DEBUG_VALUES:
            log.debug(f"[COMMAND] consumed count={len(consumed_cmd_ids)} remaining_queue={len(new_queue)}")

    # Status-Version hochzählen und speichern
    status["version"] = int(prev_version) + 1
    _save_status(status)

    dt_total = (time.perf_counter() - t_start) * 1000.0
    log.info(f"Evals={len(evals)} → Trigger={len(triggered)} — Status v{status['version']} geschrieben — Laufzeit: {dt_total:.1f} ms")
    if triggered and DEBUG_VALUES:
        try:
            log.debug(json.dumps(triggered[:2], ensure_ascii=False, indent=2))
        except Exception:
            pass
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
    print(f"✅ {len(res)} Trigger(s) generiert.")
    if DEBUG_VALUES and res:
        print(json.dumps(res[:3], indent=2, ensure_ascii=False))
