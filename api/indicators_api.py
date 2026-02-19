# api/indicators_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, time, uuid
from typing import Any, Dict, Optional, Tuple, List
import threading

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import PRICE_API_ENDPOINT


from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from indicators.custom_registry import normalize_params_for_proxy, list_customs_for_ui
# Zentrale Normalisierung für Chart-Payloads (einmalig, DRY)
from indicators._utils import normalize_chart_df

# ──────────────────────────────────────────────────────────────────────────────
# Konfiguration
# ──────────────────────────────────────────────────────────────────────────────
DEBUG = os.getenv("DEBUG", "1") not in ("0", "false", "False")
DEFAULT_TIMEOUT = float(os.getenv("IND_PROXY_TIMEOUT", "20"))

PRICE_API_BASE = str(PRICE_API_ENDPOINT).rstrip("/")
if DEBUG:
    print(f"[BOOT][INDPROXY] PRICE_API_BASE(from config.PRICE_API_ENDPOINT)={PRICE_API_BASE!r}")


# Kleine TTLs für häufige, kleine Endpoints
SMALL_TTL = float(os.getenv("IND_PROXY_SMALL_TTL", "5.0"))  # /symbols, /intervals, /indicators

# Maximal erlaubtes count (DoS-Schutz)
MAX_COUNT = int(os.getenv("IND_PROXY_MAX_COUNT", "5000"))

# Per-Route-Timeouts (sekunden)
TO_CHART = float(os.getenv("IND_TO_CHART", "20"))
TO_INDICATORS = float(os.getenv("IND_TO_INDICATORS", "15"))
TO_SYMBOLS = float(os.getenv("IND_TO_SYMBOLS", "5"))
TO_INTERVALS = float(os.getenv("IND_TO_INTERVALS", "5"))
TO_INDICATOR = float(os.getenv("IND_TO_INDICATOR", "25"))
TO_SIGNAL = float(os.getenv("IND_TO_SIGNAL", "25"))
TO_CUSTOM = float(os.getenv("IND_TO_CUSTOM", "25"))
TO_SCREENER = float(os.getenv("IND_TO_SCREENER", "15"))

PROXY_NAME = "IndicatorsProxy"
PROXY_VERSION = "1.4.0"  # kombiniert

# ──────────────────────────────────────────────────────────────────────────────
# Requests-Session mit Retries
# ──────────────────────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.25,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": f"{PROXY_NAME}/{PROXY_VERSION} (+api-manager)",
        "Accept": "application/json",
        "X-Proxy-Name": PROXY_NAME,
        "X-Proxy-Version": PROXY_VERSION,
    })
    return s

S = _session()
router = APIRouter()

# (Optional) Mini-App für Standalone-Betrieb/Tests
app = FastAPI(title="API Manager – Indicators Proxy", version=PROXY_VERSION)

# CORS – ohne Credentials, damit "*" gültig ist
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# GZip spart bei langen rows/columns Bandbreite
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ──────────────────────────────────────────────────────────────────────────────
# Mini-TTL-Cache für kleine, häufige GETs (Thread-safe)
# ──────────────────────────────────────────────────────────────────────────────
_cache_lock = threading.Lock()
_cache: Dict[str, Tuple[float, Any]] = {}

def _cache_get(key: str) -> Optional[Any]:
    with _cache_lock:
        hit = _cache.get(key)
        if not hit:
            return None
        ts, val = hit
        if (time.time() - ts) <= SMALL_TTL:
            return val
        _cache.pop(key, None)
        return None

def _cache_set(key: str, val: Any) -> None:
    with _cache_lock:
        _cache[key] = (time.time(), val)

# ──────────────────────────────────────────────────────────────────────────────
# Utils
# ──────────────────────────────────────────────────────────────────────────────
def _sj(d: Dict[str, Any]) -> str:
    return json.dumps(d, sort_keys=True, separators=(",", ":"))

def _new_req_id(client_req: Optional[str]) -> str:
    try:
        if client_req and len(client_req) <= 64:
            return client_req
    except Exception:
        pass
    return uuid.uuid4().hex[:8]

def _parse_json_or_raise(resp: requests.Response) -> Any:
    """
    Saubere JSON-Verarbeitung: Wenn Upstream keinen JSON liefert → 502 mit Text-Snippet.
    Wenn Upstream-Status !ok mit JSON → strukturiert durchreichen.
    """
    try:
        data = resp.json()
    except ValueError:
        text_snip = (resp.text or "")[:400]
        if DEBUG:
            try:
                print(f"[PROXY][IND][ERR] upstream_non_json status={resp.status_code} text={text_snip!r}")
            except Exception:
                pass
        raise HTTPException(status_code=502, detail={"status": resp.status_code, "text": text_snip})

    if not resp.ok:
        if DEBUG:
            try:
                body_snip = (resp.text or "")[:3000]
                print(f"[PROXY][IND][ERR] upstream_error status={resp.status_code} body={body_snip}")
            except Exception as e:
                print(f"[PROXY][IND][ERR] upstream_error body_read_failed: {type(e).__name__}: {e}")

        # Upstream-Fehler JSON bleibt erhalten
        raise HTTPException(status_code=resp.status_code, detail=data)

    return data


def _dbg_out_preview(label: str, payload: Dict[str, Any], req_id: str = "-") -> None:
    if not DEBUG:
        return
    try:
        cols = payload.get("columns")
        rows = payload.get("rows")
        data = payload.get("data")  # manche Upstreams nutzen 'data'
        cnt = payload.get("count")

        # Fallbacks
        rows_like = rows if isinstance(rows, list) else (data if isinstance(data, list) else [])
        rows_len = len(rows_like) if isinstance(rows_like, list) else None

        # Timestamps aus erster/letzter Zeile
        ts_first = ts_last = None
        if rows_like and isinstance(rows_like[0], dict):
            ts_first = rows_like[0].get("Timestamp") or rows_like[0].get("timestamp") or rows_like[0].get("Timestamp_ISO")
            last = rows_like[-1]
            if isinstance(last, dict):
                ts_last = last.get("Timestamp") or last.get("timestamp") or last.get("Timestamp_ISO")

        # Beispiel (erste Zeile ohne Timestamp)
        sample = None
        if rows_like and isinstance(rows_like[0], dict):
            first = dict(rows_like[0])
            for k in list(first.keys()):
                if str(k).lower().startswith("timestamp"):
                    first.pop(k, None)
            sample = {k: first[k] for k in list(first.keys())[:5]}

        if isinstance(cols, list) and len(cols) > 20:
            cols_log = cols[:20] + ["…", f"+{len(cols)-20} more"]
        else:
            cols_log = cols if isinstance(cols, list) else "<none>"

        print(
            f"[PROXY][IND][{req_id}] {label} OUT "
            f"count={cnt} rows_len={rows_len} columns={cols_log} "
            f"ts_first={ts_first} ts_last={ts_last} sample={sample}"
        )
    except Exception as e:
        print(f"[PROXY][IND][{req_id}] {label} OUT <debug-failed> reason={type(e).__name__}: {e}")

def _get_upstream(
    path: str, *,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = DEFAULT_TIMEOUT,
    req_id: str = "-"
) -> Any:
    url = f"{PRICE_API_BASE}{path}"
    t0 = time.time()
    try:
        if DEBUG:
            pview = _sj(params or {}) if params else "-"
            pv = pview if len(pview) <= 400 else (pview[:400] + "…")
            print(f"[PROXY][IND][{req_id}] GET {url} params={pv}")
        r = S.get(url, params=params, timeout=timeout, headers={"X-Proxy-Req-ID": req_id})
        data = _parse_json_or_raise(r)
        if DEBUG:
            dt = (time.time() - t0) * 1000.0
            print(f"[PROXY][IND][{req_id}] GET {url} status={r.status_code} dt_ms={dt:.1f}")
        return data
    except HTTPException:
        raise
    except requests.RequestException as e:
        if DEBUG:
            dt = (time.time() - t0) * 1000.0
            print(f"[PROXY][IND][{req_id}] GET {url} REXC {type(e).__name__}: {e} dt_ms={dt:.1f}")
        raise HTTPException(status_code=502, detail=str(e))

def _coerce_params_types(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Wandelt stringifizierte Zahlen/Bools in echte Typen um: "22"->22, "3.14"->3.14, "true"->True.
    Vorsichtig: nur flache Dicts – genau richtig für unsere Indicator-Params.
    """
    out: Dict[str, Any] = {}
    for k, v in (d or {}).items():
        if isinstance(v, str):
            s = v.strip()
            # Bool
            if s.lower() in ("true", "false"):
                out[k] = (s.lower() == "true")
                continue
            # Zahl
            try:
                if any(c in s for c in (".", "e", "E")):
                    out[k] = float(s)
                else:
                    out[k] = int(s)
                continue
            except Exception:
                pass
        out[k] = v
    return out

def _inject_default_source_if_missing(ind_name: str, p: Dict[str, Any], req_id: str = "-") -> Dict[str, Any]:
    """
    Upstream-Kompatibilität:
    Wenn 'source' fehlt, default auf 'Close'.
    """
    lname = (ind_name or "").strip().lower()
    if not isinstance(p, dict):
        return p

    if "source" not in p or p.get("source") in (None, "", "null"):
        p["source"] = "Close"
        if DEBUG:
            print(f"[PROXY][IND][{req_id}] injected default source='Close' for name={lname}")
    return p


def _cap_count(n: Optional[int]) -> Optional[int]:
    if n is None:
        return None
    try:
        iv = int(n)
    except Exception:
        return None
    return max(1, min(iv, MAX_COUNT))

# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────
@router.get("/healthz")
def health(request: Request):
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    t0 = time.time()
    try:
        r = S.get(f"{PRICE_API_BASE}/intervals", timeout=DEFAULT_TIMEOUT, headers={"X-Proxy-Req-ID": req_id})
        upstream_ok = r.ok
        upstream_status = r.status_code
    except Exception:
        upstream_ok = False
        upstream_status = None
    dt = (time.time() - t0) * 1000.0
    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /healthz -> upstream_ok={upstream_ok} status={upstream_status} upstream={PRICE_API_BASE} dt_ms={dt:.1f}")
    return {"ok": True, "upstream_ok": upstream_ok, "status": upstream_status, "upstream": PRICE_API_BASE, "dt_ms": dt}

# ──────────────────────────────────────────────────────────────────────────────
# UI-Metadaten (mit display_name)
# ──────────────────────────────────────────────────────────────────────────────
@router.get("/customs")
def customs(
    request: Request,
    visibility: Optional[str] = Query(None, description="CSV: notifier, screener, input, source"),
    order_by: str = Query("sort_order", description="sort_order|name|display_name"),
    order: str = Query("asc", description="asc|desc"),
):
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    if order_by not in ("sort_order", "name", "display_name"):
        raise HTTPException(status_code=422, detail="order_by must be one of: sort_order,name,display_name")
    if order.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=422, detail="order must be one of: asc,desc")

    vis = [v.strip() for v in visibility.split(",")] if visibility else None
    rows = list_customs_for_ui(
        visibility=vis,
        order_by=order_by,
        desc=(order.lower() == "desc"),
    )

    def _norm_row(r: Dict[str, Any]) -> Dict[str, Any]:
        d = dict(r or {})

        # required_params: list|str -> dict
        rp = d.get("required_params")
        if isinstance(rp, list):
            rp = {str(k): "string" for k in rp}
        elif isinstance(rp, str):
            rp = {rp: "string"}
        elif not isinstance(rp, dict) or rp is None:
            rp = {}
        d["required_params"] = rp

        # default_params: ensure dict
        dp = d.get("default_params")
        if not isinstance(dp, dict) or dp is None:
            dp = {}
        d["default_params"] = dp

        # outputs: ensure list[str]
        outs = d.get("outputs")
        if outs is None:
            outs = []
        elif isinstance(outs, str):
            outs = [outs]
        d["outputs"] = outs

        return d

    out = [_norm_row(x) for x in rows]
    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /customs -> {len(out)}")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Passthrough: chart/symbols/intervals/indicators (+ Cache)
# ──────────────────────────────────────────────────────────────────────────────
@router.get("/symbols")
def symbols(request: Request):
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    cache_key = "symbols"
    cached = _cache_get(cache_key)
    if cached is not None:
        if DEBUG:
            try: print(f"[PROXY][IND][{req_id}] /symbols -> cache-hit ({len(cached)})")
            except Exception: print(f"[PROXY][IND][{req_id}] /symbols -> cache-hit")
        return cached
    out = _get_upstream("/symbols", req_id=req_id, timeout=TO_SYMBOLS)
    _cache_set(cache_key, out)
    if DEBUG:
        try:
            print(f"[PROXY][IND][{req_id}] /symbols -> {len(out)}")
        except Exception:
            print(f"[PROXY][IND][{req_id}] /symbols -> <unknown length>")
    return out

@router.get("/intervals")
def intervals(request: Request):
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    cache_key = "intervals"
    cached = _cache_get(cache_key)
    if cached is not None:
        if DEBUG:
            try: print(f"[PROXY][IND][{req_id}] /intervals -> cache-hit ({len(cached)})")
            except Exception: print(f"[PROXY][IND][{req_id}] /intervals -> cache-hit")
        return cached
    out = _get_upstream("/intervals", req_id=req_id, timeout=TO_INTERVALS)
    _cache_set(cache_key, out)
    if DEBUG:
        try:
            print(f"[PROXY][IND][{req_id}] /intervals -> {len(out)}")
        except Exception:
            print(f"[PROXY][IND][{req_id}] /intervals -> <unknown length>")
    return out

@router.get("/chart")
def chart(
    request: Request,
    symbol: str,
    interval: str,
    count: Optional[int] = Query(None, ge=1),
):
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    capped_count = _cap_count(count)

    params: Dict[str, Any] = {"symbol": symbol, "interval": interval}
    if capped_count is not None:
        params["count"] = capped_count

    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /chart IN params={params}")

    out = _get_upstream("/chart", params=params, req_id=req_id, timeout=TO_CHART)

    if DEBUG:
        _dbg_out_preview("/chart", out, req_id=req_id)

    return out


@router.get("/indicators")
def indicators(request: Request):
    """
    Passthrough zum Upstream /indicators.
    Liefert die Liste verfügbarer Kern-Indikatoren (Name, Required Params, Outputs, etc.).
    """
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    cache_key = "indicators"
    cached = _cache_get(cache_key)
    if cached is not None:
        if DEBUG:
            print(f"[PROXY][IND][{req_id}] /indicators -> cache-hit")
        return cached
    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /indicators -> upstream {PRICE_API_BASE}/indicators")
    out = _get_upstream("/indicators", req_id=req_id, timeout=TO_INDICATORS)
    _cache_set(cache_key, out)
    if DEBUG:
        try:
            ln = len(out) if hasattr(out, "__len__") else "<n/a>"
        except Exception:
            ln = "<n/a>"
        print(f"[PROXY][IND][{req_id}] /indicators OUT len={ln}")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Zusätzliche Passthroughs: Screener & Signals
# ──────────────────────────────────────────────────────────────────────────────
@router.get("/screener-data")
def screener_data(
    request: Request,
    distinct: Optional[str] = Query(None),
    limit: Optional[int] = Query(None, ge=1),
    offset: Optional[int] = Query(None, ge=0),
    symbols: Optional[List[str]] = Query(None),
    intervals: Optional[List[str]] = Query(None),
):
    """
    Passthrough für /screener-data – wird u.a. von get_symbols() und latest_values() genutzt.
    Wir geben die Query-Parameter 1:1 an den Upstream weiter.
    """
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    params: Dict[str, Any] = {}
    if distinct is not None:
        params["distinct"] = distinct
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    if symbols:
        params["symbols"] = symbols
    if intervals:
        params["intervals"] = intervals

    if DEBUG:
        p = _sj(params) if params else "-"
        print(f"[PROXY][IND][{req_id}] /screener-data IN params={p}")

    out = _get_upstream("/screener-data", params=params, timeout=TO_SCREENER, req_id=req_id)
    if DEBUG:
        try:
            cnt = len(out.get("data") or out.get("rows") or [])
        except Exception:
            cnt = -1
        print(f"[PROXY][IND][{req_id}] /screener-data OUT count={cnt}")
    return out

@router.get("/signals")
def signals(request: Request):
    """
    Passthrough zum Upstream /signals.
    Fallback: Wenn 404, liefere [] (verhindert UI-Fehler-Spam).
    """
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /signals -> upstream {PRICE_API_BASE}/signals")
    try:
        out = _get_upstream("/signals", timeout=TO_INDICATORS, req_id=req_id)
    except HTTPException as e:
        if int(getattr(e, "status_code", 0) or 0) == 404:
            if DEBUG:
                print(f"[PROXY][IND][{req_id}] /signals upstream=404 -> returning [] (fallback)")
            return []
        raise
    if DEBUG:
        try:
            ln = len(out) if hasattr(out, "__len__") else "<n/a>"
        except Exception:
            ln = "<n/a>"
        print(f"[PROXY][IND][{req_id}] /signals OUT len={ln}")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Local fallback for /custom (value, price, slope, change)
# ──────────────────────────────────────────────────────────────────────────────
from cachetools import TTLCache
from threading import RLock

_local_chart_cache = TTLCache(maxsize=64, ttl=SMALL_TTL)

_local_chart_lock = threading.RLock()


def _get_chart_cached_local(symbol: str, interval: str, req_id: str, count: Optional[int] = None):
    capped_count = _cap_count(count)

    # Cache-Key MUSS count enthalten, sonst kriegst du falsche Treffer (full vs small)
    key = (PRICE_API_BASE, symbol, interval, capped_count)

    with _local_chart_lock:
        if key in _local_chart_cache:
            if DEBUG:
                print(f"[PROXY][IND][{req_id}] chart LOCAL cache-hit {key}")
            return _local_chart_cache[key]

    params: Dict[str, Any] = {"symbol": symbol, "interval": interval}
    if capped_count is not None:
        params["count"] = capped_count

    data = _get_upstream(
        "/chart",
        params=params,
        timeout=TO_CHART,
        req_id=req_id,
    )

    if DEBUG:
        try:
            rows = (data or {}).get("data") or (data or {}).get("rows") or []
            cnt = int((data or {}).get("count") or (len(rows) if isinstance(rows, list) else -1))
        except Exception as e:
            cnt = -1
            print(f"[PROXY][IND][{req_id}] chart LOCAL store cnt parse failed: {type(e).__name__}: {e}")
        print(f"[PROXY][IND][{req_id}] chart LOCAL store {key} cnt={cnt}")

    with _local_chart_lock:
        _local_chart_cache[key] = data

    return data



def _local_compute_custom(
    name: str,
    symbol: str,
    chart_interval: str,
    indicator_interval: str,  # derzeit ungenutzt im Fallback, aber behalten
    shaped_params: Dict[str, Any],
    count: Optional[int] = None,
    req_id: str = "-",
) -> Dict[str, Any]:
    """
    Lokaler Fallback für /custom.
    Unterstützt: value, price, slope, change.
    Holt Chart-Daten (für value mit – damit count & Zeitachse konsistent sind),
    normalisiert Spalten → ruft lokale Indicator-Implementierungen auf.
    """
    lname = (name or "").strip().lower()
    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /custom LOCAL DISPATCH name={lname} shaped={shaped_params}")

    # 1) Chart holen (für alle, inkl. value; bei value gibt es optionalen Synthetic-Fallback)
    # Heuristik: wenn der Client nur 5 will, brauchst du für price/value nicht mehr.
    # Für echte Indikatoren ggf. später Lookback hochziehen (z.B. 200).
    chart = _get_chart_cached_local(symbol, chart_interval, req_id, count=count)



    # 2) Einheitlich via Utils normalisieren
    try:
        df = normalize_chart_df(chart)
    except Exception as e:
        raise HTTPException(status_code=424, detail={"error": "chart_normalize_failed", "reason": str(e)})

    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /custom LOCAL df.shape={df.shape} cols={list(df.columns)[:12]}")

    # 3) Dispatch (dynamisch über Registry)
    try:
        from importlib import import_module
        from indicators.custom_registry import get_custom_exec

        module_name, fn_name = get_custom_exec(lname)
        if DEBUG:
            print(f"[PROXY][IND][{req_id}] /custom LOCAL dyn={module_name}.{fn_name} shaped_keys={list(shaped_params.keys())}")

        mod = import_module(module_name)
        fn = getattr(mod, fn_name)

        # --- Hidden Base-Injection für HTTP-Fallbacks in lokalen Indikatoren ---
        try:
            if isinstance(shaped_params, dict) and "base" in shaped_params:
                bp = dict(shaped_params.get("base_params") or {})
                # Nicht überschreiben, nur setzen wenn nicht vorhanden:
                bp.setdefault("_symbol", symbol)
                bp.setdefault("_chart_interval", chart_interval)
                bp.setdefault("_indicator_interval", indicator_interval)
                shaped_params["base_params"] = bp
                if DEBUG:
                    print(f"[PROXY][IND][{req_id}] injected base_params keys={sorted(list(bp.keys()))}")
        except Exception as e:
            if DEBUG:
                print(f"[PROXY][IND][{req_id}] base_params injection failed: {type(e).__name__}: {e}")

        # Zwei gängige Call-Konventionen unterstützen:
        # 1) fn(df, **kwargs)
        # 2) fn(df, params_dict)  (z. B. value.compute)
        try:
            out_df, used, out_cols = fn(df, **shaped_params)
        except TypeError as te:
            if DEBUG:
                print(f"[PROXY][IND][{req_id}] kwargs-call failed ({te}); trying dict-call")
            out_df, used, out_cols = fn(df, shaped_params)


    except KeyError:
        # Unbekannt in Registry
        raise HTTPException(status_code=501, detail={"error": f"Local custom '{lname}' not supported"})
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        tb = traceback.format_exc(limit=10)
        print(f"[PROXY][ERR][{req_id}] dispatch {lname} failed: {e}\n{tb}")
        raise HTTPException(status_code=500, detail={"error": f"{lname} failed", "type": type(e).__name__, "reason": str(e), "trace": tb})


    # 4) Ausgabe
    if not isinstance(out_df, pd.DataFrame) or "Timestamp" not in out_df.columns:
        raise HTTPException(status_code=500, detail={"error": "Local compute produced invalid DataFrame (no Timestamp)"})

    if count is not None and isinstance(count, int) and count > 0:
        out_df = out_df.tail(count)

    out = {
        "custom": lname,
        "count": int(len(out_df)),
        "columns": list(out_df.columns),
        "rows": out_df.to_dict("records"),
        "used": used,
    }
    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /custom LOCAL OUT rows={out['count']} columns={out['columns']}")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# STRICT PROXY: /custom (mit Fallback-Kaskade & Typ-Coercion & Count-Cap)
# ──────────────────────────────────────────────────────────────────────────────
@router.get("/custom")
def custom(
    request: Request,
    name: str = Query(..., description="z. B. slope | change | price | value"),
    symbol: str = Query(...),
    chart_interval: str = Query(...),
    indicator_interval: str = Query(...),
    params: str = Query("{}", description="JSON-Objekt (flach oder bereits shaped)"),
    count: Optional[int] = Query(None, ge=1),
):
    req_id = _new_req_id(request.headers.get("X-Request-ID"))

    # Eingangs-JSON validieren/parsen
    try:
        user_params = json.loads(params) if params else {}
        if not isinstance(user_params, dict):
            raise ValueError("params must be JSON object")
    except Exception as ex:
        raise HTTPException(status_code=422, detail=f"Invalid JSON in params: {ex}")

    shaped = normalize_params_for_proxy(name, user_params)
    capped_count = _cap_count(count)

    if DEBUG:
        p_raw = _sj(user_params)
        p_shp = _sj(shaped)
        p_raw = p_raw if len(p_raw) <= 400 else (p_raw[:400] + "…")
        p_shp = p_shp if len(p_shp) <= 400 else (p_shp[:400] + "…")
        print(f"[PROXY][IN ][{req_id}] /custom name={name} sym={symbol} chart={chart_interval} ind={indicator_interval} count={capped_count}")
        print(f"[PROXY][RAW][{req_id}] {p_raw}")
        print(f"[PROXY][SHP][{req_id}] {p_shp}")

    # Upstream-Call vorbereiten
    query: Dict[str, Any] = {
        "name": name,
        "symbol": symbol,
        "chart_interval": chart_interval,
        "indicator_interval": indicator_interval,
        "params": _sj(shaped),
    }
    if capped_count is not None:
        query["count"] = capped_count

    if DEBUG:
        print(f"[PROXY][IND][{req_id}] /custom -> upstream GET {PRICE_API_BASE}/custom")

    # --- Short-circuit: lokale Customs IMMER lokal berechnen ---
    # Diese Namen sind bei dir bewusst "custom" und NICHT Upstream-Indikatoren.
    LOCAL_ONLY = {"price", "value", "slope", "change"}

    lname = (name or "").strip().lower()
    if lname in LOCAL_ONLY:
        if DEBUG:
            print(f"[PROXY][IND][{req_id}] /custom local-only hit -> computing locally (skip upstream)")
        return _local_compute_custom(
            name=lname,
            symbol=symbol,
            chart_interval=chart_interval,
            indicator_interval=indicator_interval,
            shaped_params=shaped,
            count=capped_count,
            req_id=req_id,
        )

    # --- Für alles andere: Upstream /custom versuchen (falls du später echten Upstream hast) ---
    try:
        out = _get_upstream("/custom", params=query, req_id=req_id, timeout=TO_CUSTOM)
        if DEBUG:
            rows = out.get("count")
            print(f"[PROXY][OUT][{req_id}] /custom ok rows={rows} custom={out.get('custom')}")
        return out
    except HTTPException as e:
        # Wenn Upstream /custom nicht existiert, ist das ein Setup-Fehler.
        # Du kannst hier entscheiden: 404 -> direkt 501 statt Chaos-Fallback.
        status = int(getattr(e, "status_code", 0) or 0)
        if status == 404:
            raise HTTPException(status_code=501, detail={"error": "upstream_custom_missing", "upstream": PRICE_API_BASE})
        raise

# ──────────────────────────────────────────────────────────────────────────────
# Passthrough: Indicator & Signal (mit Output-Debug + Count-Cap + Param-Coercion)
# ──────────────────────────────────────────────────────────────────────────────
@router.get("/indicator")
def indicator(
    request: Request,
    name: str,
    symbol: str,
    chart_interval: str,
    indicator_interval: str,
    params: str,
    count: Optional[int] = Query(None, ge=1),

    # NEW: evaluator sends these; upstream may require them
    exchange: Optional[str] = Query(None),
    output: Optional[str] = Query(None),
    mode: Optional[str] = Query(None),
    as_of: Optional[str] = Query(None),
):

    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    capped_count = _cap_count(count)

    lname = (name or "").strip().lower()

    # Eingangs-Logging inkl. kompaktem Params-Preview
    _params_preview = params if len(str(params)) <= 400 else (str(params)[:400] + "…")
    if DEBUG:
        print(
            f"[PROXY][IN ][{req_id}] /indicator "
            f"name={name} sym={symbol} chart={chart_interval} ind={indicator_interval} "
            f"count={capped_count} ex={exchange} mode={mode} as_of={as_of} output={output} "
            f"params={_params_preview}"
        )

    # Wenn es nach JSON aussieht → parse & coerzen → wieder dumpen
    p_dict: Dict[str, Any] = {}
    try:
        if params and str(params).strip().startswith(("{", "[")):
            loaded = json.loads(params)

            if isinstance(loaded, dict):
                p_dict = _coerce_params_types(loaded)

                # Default source, wenn nicht gesetzt (Upstream erwartet es)
                try:
                    p_dict = _inject_default_source_if_missing(name, p_dict, req_id=req_id)
                except Exception as e:
                    if DEBUG:
                        print(f"[PROXY][IND][{req_id}] default-source inject failed: {type(e).__name__}: {e}")

                params = _sj(p_dict)

            else:
                # keep original for upstream; but log it
                if DEBUG:
                    print(f"[PROXY][IN ][{req_id}] /indicator params is not dict type={type(loaded).__name__}")
    except Exception as ex:
        raise HTTPException(status_code=422, detail=f"Invalid JSON in params: {ex}")

    # --- Short-circuit: lokale Customs IMMER lokal berechnen ---
    # Diese Namen sind bei dir bewusst "custom" und NICHT Upstream-Indikatoren.
    LOCAL_ONLY = {"price", "value", "slope", "change"}

    if lname in LOCAL_ONLY:
        if DEBUG:
            print(f"[PROXY][IND][{req_id}] /indicator local-only hit -> computing locally (skip upstream)")

        shaped = normalize_params_for_proxy(lname, p_dict)

        out_local = _local_compute_custom(
            name=lname,
            symbol=symbol,
            chart_interval=chart_interval,
            indicator_interval=indicator_interval,
            shaped_params=shaped,
            count=capped_count,
            req_id=req_id,
        )

        # Make it look like an /indicator response (harmless extra keys)
        out_local["name"] = lname
        out_local["indicator"] = lname
        out_local["output"] = output
        out_local["exchange"] = exchange
        out_local["mode"] = mode
        out_local["as_of"] = as_of

        _dbg_out_preview("/indicator(local)", out_local, req_id=req_id)
        return out_local

    # --- Upstream call ---
    query: Dict[str, Any] = {
        "name": name,
        "symbol": symbol,
        "chart_interval": chart_interval,
        "indicator_interval": indicator_interval,
        "params": params,
    }
    if capped_count is not None:
        query["count"] = capped_count

    # Forward evaluator/upstream knobs (only if provided)
    if exchange:
        query["exchange"] = exchange
    if output:
        query["output"] = output
    if mode:
        query["mode"] = mode
    if as_of:
        query["as_of"] = as_of

    out = _get_upstream("/indicator", params=query, req_id=req_id, timeout=TO_INDICATOR)
    _dbg_out_preview("/indicator", out, req_id=req_id)
    return out


@router.get("/signal")
def signal(
    request: Request,
    name: str,
    symbol: str,
    chart_interval: str,
    indicator_interval: str,
    params: str = "{}",
    count: Optional[int] = Query(None, ge=1),
):
    req_id = _new_req_id(request.headers.get("X-Request-ID"))
    capped_count = _cap_count(count)

    # (Sanft) validieren + coerzen
    try:
        if params and str(params).strip().startswith(("{", "[")):
            p_dict = json.loads(params)
            if isinstance(p_dict, dict):
                p_dict = _coerce_params_types(p_dict)
                params = _sj(p_dict)
    except Exception as ex:
        raise HTTPException(status_code=422, detail=f"Invalid JSON in params: {ex}")

    out = _get_upstream(
        "/signal",
        params={
            "name": name,
            "symbol": symbol,
            "chart_interval": chart_interval,
            "indicator_interval": indicator_interval,
            "params": params,
            **({} if capped_count is None else {"count": capped_count}),
        },
        req_id=req_id,
        timeout=TO_SIGNAL,
    )
    if DEBUG:
        _dbg_out_preview("/signal", out, req_id=req_id)
    return out

# Router ERST NACH allen Endpoints einhängen
app.include_router(router)

# ---- Optional: lokaler Start nur dieses Proxys ------------------------------
if __name__ == "__main__":
    import uvicorn
    host = os.getenv("IND_PROXY_HOST", "127.0.0.1")
    port = int(os.getenv("IND_PROXY_PORT", "8097"))
    print(f"### Starte Indicators Proxy auf http://{host}:{port} -> Upstream {PRICE_API_BASE}")
    uvicorn.run("api.indicators_api:app", host=host, port=port, reload=False)
