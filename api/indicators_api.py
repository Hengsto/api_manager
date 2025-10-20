# api/indicators_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json
from typing import Any, Dict, Optional, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from fastapi import APIRouter, FastAPI, HTTPException, Query

from indicators.custom_registry import normalize_params_for_proxy, list_customs_for_ui

PRICE_API_BASE = os.getenv("PRICE_API_BASE", "http://127.0.0.1:8000").rstrip("/")
DEBUG = os.getenv("DEBUG", "1") not in ("0", "false", "False")


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
    return s


S = _session()
router = APIRouter()
# (Optional) Mini-App für Standalone-Betrieb/Tests
app = FastAPI(title="API Manager – Indicators Proxy", version="1.1.0")
app.include_router(router)


def _sj(d: Dict[str, Any]) -> str:
    return json.dumps(d, sort_keys=True, separators=(",", ":"))


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------
@router.get("/healthz")
def health():
    try:
        r = S.get(f"{PRICE_API_BASE}/intervals", timeout=5)
        ok = r.ok
    except Exception:
        ok = False
    return {"ok": True, "upstream_ok": ok, "upstream": PRICE_API_BASE}


# ------------------------------------------------------------------
# UI-Metadaten (mit display_name)
# ------------------------------------------------------------------
@router.get("/customs")
def customs(
    visibility: Optional[str] = Query(None, description="CSV: notifier,screener,input,source"),
    order_by: str = Query("sort_order", description="sort_order|name|display_name"),
    order: str = Query("asc", description="asc|desc"),
):
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

    return [_norm_row(x) for x in rows]


# ------------------------------------------------------------------
# Passthrough: chart/symbols/intervals/indicators
# ------------------------------------------------------------------
@router.get("/symbols")
def symbols():
    try:
        r = S.get(f"{PRICE_API_BASE}/symbols", timeout=10)
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            print(f"[PROXY][IND] /symbols -> {len(out)}")
        return out
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)


@router.get("/intervals")
def intervals():
    try:
        r = S.get(f"{PRICE_API_BASE}/intervals", timeout=10)
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            print(f"[PROXY][IND] /intervals -> {len(out)}")
        return out
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)


@router.get("/chart")
def chart(symbol: str, interval: str):
    try:
        r = S.get(f"{PRICE_API_BASE}/chart", params={"symbol": symbol, "interval": interval}, timeout=20)
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            print(f"[PROXY][IND] /chart {symbol=} {interval=} -> count={out.get('count')}")
        return out
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)


@router.get("/indicators")
def indicators():
    """
    Passthrough zum Upstream /indicators.
    Liefert die Liste verfügbarer Kern-Indikatoren (Name, Required Params, Outputs, etc.).
    """
    try:
        r = S.get(f"{PRICE_API_BASE}/indicators", timeout=15)
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            try:
                print(f"[PROXY][IND] /indicators -> {len(out)}")
            except Exception:
                print("[PROXY][IND] /indicators -> <unknown length>")
        return out
    except requests.HTTPError as e:
        # Upstream-Fehler roh durchreichen
        # (bewusst kein JSON-Rewrap hier, analog zu /symbols,/intervals,/chart)
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    
@router.get("/screener-data")
def screener_data(
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
    try:
        params: Dict[str, Any] = {}
        if distinct is not None:
            params["distinct"] = distinct
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if symbols:
            # Upstream akzeptiert in der Regel Wiederholung: symbols=AAPL&symbols=MSFT
            params["symbols"] = symbols
        if intervals:
            params["intervals"] = intervals

        r = S.get(f"{PRICE_API_BASE}/screener-data", params=params, timeout=15)
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            try:
                cnt = len(out.get("data") or out.get("rows") or [])
            except Exception:
                cnt = -1
            print(f"[PROXY][IND] /screener-data -> count={cnt}")
        return out
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    

@router.get("/signals")
def signals():
    """
    Passthrough zum Upstream /signals.
    Wird von get_indicator_catalog() benötigt, um die Signal-Spezifikationen zu laden.
    """
    try:
        r = S.get(f"{PRICE_API_BASE}/signals", timeout=15)
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            try:
                print(f"[PROXY][IND] /signals -> {len(out)}")
            except Exception:
                print("[PROXY][IND] /signals -> <unknown length>")
        return out
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)




# ------------------------------------------------------------------
# Proxy: CUSTOM
# ------------------------------------------------------------------
@router.get("/custom")
def custom(
    name: str = Query(..., description="z. B. slope | change | price | value"),
    symbol: str = Query(...),
    chart_interval: str = Query(...),
    indicator_interval: str = Query(...),
    params: str = Query("{}", description="JSON-Objekt (flach oder bereits shaped)"),
    count: Optional[int] = Query(None, ge=1),
):
    try:
        user_params = json.loads(params) if params else {}
        if not isinstance(user_params, dict):
            raise ValueError("params must be JSON object")
    except Exception as ex:
        raise HTTPException(status_code=422, detail=f"Invalid JSON in params: {ex}")

    shaped = normalize_params_for_proxy(name, user_params)

    if DEBUG:
        print(f"[PROXY][IN ] name={name} sym={symbol} chart={chart_interval} ind={indicator_interval}")
        print(f"[PROXY][RAW] { _sj(user_params) }")
        print(f"[PROXY][SHP] { _sj(shaped) }")

    try:
        r = S.get(
            f"{PRICE_API_BASE}/custom",
            params={
                "name": name,
                "symbol": symbol,
                "chart_interval": chart_interval,
                "indicator_interval": indicator_interval,
                "params": _sj(shaped),
                **({} if count is None else {"count": count}),
            },
            timeout=25,
        )
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            print(f"[PROXY][OUT] ok rows={out.get('count')} custom={out.get('custom')}")
        return out
    except requests.HTTPError as e:
        # Upstream-Fehler strukturiert durchreichen
        try:
            body = e.response.json()
        except Exception:
            body = {"status": e.response.status_code, "text": e.response.text[:400]}
        print(f"[PROXY][ERR] upstream {e.response.status_code}: {body}")
        raise HTTPException(status_code=e.response.status_code, detail=body)
    except Exception as e:
        import traceback
        print("[PROXY][ERR] internal\n" + traceback.format_exc(limit=20))
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


# ------------------------------------------------------------------
# Optional: direkte Passthroughs
# ------------------------------------------------------------------
@router.get("/indicator")
def indicator(
    name: str,
    symbol: str,
    chart_interval: str,
    indicator_interval: str,
    params: str,
    count: Optional[int] = Query(None, ge=1),
):
    try:
        r = S.get(
            f"{PRICE_API_BASE}/indicator",
            params={
                "name": name,
                "symbol": symbol,
                "chart_interval": chart_interval,
                "indicator_interval": indicator_interval,
                "params": params,
                **({} if count is None else {"count": count}),
            },
            timeout=25,
        )
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            print(f"[PROXY][IND] /indicator {name=} -> count={out.get('count')}")
        return out
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)


@router.get("/signal")
def signal(
    name: str,
    symbol: str,
    chart_interval: str,
    indicator_interval: str,
    params: str = "{}",
    count: Optional[int] = Query(None, ge=1),
):
    try:
        r = S.get(
            f"{PRICE_API_BASE}/signal",
            params={
                "name": name,
                "symbol": symbol,
                "chart_interval": chart_interval,
                "indicator_interval": indicator_interval,
                "params": params,
                **({} if count is None else {"count": count}),
            },
            timeout=25,
        )
        r.raise_for_status()
        out = r.json()
        if DEBUG:
            print(f"[PROXY][IND] /signal {name=} -> count={out.get('count')}")
        return out
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)


# ---- Optional: lokaler Start nur dieses Proxys ------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.indicators_api:app", host="127.0.0.1", port=int(os.getenv("IND_PROXY_PORT", "8097")), reload=False)
