# notifier_evaluator/fetch/client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from notifier_evaluator.fetch.types import RequestKey, normalize_indicator_response
from notifier_evaluator.models.runtime import FetchResult


# ──────────────────────────────────────────────────────────────────────────────
# HTTP Client wrapper
# - macht NUR HTTP + Basic-Param-Building
# - keine Planner/Cache/Eval/Policy-Logik
#
# Erwarteter Endpoint (Default):
#   GET {base_url}/indicator
#
# Query Params (typisch in deinem Stack):
#   name=<indicator>
#   symbol=<symbol>
#   chart_interval=<interval>
#   indicator_interval=<interval>
#   params=<json>
#   output=<output>
#   count=<count>
# Optional:
#   exchange=<exchange>
#   mode=<latest|as_of>
#   as_of=<ts>
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class ClientConfig:
    base_url: str
    timeout_sec: int = 10
    retries: int = 2
    backoff: float = 0.3
    verify_ssl: bool = True

    # Wenn dein API andere Param-Namen will, kannst du hier später umstellen
    endpoint_indicator: str = "/indicator"


class IndicatorClient:
    def __init__(self, cfg: ClientConfig, session: Optional[requests.Session] = None):
        self.cfg = cfg
        self.session = session or self._make_session(cfg)

        # Normalize base_url
        self.base_url = (cfg.base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("[fetch.client] base_url is empty")

        print(
            "[fetch.client] init base_url=%s timeout=%ss retries=%d backoff=%.3f verify_ssl=%s endpoint=%s"
            % (self.base_url, cfg.timeout_sec, cfg.retries, cfg.backoff, cfg.verify_ssl, cfg.endpoint_indicator)
        )

    def _make_session(self, cfg: ClientConfig) -> requests.Session:
        s = requests.Session()

        # urllib3 Retry config
        retry = Retry(
            total=max(0, int(cfg.retries)),
            connect=max(0, int(cfg.retries)),
            read=max(0, int(cfg.retries)),
            status=max(0, int(cfg.retries)),
            backoff_factor=float(cfg.backoff),
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        return s

    def fetch_indicator(self, key: RequestKey) -> FetchResult:
        """
        Führt HTTP Request aus und gibt *normalisierte* FetchResult zurück.
        """
        req_id = uuid.uuid4().hex[:8]
        url = self._build_url()
        params = self._build_params(key)

        t0 = time.time()
        print("[fetch.client] REQ id=%s GET %s key=%s params=%s" % (req_id, url, key.short(), _short_params(params)))

        try:
            r = self.session.get(
                url,
                params=params,
                timeout=self.cfg.timeout_sec,
                verify=self.cfg.verify_ssl,
            )
        except Exception as e:
            dt = time.time() - t0
            print("[fetch.client] EXC id=%s key=%s dt=%.3fs err=%s" % (req_id, key.short(), dt, e))
            return normalize_indicator_response(
                {
                    "ok": False,
                    "error": f"request_exc:{e}",
                    "rows": [],
                    "_http": {"req_id": req_id, "elapsed_sec": dt, "url": url},
                },
                key=key,
            )

        dt = time.time() - t0
        clen = r.headers.get("Content-Length")
        print(
            "[fetch.client] RESP id=%s key=%s status=%s dt=%.3fs len=%s"
            % (req_id, key.short(), r.status_code, dt, clen)
        )

        # best-effort parse
        payload: Any = None
        try:
            payload = r.json()
        except Exception as e:
            txt = ""
            try:
                txt = (r.text or "")[:5000]
            except Exception:
                txt = "<no-text>"
            print("[fetch.client] JSON_PARSE_FAIL id=%s key=%s err=%s text_snip=%s" % (req_id, key.short(), e, txt[:200]))
            payload = {"ok": False, "error": f"json_parse_fail:{e}", "text": txt, "rows": []}

        # attach some meta from HTTP
        if isinstance(payload, dict):
            payload.setdefault("_http", {})
            payload["_http"].update(
                {
                    "req_id": req_id,
                    "status_code": r.status_code,
                    "elapsed_sec": dt,
                    "url": url,
                }
            )

            # If HTTP status is not OK, force an error marker (even if body looks "ok")
            if r.status_code != 200:
                payload.setdefault("ok", False)
                payload.setdefault("error", f"http_{r.status_code}")
                payload["_http"]["http_error"] = True

                # Also include a small response snippet for quick debugging (without flooding logs)
                try:
                    payload["_http"]["text_snip"] = (r.text or "")[:300]
                except Exception:
                    payload["_http"]["text_snip"] = "<no-text>"

        else:
            # Non-dict payload but status != 200 -> wrap it so normalize sees the error
            if r.status_code != 200:
                payload = {
                    "ok": False,
                    "error": f"http_{r.status_code}",
                    "data": payload if payload is not None else [],
                    "_http": {"req_id": req_id, "status_code": r.status_code, "elapsed_sec": dt, "url": url, "http_error": True},
                }

        return normalize_indicator_response(payload, key=key)

    def _build_url(self) -> str:
        ep = (self.cfg.endpoint_indicator or "").strip()
        if not ep:
            ep = "/indicator"
        if not ep.startswith("/"):
            ep = "/" + ep
        return self.base_url + ep

    def _build_params(self, key: RequestKey) -> Dict[str, Any]:
        """
        Map RequestKey -> query params.
        """
        cnt = int(key.count) if key.count is not None else 1
        if cnt <= 0:
            print(f"[fetch.client] WARN count<=0 for key={key.short()} -> forcing count=1")
            cnt = 1

        out: Dict[str, Any] = {
            "name": key.indicator,
            "symbol": key.symbol,
            "chart_interval": key.interval,
            "indicator_interval": key.interval,
            "count": cnt,
        }

        # exchange (falls API es nutzt)
        if key.exchange:
            out["exchange"] = key.exchange

        # params ist JSON string
        if key.params_json:
            out["params"] = key.params_json
        else:
            out["params"] = "{}"

        # output optional
        if key.output:
            out["output"] = key.output

        # mode / as_of optional
        # only send as_of if mode is as_of or as_of is explicitly set
        if key.mode:
            out["mode"] = key.mode
        if key.as_of:
            out["as_of"] = key.as_of

        # Extra noisy sanity check
        print(
            "[fetch.client] PARAMS key=%s name=%s sym=%s itv=%s ex=%s out=%s cnt=%s mode=%s asof=%s"
            % (
                key.short(),
                out.get("name"),
                out.get("symbol"),
                out.get("chart_interval"),
                out.get("exchange"),
                out.get("output"),
                out.get("count"),
                out.get("mode"),
                out.get("as_of"),
            )
        )

        return out


def _short_params(p: Dict[str, Any]) -> str:
    """
    Kürzt params fürs Logging (sonst spammst du die Konsole zu).
    """
    try:
        pp = dict(p)
        if "params" in pp:
            s = str(pp["params"])
            pp["params"] = (s[:180] + "…") if len(s) > 180 else s
        return json.dumps(pp, ensure_ascii=False)
    except Exception:
        return str(p)
