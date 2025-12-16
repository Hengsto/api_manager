# notifier_evaluator/fetch/client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from notifier_evaluator.fetch.types import RequestKey, normalize_indicator_response


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
            "[fetch.client] init base_url=%s timeout=%ss retries=%d verify_ssl=%s"
            % (self.base_url, cfg.timeout_sec, cfg.retries, cfg.verify_ssl)
        )

    def _make_session(self, cfg: ClientConfig) -> requests.Session:
        s = requests.Session()

        # urllib3 Retry config (robust genug für Start)
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

    def fetch_indicator(self, key: RequestKey) -> Any:
        """
        Führt HTTP Request aus und gibt *normalisierte* FetchResult zurück.
        """
        url = self.base_url + self.cfg.endpoint_indicator
        params = self._build_params(key)

        t0 = time.time()
        print("[fetch.client] GET %s params=%s" % (url, _short_params(params)))

        try:
            r = self.session.get(
                url,
                params=params,
                timeout=self.cfg.timeout_sec,
                verify=self.cfg.verify_ssl,
            )
        except Exception as e:
            dt = time.time() - t0
            print("[fetch.client] EXC key=%s dt=%.3fs err=%s" % (key.short(), dt, e))
            return normalize_indicator_response(
                {"error": f"request_exc:{e}", "rows": []},
                key=key,
            )

        dt = time.time() - t0
        print("[fetch.client] RESP key=%s status=%s dt=%.3fs len=%s" % (key.short(), r.status_code, dt, r.headers.get("Content-Length")))

        # best-effort parse
        payload: Any = None
        try:
            payload = r.json()
        except Exception as e:
            # fallback: text
            txt = ""
            try:
                txt = r.text[:5000]
            except Exception:
                txt = "<no-text>"
            print("[fetch.client] JSON_PARSE_FAIL key=%s err=%s text_snip=%s" % (key.short(), e, txt[:200]))
            payload = {"error": f"json_parse_fail:{e}", "text": txt, "rows": []}

        # attach some meta from HTTP
        if isinstance(payload, dict):
            payload.setdefault("_http", {})
            payload["_http"].update(
                {
                    "status_code": r.status_code,
                    "elapsed_sec": dt,
                    "url": url,
                }
            )

        return normalize_indicator_response(payload, key=key)

    def _build_params(self, key: RequestKey) -> Dict[str, Any]:
        """
        Map RequestKey -> query params.
        """
        # NOTE:
        # Wenn dein API chart_interval vs indicator_interval trennt,
        # kannst du das später hier erweitern.
        out: Dict[str, Any] = {
            "name": key.indicator,
            "symbol": key.symbol,
            "chart_interval": key.interval,
            "indicator_interval": key.interval,
            "count": int(key.count),
        }

        # exchange (falls API es nutzt)
        if key.exchange:
            out["exchange"] = key.exchange

        # params ist JSON string
        # key.params_json ist bereits stable_json(...) aus fetch/types.py
        if key.params_json:
            out["params"] = key.params_json
        else:
            out["params"] = "{}"

        # output optional
        if key.output:
            out["output"] = key.output

        # mode / as_of optional (future)
        if key.mode:
            out["mode"] = key.mode
        if key.as_of:
            out["as_of"] = key.as_of

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
