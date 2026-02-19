# notifier_evaluator/fetch/client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Dict

from notifier_evaluator.fetch.types import RequestKey, normalize_indicator_response
from notifier_evaluator.models.runtime import FetchResult


@dataclass
class ClientConfig:
    base_url: str
    timeout_sec: int = 10
    retries: int = 2
    backoff: float = 0.3
    verify_ssl: bool = True
    endpoint_indicator: str = "/indicator"


class IndicatorClient:
    def __init__(self, cfg: ClientConfig):
        self.cfg = cfg
        self.base_url = (cfg.base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("[fetch.client] base_url is empty")
        print(f"[evaluator][DBG] fetch.client init base_url={self.base_url}")

    def _build_url(self) -> str:
        ep = self.cfg.endpoint_indicator if self.cfg.endpoint_indicator.startswith("/") else f"/{self.cfg.endpoint_indicator}"
        return f"{self.base_url}{ep}"

    def _build_params(self, key: RequestKey) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "name": key.indicator,
            "symbol": key.symbol,
            "chart_interval": key.interval,
            "indicator_interval": key.interval,
            "count": int(key.count) if int(key.count) > 0 else 1,
            "params": key.params_json or "{}",
            "mode": key.mode,
        }
        if key.exchange:
            out["exchange"] = key.exchange
        if key.output:
            out["output"] = key.output
        if key.as_of:
            out["as_of"] = key.as_of
        return out

    def fetch_indicator(self, key: RequestKey) -> FetchResult:
        req_id = uuid.uuid4().hex[:8]
        params = self._build_params(key)
        query = urllib.parse.urlencode(params)
        url = f"{self._build_url()}?{query}"
        t0 = time.time()
        print(f"[evaluator][DBG] fetch req_id={req_id} url={url}")

        payload: Any
        status_code = 0
        try:
            with urllib.request.urlopen(url, timeout=self.cfg.timeout_sec) as resp:
                status_code = int(getattr(resp, "status", 200))
                body = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(body)
        except urllib.error.HTTPError as e:
            status_code = e.code
            body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
            try:
                payload = json.loads(body)
            except Exception:
                payload = {"ok": False, "error": f"http_{status_code}", "text": body}
        except Exception as e:
            payload = {"ok": False, "error": f"request_exc:{e}", "rows": []}

        dt = time.time() - t0
        if isinstance(payload, dict):
            payload.setdefault("_http", {})
            payload["_http"].update({"req_id": req_id, "status_code": status_code, "elapsed_sec": dt, "url": url})
            if status_code and status_code != 200:
                payload.setdefault("ok", False)
                payload.setdefault("error", f"http_{status_code}")
        return normalize_indicator_response(payload, key=key)
