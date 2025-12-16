# notifier_evaluator/alarms/dispatcher.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from notifier_evaluator.alarms.formatter import FormattedMessage, format_event
from notifier_evaluator.models.runtime import HistoryEvent


# ──────────────────────────────────────────────────────────────────────────────
# Dispatcher
# - nimmt Events, filtert push-relevante, formatiert, sendet (oder dry-run)
# - KEINE Policy hier (Policy entscheidet push ja/nein)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class DispatchConfig:
    mode: str = "dry_run"  # "dry_run" | "telegram"
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    timeout_sec: int = 10


@dataclass
class DispatchResult:
    sent: int
    failed: int
    details: List[Dict[str, Any]]


class Dispatcher:
    def __init__(self, cfg: DispatchConfig, session: Optional[requests.Session] = None):
        self.cfg = cfg
        self.session = session or requests.Session()

        print(
            "[dispatcher] init mode=%s chat_id=%s token_set=%s"
            % (cfg.mode, cfg.telegram_chat_id, bool(cfg.telegram_bot_token))
        )

    def dispatch(self, events: List[HistoryEvent]) -> DispatchResult:
        """
        Dispatches relevant events:
          - "push"
          - (optional) "partial_change" if you want pre-notifications as messages too
        """
        sent = 0
        failed = 0
        details: List[Dict[str, Any]] = []

        for ev in events or []:
            et = (ev.event or "").strip().lower()
            if et not in ("push", "partial_change"):
                continue

            msg = format_event(ev)

            if self.cfg.mode == "dry_run":
                print("[dispatcher] DRY_RUN send:\n%s\n---" % msg.text)
                sent += 1
                details.append({"event": et, "ok": True, "mode": "dry_run"})
                continue

            if self.cfg.mode == "telegram":
                ok, info = self._send_telegram(msg)
                if ok:
                    sent += 1
                else:
                    failed += 1
                details.append({"event": et, "ok": ok, "mode": "telegram", "info": info})
                continue

            # unknown mode
            print("[dispatcher] WARN unknown mode=%s (skip)" % self.cfg.mode)
            failed += 1
            details.append({"event": et, "ok": False, "mode": self.cfg.mode, "info": "unknown_mode"})

        print("[dispatcher] DONE sent=%d failed=%d total=%d" % (sent, failed, len(details)))
        return DispatchResult(sent=sent, failed=failed, details=details)

    def _send_telegram(self, msg: FormattedMessage) -> (bool, str):
        token = (self.cfg.telegram_bot_token or "").strip()
        chat_id = (self.cfg.telegram_chat_id or "").strip()
        if not token or not chat_id:
            err = "missing_telegram_config"
            print("[dispatcher] FAIL %s token=%s chat_id=%s" % (err, bool(token), bool(chat_id)))
            return False, err

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": msg.text,
            "disable_web_page_preview": True,
        }

        t0 = time.time()
        try:
            r = self.session.post(url, json=payload, timeout=self.cfg.timeout_sec)
        except Exception as e:
            dt = time.time() - t0
            print("[dispatcher] EXC telegram dt=%.3fs err=%s" % (dt, e))
            return False, f"request_exc:{e}"

        dt = time.time() - t0
        ok = (r.status_code == 200)
        snip = ""
        try:
            snip = r.text[:400]
        except Exception:
            snip = "<no-text>"

        print("[dispatcher] telegram status=%s ok=%s dt=%.3fs resp_snip=%s" % (r.status_code, ok, dt, snip))

        if not ok:
            return False, f"http_{r.status_code}:{snip}"

        return True, "ok"
