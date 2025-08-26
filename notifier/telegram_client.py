# notifier/telegram_client.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import requests
from typing import List
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, AUTHORIZED_USERS

def _escape_md(text: str) -> str:
    # minimal für Markdown (Legacy)
    return (text or "").replace("\\","\\\\").replace("_","\\_").replace("*","\\*")\
        .replace("[","\\[").replace("]","\\]").replace("(","\\(").replace(")","\\)")\
        .replace("`","\\`").replace("~","\\~").replace(">","\\>").replace("#","\\#")\
        .replace("+","\\+").replace("-","\\-").replace("=","\\=").replace("|","\\|")\
        .replace("{","\\{").replace("}","\\}").replace(".","\\.").replace("!","\\!")

def recipients_from_config() -> List[int]:
    recips: List[int] = []
    try:
        if TELEGRAM_CHAT_ID and int(TELEGRAM_CHAT_ID) != 0:
            recips.append(int(TELEGRAM_CHAT_ID))
    except Exception:
        pass
    try:
        for u in (AUTHORIZED_USERS or []):
            iu = int(u)
            if iu not in recips:
                recips.append(iu)
    except Exception:
        pass
    return recips

def is_ready() -> bool:
    return bool(TELEGRAM_BOT_TOKEN) and bool(recipients_from_config())

def debug_status() -> None:
    recips = recipients_from_config()
    ready = is_ready()
    print(f"[TG] ready={ready} token_len={len(TELEGRAM_BOT_TOKEN or '')} recipients={recips} "
          f"(chat_id_cfg={TELEGRAM_CHAT_ID}, authorized={AUTHORIZED_USERS})")

def refresh_settings() -> None:
    # Platzhalter – falls du später dynamisch ENV einlesen willst.
    pass

def send_message(text: str) -> None:
    recips = recipients_from_config()
    if not TELEGRAM_BOT_TOKEN or not recips:
        print("[TG] nicht konfiguriert – skip send"); return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload_base = {"parse_mode": "Markdown", "disable_web_page_preview": True}
    for rid in recips:
        payload = dict(payload_base); payload["chat_id"] = rid; payload["text"] = _escape_md(text)
        try:
            r = requests.post(url, json=payload, timeout=10)
            ok = (r.status_code == 200)
            print(f"[TG] send -> {rid} {r.status_code} {'OK' if ok else r.text[:120]}")
            r.raise_for_status()
        except Exception as e:
            print(f"[TG] Exception -> {rid}: {e}")
