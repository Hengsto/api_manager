# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
# ⬇️ wichtig: Empfänger-Liste wie beim Watcher verwenden
from notifier.telegram_client import recipients_from_config

# -----------------------------------------------------------------------------
# Einstellungen
# -----------------------------------------------------------------------------
DEFAULT_BOT_TOKEN: str | None = TELEGRAM_BOT_TOKEN
DEFAULT_CHAT_ID_RAW = TELEGRAM_CHAT_ID  # kann int oder 0 sein

PARSE_MODE: str = "Markdown"  # bewusst Legacy-Markdown, Escaping angepasst
MAX_MSG_LEN: int = 4096       # Telegram Limit
SEND_DELAY_SEC: float = 0.25  # kleines Intervall gegen Rate-Limits
COOLDOWN_SEC: int = 300       # 5 Minuten Cooldown pro (profile, group, symbol, interval)

STATE_FILE = Path(".alarm_checker_state.json")  # persistente Dedupe-State

DEBUG = True

# -----------------------------------------------------------------------------
# State-Handling (Dedupe/Cooldown)
# -----------------------------------------------------------------------------
def _load_state() -> Dict[str, float]:
    if not STATE_FILE.exists():
        return {}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): float(v) for k, v in data.items()}
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] State load failed: {e}")
    return {}

def _save_state(state: Dict[str, float]) -> None:
    try:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] State save failed: {e}")

def _alarm_key(alarm: Dict[str, Any]) -> str:
    payload = {
        "profile_id": alarm.get("profile_id"),
        "group_index": alarm.get("group_index"),
        "group_name": alarm.get("group_name"),
        "symbol": alarm.get("symbol"),
        "interval": alarm.get("interval"),
        "status": (alarm.get("status") or "FULL"),  # PARTIAL/FULL getrennt dedupen
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _is_cooled_down(state: Dict[str, float], key: str, now: float) -> bool:
    last = state.get(key, 0.0)
    return (now - last) >= COOLDOWN_SEC

def _mark_sent(state: Dict[str, float], key: str, now: float) -> None:
    state[key] = now

# -----------------------------------------------------------------------------
# Markdown Escaping (Legacy Markdown)
# -----------------------------------------------------------------------------
def _escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    replacements = [
        ("\\", "\\\\"),
        ("_", "\\_"),
        ("*", "\\*"),
        ("[", "\\["),
        ("]", "\\]"),
        ("(", "\\("),
        (")", "\\)"),
        ("`", "\\`"),
        ("~", "\\~"),
        (">", "\\>"),
        ("#", "\\#"),
        ("+", "\\+"),
        ("-", "\\-"),
        ("=", "\\="),
        ("|", "\\|"),
        ("{", "\\{"),
        ("}", "\\}"),
        (".", "\\."),
        ("!", "\\!"),
    ]
    out = text
    for a, b in replacements:
        out = out.replace(a, b)
    return out

# -----------------------------------------------------------------------------
# Telegram Senden mit Retries
# -----------------------------------------------------------------------------
def _telegram_post(token: str, payload: Dict[str, Any], timeout: float = 10.0, max_tries: int = 3) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    last_err: Optional[Exception] = None
    for i in range(max_tries):
        try:
            if DEBUG:
                print(f"[DEBUG] Telegram POST try {i+1}/{max_tries} → chat_id={payload.get('chat_id')} len(text)={len(str(payload.get('text','')))}")
            r = requests.post(url, json=payload, timeout=timeout)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "1"))
                if DEBUG:
                    print(f"[DEBUG] 429 Too Many Requests. Retry-After={retry_after}")
                time.sleep(retry_after + 0.5)
                continue
            if r.status_code >= 500:
                if DEBUG:
                    print(f"[DEBUG] 5xx from Telegram: {r.status_code} {r.text[:200]}")
                time.sleep(0.75 * (i + 1))
                continue
            if r.status_code != 200:
                print("⚠️ Telegram Error:", r.text)
            r.raise_for_status()
            if DEBUG:
                print("[DEBUG] Telegram message sent OK")
            return
        except Exception as e:
            last_err = e
            if DEBUG:
                print(f"[DEBUG] Telegram Exception: {e}")
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"Telegram POST failed after retries: {last_err}")

def _default_recipients() -> List[int]:
    """
    Fallback-Empfänger wie beim Watcher:
    - TELEGRAM_CHAT_ID (falls != 0) ODER
    - AUTHORIZED_USERS aus config (.env)
    """
    recips = recipients_from_config()  # kommt aus notifier.telegram_client
    return recips

def _send_tg_text(
    text: str,
    recipients: List[int],
    bot_token: Optional[str],
    disable_web_page_preview: bool = True,
) -> None:
    token = (bot_token or DEFAULT_BOT_TOKEN or "").strip()
    if not token:
        raise RuntimeError("Telegram token missing.")

    txt = _escape_markdown(text)
    if len(txt) > MAX_MSG_LEN:
        txt = txt[: MAX_MSG_LEN - 50] + "\n… _gekürzt_"

    for rid in recipients:
        payload = {
            "chat_id": str(rid),
            "text": txt,
            "parse_mode": PARSE_MODE,
            "disable_web_page_preview": disable_web_page_preview,
        }
        _telegram_post(token, payload)
        time.sleep(SEND_DELAY_SEC)

# -----------------------------------------------------------------------------
# Nachricht aus Evaluator-Payload bauen
# -----------------------------------------------------------------------------
def _fmt_value(v: Any) -> str:
    try:
        if v is None:
            return "—"
        if isinstance(v, float):
            return f"{v:.6g}"
        return str(v)
    except Exception:
        return str(v)

def _fmt_cond_line(c: Dict[str, Any]) -> str:
    ok = bool(c.get("result"))
    op = c.get("op", "?")
    L = c.get("left", {}) or {}
    R = c.get("right", {}) or {}

    l_label = L.get("label") or L.get("spec") or "?"
    l_out   = L.get("output") or L.get("col") or ""
    l_val   = _fmt_value(L.get("value"))

    r_label = R.get("label") or R.get("spec") or "?"
    r_out   = R.get("output") or R.get("col") or ""
    r_val   = _fmt_value(R.get("value"))

    l_tag = f"{l_label}{'·'+str(l_out) if l_out else ''}"
    r_tag = f"{r_label}{'·'+str(r_out) if r_out else ''}"

    emoji = "✅" if ok else "❌"
    line = f"{emoji} `{l_tag} {op} {r_tag}`  →  {l_val} vs {r_val}"
    return line

def _derive_overrides(alarm: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Liefert (chat_id, bot_token) aus Alarm, falls vorhanden.
    Regeln:
      - telegram_chat_id → Chat-ID (string/int)
      - telegram_bot_token → Token
      - telegram_bot_id → wenn ':' enthält ⇒ Token, sonst ⇒ Chat-ID
    """
    chat_id = alarm.get("telegram_chat_id")
    bot_token = alarm.get("telegram_bot_token")

    group_field = alarm.get("telegram_bot_id")
    if group_field and not chat_id and not bot_token:
        s = str(group_field)
        if ":" in s:
            bot_token = s
        else:
            chat_id = s

    return (str(chat_id) if chat_id else None, str(bot_token) if bot_token else None)

def format_alarm_message(alarm: Dict[str, Any]) -> str:
    profile = alarm.get("profile_name") or "?"
    group   = alarm.get("group_name") or f"group_{alarm.get('group_index')}"
    sym     = alarm.get("symbol") or "?"
    iv      = alarm.get("interval") or "?"
    exch    = alarm.get("exchange") or ""
    desc    = alarm.get("description") or ""
    ts      = alarm.get("ts") or ""

    status = (alarm.get("status") or "").upper()  # "FULL" | "PARTIAL" | ""
    mode   = (alarm.get("notify_mode") or "").lower()  # "true" | "any_true" | "always"
    streak = alarm.get("streak")
    mtt    = alarm.get("min_true_ticks")

    header = (
        "🚨 *Alarm ausgelöst*\n"
        f"*Profil:* {profile}\n"
        f"*Gruppe:* {group}\n"
        f"*Symbol:* {sym}  \n"
        f"*Intervall:* {iv}"
        f"{('  \n*Börse:* ' + exch) if exch else ''}"
        f"{('  \n' + desc) if desc else ''}\n"
        f"*Status:* {status or '—'}"
        f"{('  ·  Modus: ' + mode) if mode else ''}"
        f"{('  ·  Streak: ' + str(streak)) if isinstance(streak, (int,float)) else ''}"
        f"{('  ·  MinTicks: ' + str(mtt)) if mtt else ''}\n"
        f"*Zeit:* {ts}"
    )


    lines: List[str] = []
    for c in (alarm.get("conditions") or []):
        try:
            lines.append(_fmt_cond_line(c))
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] format condition failed: {e}")
            lines.append("❓ (Bedingung konnte nicht formatiert werden)")

    body = "\n".join(lines) if lines else "_(keine Detaildaten)_"
    text = f"{header}\n\n{body}"
    return text

# -----------------------------------------------------------------------------
# Main-API
# -----------------------------------------------------------------------------
def run_alarm_checker(triggered: List[Dict[str, Any]]) -> None:
    """
    Nimmt die Liste ausgelöster Gruppen (vom Evaluator) und verschickt
    Telegram-Nachrichten. Dedupe/Cooldown verhindert Spam.
    - Empfänger: per-Alarm Override > recipients_from_config()
    - Token:     per-Alarm Override > TELEGRAM_BOT_TOKEN
    """
    n = len(triggered or [])
    print(f"🔔 {n} Alarm(e) prüfen...")

    if not triggered:
        return

    state = _load_state()
    now = time.time()
    sent = 0

    for alarm in triggered:
        try:
            key = _alarm_key(alarm)
            if not _is_cooled_down(state, key, now):
                if DEBUG:
                    print(f"[DEBUG] skip (cooldown): {alarm.get('profile_name')} / {alarm.get('group_name')} / {alarm.get('symbol')}")
                continue

            msg = format_alarm_message(alarm)

            # Overrides?
            chat_override, token_override = _derive_overrides(alarm)

            # Empfänger bestimmen
            if chat_override:
                recipients = [chat_override]
            else:
                recipients = _default_recipients()

            if not recipients:
                raise RuntimeError("No recipients configured (set AUTHORIZED_USERS or TELEGRAM_CHAT_ID).")

            if DEBUG:
                token_used = "<override>" if token_override else "<default>"
                print(f"[DEBUG] sending → recips={recipients} token={token_used} len={len(msg)}")

            _send_tg_text(
                text=msg,
                recipients=[int(r) for r in recipients],
                bot_token=token_override or DEFAULT_BOT_TOKEN,
            )

            _mark_sent(state, key, now)
            sent += 1

        except Exception as e:
            print("💥 Alarm send failed:", e)

        # leichte Pause zwischen Alarms
        time.sleep(SEND_DELAY_SEC)

    _save_state(state)
    print(f"✅ Versand fertig. {sent} / {n} Nachricht(en) verschickt.")
