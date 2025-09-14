# -*- coding: utf-8 -*-
from __future__ import annotations
import os

import json
import time
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, NOTIFIER_ENDPOINT
from notifier.telegram_client import recipients_from_config

# ---------------------------------------------------------------------
# Einstellungen
# ---------------------------------------------------------------------
DEFAULT_BOT_TOKEN: str | None = TELEGRAM_BOT_TOKEN
DEFAULT_CHAT_ID_RAW = TELEGRAM_CHAT_ID  # kann int oder 0 sein

PARSE_MODE: str = "Markdown"   # Legacy-Markdown (V1), Escaping angepasst
MAX_MSG_LEN: int = 4096        # Telegram Limit
SEND_DELAY_SEC: float = 0.25   # kleines Intervall gegen Rate-Limits
COOLDOWN_SEC: int = int(os.getenv("ALARM_COOLDOWN_SEC", "300"))  # via ENV übersteuerbar
DEBUG = True
BYPASS_COOLDOWN: bool = os.getenv("ALARM_BYPASS_COOLDOWN", "0").strip() in ("1","true","yes","on")


STATE_FILE = Path(".alarm_checker_state.json")  # persistenter Dedupe-State
DEBUG = True

_TELEGRAM_BASE = "https://api.telegram.org"

# ---------------------------------------------------------------------
# HTTP Session (Retries) + vereinheitlichte JSON-Calls
# ---------------------------------------------------------------------
_SESSION: Optional[requests.Session] = None

def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    _SESSION = s
    return s

def _http_json(method: str, url: str, *, params: Dict[str, Any] | None = None,
               json_body: Dict[str, Any] | None = None, timeout: float = 10.0,
               tries: int = 3) -> Optional[Dict[str, Any]]:
    """Einheitlicher HTTP-Caller mit Retries, 429/5xx-Backoff und JSON-Rückgabe."""
    sess = _get_session()
    last_err: Optional[Exception] = None
    for i in range(max(1, tries)):
        try:
            if DEBUG:
                print(f"[DEBUG] {method} {url} try {i+1}/{tries}")
            r = sess.request(method, url, params=params, json=json_body, timeout=timeout)

            # 429 – respektiere Retry-After
            if r.status_code == 429:
                ra = int(r.headers.get("Retry-After", "1"))
                if DEBUG:
                    print(f"[DEBUG] 429 Too Many Requests. Retry-After={ra}")
                time.sleep(ra + 0.5)
                continue

            # 5xx – kurzer Backoff
            if 500 <= r.status_code < 600:
                if DEBUG:
                    print(f"[DEBUG] 5xx from server: {r.status_code} {r.text[:200]}")
                time.sleep(0.5 * (i + 1))
                continue

            # andere 4xx → raise_for_status (zeigt Fehler klar)
            if r.status_code >= 400:
                try:
                    print(f"[DEBUG] HTTP {r.status_code} body: {r.text[:500]}")
                except Exception:
                    pass
                r.raise_for_status()
            return r.json() if r.text else {}

        except Exception as e:
            last_err = e
            if DEBUG:
                print(f"[DEBUG] HTTP exception on {method} {url}: {e}")
            time.sleep(0.5 * (i + 1))
    if DEBUG:
        print(f"[DEBUG] HTTP failed after retries: {last_err}")
    return None

# ---------------------------------------------------------------------
# State-Handling (Dedupe/Cooldown)
# ---------------------------------------------------------------------
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
    smode = (alarm.get("single_mode") or "symbol").lower()
    tick_id = alarm.get("tick_id") or f"{alarm.get('interval','')}:{alarm.get('bar_ts') or alarm.get('ts')}"
    payload = {
        "single_mode": smode,
        "profile_id": alarm.get("profile_id"),
        "group_id": alarm.get("group_id"),
        "symbol": (None if smode in ("group","everything") else alarm.get("symbol")),
        "interval": alarm.get("interval"),
        "status": (alarm.get("status") or "FULL"),
        "tick_id": tick_id,  # pro Kerze genau einmal
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _is_cooled_down(state: Dict[str, float], key: str, now: float) -> bool:
    if BYPASS_COOLDOWN:
        if DEBUG: print(f"[DEBUG] cooldown bypass active → key={key[:10]}…")
        return True
    last = state.get(key, 0.0)
    return (now - last) >= COOLDOWN_SEC

def _mark_sent(state: Dict[str, float], key: str, now: float) -> None:
    state[key] = now

# ---------------------------------------------------------------------
# Markdown Escaping (Legacy Markdown)
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Telegram Senden mit Retries
# ---------------------------------------------------------------------
def _telegram_post(token: str, payload: Dict[str, Any], timeout: float = 10.0, max_tries: int = 3) -> None:
    url = f"{_TELEGRAM_BASE}/bot{token}/sendMessage"
    last_err: Optional[Exception] = None
    sess = _get_session()
    for i in range(max_1 := max(1, max_tries)):
        try:
            if DEBUG:
                print(f"[DEBUG] Telegram POST try {i+1}/{max_1} → chat_id={payload.get('chat_id')} len(text)={len(str(payload.get('text','')))}")
            r = sess.post(url, json=payload, timeout=timeout)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "1"))
                if DEBUG:
                    print(f"[DEBUG] 429 Too Many Requests. Retry-After={retry_after}")
                time.sleep(retry_after + 0.5)
                continue
            if 500 <= r.status_code < 600:
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
    """Fallback-Empfänger wie beim Watcher: AUTHORIZED_USERS bzw. TELEGRAM_CHAT_ID."""
    return recipients_from_config()

def _send_tg_text(
    text: str,
    recipients: List[int | str],
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

# ---------------------------------------------------------------------
# Nachricht aus Evaluator-Payload bauen
# ---------------------------------------------------------------------
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
    sym = R.get("symbol") or L.get("symbol") or c.get("symbol") or ""
    sym_tag = f"[{sym}] " if sym else ""
    return f"{emoji} {sym_tag}`{l_tag} {op} {r_tag}`  →  {l_val} vs {r_val}"



def _derive_overrides(alarm: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Liefert (chat_id, bot_token) aus Alarm, falls vorhanden.
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

    status = (alarm.get("status") or "").upper()     # "FULL" | "PARTIAL" | ""
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
    return f"{header}\n\n{body}"

# ---------------------------------------------------------------------
# API-Helpers (Persistenz + Overrides)
# ---------------------------------------------------------------------
def _persist_alarm_via_api(alarm: Dict[str, Any]) -> None:
    try:
        url = f"{NOTIFIER_ENDPOINT}/alarms"
        # group_id-Fallback
        gid = alarm.get("group_id") or alarm.get("group_index")

        gid = alarm.get("group_id") or alarm.get("group_index")
        # alarm_checker._persist_alarm_via_api
        gid_raw = alarm.get("group_id")
        gid_fallback = alarm.get("group_index")

        # immer String für die API
        group_id_str = (
            str(gid_raw) if gid_raw not in (None, "") else str(gid_fallback)
        )

        payload = {
            "ts": alarm.get("ts"),
            "profile_id": str(alarm.get("profile_id") or ""),  # safe-String
            "group_id": group_id_str,                          # <<< wichtig
            "symbol": str(alarm.get("symbol") or ""),
            "interval": str(alarm.get("interval") or ""),
            "reason": alarm.get("reason") or "",
            "reason_code": alarm.get("reason_code") or "",
            "matched": json.dumps(alarm.get("conditions") or []),
            "deactivate_applied": alarm.get("deactivate_applied", ""),
            "meta": {
                "status": alarm.get("status"),
                "notify_mode": alarm.get("notify_mode"),
                "profile_name": alarm.get("profile_name"),
                "group_name": alarm.get("group_name"),
                "exchange": alarm.get("exchange"),
                "description": alarm.get("description"),
                "telegram_bot_id": alarm.get("telegram_bot_id"),
                "telegram_chat_id": alarm.get("telegram_chat_id"),
                "telegram_bot_token": alarm.get("telegram_bot_token"),
            },
        }
        print(f"[DEBUG] persist payload group_id={payload['group_id']!r} (type={type(payload['group_id']).__name__})")



        res = _http_json("POST", url, json_body=payload)
        if DEBUG:
            print(f"[DEBUG] Persist alarm -> {'OK' if res is not None else 'FAIL'}")
    except Exception as e:
        print(f"[DEBUG] Persist alarm error: {e}")

def _maybe_auto_deactivate_via_overrides(alarm: Dict[str, Any]) -> bool:
    """
    Setzt forced_off=true via API-Patch wenn deactivate_on-Regel greift.
    Rückgabe: True, wenn Deaktivierung angewandt wurde.
    """
    mode = (alarm.get("notify_mode") or "").lower()    # "true" | "any_true" | "always"
    status = (alarm.get("status") or "").upper()       # "FULL" | "PARTIAL" | ...
    pid = alarm.get("profile_id")
    gid = alarm.get("group_id") or alarm.get("group_index")

    if not pid or gid in (None, ""):
        if DEBUG:
            print("[DEBUG] auto-deactivate skipped (missing pid/gid)")
        return False

    apply = False
    if mode == "true" and status == "FULL":
        apply = True
    elif mode == "any_true" and status in ("FULL", "PARTIAL"):
        apply = True
    elif mode == "always":
        apply = False  # keine Auto-Deaktivierung

    if not apply:
        if DEBUG:
            print(f"[DEBUG] auto-deactivate not applicable (mode={mode}, status={status})")
        return False

    try:
        url = f"{NOTIFIER_ENDPOINT}/overrides/{pid}/{gid}"
        body = {"forced_off": True}
        res = _http_json("PATCH", url, json_body=body)
        if DEBUG:
            print(f"[DEBUG] overrides forced_off=true -> {'OK' if res is not None else 'FAIL'} pid={pid} gid={gid}")
        return res is not None
    except Exception as e:
        print(f"[DEBUG] auto-deactivate error: {e}")
        return False

# ---------------------------------------------------------------------
# Main-API
# ---------------------------------------------------------------------
def _merge_bucket(pack: Dict[str, Any]) -> Dict[str, Any]:
    head = dict(pack["head"])
    items = pack["items"]

    # Aggregierte Conditions + Symbol-Liste
    agg_conditions: List[Dict[str, Any]] = []
    symbols: List[str] = []
    for it in items:
        sym = it.get("symbol") or "?"
        symbols.append(sym)
        for c in (it.get("conditions") or []):
            cc = dict(c)
            cc["symbol"] = sym  # damit die Zeile weiß, zu welchem Symbol sie gehört
            agg_conditions.append(cc)

    head["conditions"] = agg_conditions
    # Mehrere Symbole im Header zusammenziehen
    uniq_syms = sorted(set(symbols))
    if len(uniq_syms) > 1:
        head["symbol"] = ", ".join(uniq_syms)
    # tick_id/single_mode aus head bleiben erhalten
    return head

def _compact_matched(conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in (conditions or []):
        L = (c.get("left")  or {}) if isinstance(c.get("left"),  dict) else {}
        R = (c.get("right") or {}) if isinstance(c.get("right"), dict) else {}
        item = {
            "left":  L.get("label") or L.get("spec") or "",
            "right": R.get("label") or R.get("spec") or "",
            "op":    c.get("op_norm") or c.get("op") or "",
            "passed": bool(c.get("result")),
            "left_value":  L.get("value"),
            "right_value": R.get("value"),
            "left_ts":  L.get("ts"),
            "right_ts": R.get("ts"),
        }
        # optional: symbol anheften, falls vorhanden (für group/everything)
        sym = R.get("symbol") or L.get("symbol") or c.get("symbol")
        if sym: item["symbol"] = sym
        out.append(item)
    return out


def run_alarm_checker(triggered: List[Dict[str, Any]]) -> None:
    """
    Nimmt die Liste ausgelöster Gruppen (vom Evaluator) und verschickt Telegram-Nachrichten.
    Dedupe/Cooldown verhindert Spam.
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
    persisted = 0
    deactivated = 0

    # 1) Buckets bauen
    buckets: Dict[str, Dict[str, Any]] = {}
    for a in (triggered or []):
        # Fallback gid
        gid = a.get("group_id") or a.get("group_index")
        smode = (a.get("single_mode") or "symbol").strip().lower()
        status = (a.get("status") or "FULL").upper()
        tick_id = a.get("tick_id") or f"{a.get('interval','')}:{a.get('bar_ts') or a.get('ts')}"
        # Bucket-Key
        if smode == "everything":
            bkey = f"ALL::{a.get('profile_id')}::{tick_id}::{status}"
        elif smode == "group":
            bkey = f"GRP::{a.get('profile_id')}::{gid}::{tick_id}::{status}"
        else:
            bkey = f"SYM::{a.get('profile_id')}::{gid}::{a.get('symbol')}::{tick_id}::{status}"

        if bkey not in buckets:
            buckets[bkey] = {
                "proto": a,         # eine Vorlage (für Meta/Chat/Token etc.)
                "items": [a],       # alle Alarme in diesem Bucket
            }
        else:
            buckets[bkey]["items"].append(a)

    if DEBUG:
        print(f"[DEBUG] built {len(buckets)} buckets")

    # 2) Pro Bucket: Cooldown/Dedupe + Nachricht bauen + senden
    for bkey, bundle in buckets.items():
        proto = bundle["proto"]
        items: List[Dict[str, Any]] = bundle["items"]
        # Dedupe-Key pro Bucket
        dedupe_key = _alarm_key({
            "profile_id": proto.get("profile_id"),
            "group_index": proto.get("group_index"),
            "group_name": proto.get("group_name"),
            "symbol": proto.get("symbol"),  # bei group/everything egal
            "interval": proto.get("interval"),
            "status": (proto.get("status") or "FULL").upper(),
        })

        cooled = _is_cooled_down(state, dedupe_key, now)
        if DEBUG:
            print(f"[DEBUG] bucket {bkey[:40]}… cooled={cooled} items={len(items)}")
        if not cooled:
            remaining = COOLDOWN_SEC - int(now - state.get(dedupe_key, 0.0))
            if DEBUG: print(f"[DEBUG] skip bucket (cooldown {remaining}s): {bkey}")
            continue

        # Nachricht zusammenbauen: Kopf vom proto, Body aus allen items
        # Headline wie gehabt:
        msg_header = format_alarm_message(proto).split("\n\n", 1)[0]
        # Body = Liste der kondensierten Condition-Lines
        lines: List[str] = []
        for it in items:
            for c in (it.get("conditions") or []):
                lines.append(_fmt_cond_line(c))
        msg = f"{msg_header}\n\n" + ("\n".join(lines) if lines else "_(keine Detaildaten)_")

        # Overrides pro Bucket aus proto
        chat_override, token_override = _derive_overrides(proto)
        recipients: List[int | str]
        if chat_override:
            recipients = [chat_override]
        else:
            recipients = _default_recipients()
        if not recipients:
            raise RuntimeError("No recipients configured (set AUTHORIZED_USERS or TELEGRAM_CHAT_ID).")

        if DEBUG:
            token_used = "<override>" if token_override else "<default>"
            print(f"[DEBUG] sending bucket → {bkey} items={len(items)} recips={recipients} token={token_used} len={len(msg)}")

        # 3) Senden
        try:
            _send_tg_text(
                text=msg,
                recipients=[int(r) if isinstance(r, str) and r.isdigit() else r for r in recipients],
                bot_token=token_override or DEFAULT_BOT_TOKEN,
            )
            sent += 1
        except Exception as e:
            print("💥 Alarm send failed:", e)
            continue

        # 4) Persistieren (alle items persistieren; zählt nur bei Erfolg)
        for it in items:
            try:
                _persist_alarm_via_api(it)
                persisted += 1
            except Exception as _:
                if DEBUG: print("[DEBUG] persist failed (not counting)")

        # 5) Optional: Auto-Deaktivierung (einmal pro item prüfen)
        for it in items:
            try:
                if _maybe_auto_deactivate_via_overrides(it):
                    deactivated += 1
                    it["deactivate_applied"] = (it.get("notify_mode") or "").lower() in ("true", "any_true")
            except Exception as _:
                if DEBUG: print("[DEBUG] auto-deactivate failed")

        _mark_sent(state, dedupe_key, now)
        time.sleep(SEND_DELAY_SEC)

    _save_state(state)
    print(f"✅ Versand fertig. {sent} / {n} Nachricht(en) verschickt. Persistiert: {persisted}. Auto-deaktiviert: {deactivated}.")

    _save_state(state)
    print(f"✅ Versand fertig. {sent} / {n} Nachricht(en) verschickt. Persistiert: {persisted}. Auto-deaktiviert: {deactivated}.")
