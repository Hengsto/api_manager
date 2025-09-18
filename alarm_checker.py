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
DEBUG = os.getenv("ALARM_DEBUG", "1").strip() in ("1","true","yes","on")
BYPASS_COOLDOWN: bool = os.getenv("ALARM_BYPASS_COOLDOWN", "0").strip() in ("1","true","yes","on")

STATE_FILE = Path(".alarm_checker_state.json")  # persistenter Dedupe-State

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
                print(f"[DEBUG] HTTP {_fmt_try(i, tries)} {method} {url}")
                if json_body is not None:
                    print(f"[DEBUG]   payload: {json.dumps(json_body, ensure_ascii=False)[:400]}")
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

            if not r.text:
                if DEBUG:
                    print("[DEBUG] HTTP ok: empty body -> {}")
                return {}
            try:
                out = r.json()
                if DEBUG:
                    print(f"[DEBUG] HTTP ok: JSON len={len(r.text)}")
                return out
            except Exception:
                if DEBUG:
                    print(f"[DEBUG] Non-JSON response (len={len(r.text)}), returning text-wrapper")
                return {"_text": r.text}

        except Exception as e:
            last_err = e
            if DEBUG:
                print(f"[DEBUG] HTTP exception on {method} {url}: {e}")
            time.sleep(0.5 * (i + 1))
    if DEBUG:
        print(f"[DEBUG] HTTP failed after retries: {last_err}")
    return None

def _fmt_try(i: int, tries: int) -> str:
    return f"try {i+1}/{tries}"

# ---------------------------------------------------------------------
# State-Handling (Dedupe/Cooldown)
# ---------------------------------------------------------------------
def _load_state() -> Dict[str, float]:
    if not STATE_FILE.exists():
        if DEBUG:
            print(f"[DEBUG] state file missing -> {STATE_FILE} (starting fresh)")
        return {}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            if DEBUG:
                print(f"[DEBUG] state loaded: {len(data)} keys")
            return {str(k): float(v) for k, v in data.items()}
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] State load failed: {e}")
    return {}

def _save_state(state: Dict[str, float]) -> None:
    try:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        if DEBUG:
            print(f"[DEBUG] state saved: {len(state)} keys -> {STATE_FILE}")
    except Exception as e:
        if DEBUG:
            print(f"[DEBUG] State save failed: {e}")

def _alarm_key(alarm: Dict[str, Any]) -> str:
    smode = (alarm.get("single_mode") or "symbol").lower()
    tick_id = alarm.get("tick_id") or f"{alarm.get('interval','')}:{alarm.get('bar_ts') or alarm.get('ts')}"
    payload = {
        "single_mode": smode,
        "profile_id": alarm.get("profile_id"),
        # Bevorzugt group_id (stabiler als group_index, falls beide vorhanden)
        "group_id": alarm.get("group_id") or alarm.get("group_index"),
        "symbol": (None if smode in ("group","everything") else alarm.get("symbol")),
        "interval": alarm.get("interval"),
        "status": (alarm.get("status") or "FULL").upper(),
        "tick_id": tick_id,  # pro Kerze genau einmal
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    key = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    if DEBUG:
        print(f"[DEBUG] dedupe payload={raw} -> key={key[:10]}…")
    return key

def _is_cooled_down(state: Dict[str, float], key: str, now: float) -> bool:
    if BYPASS_COOLDOWN:
        if DEBUG:
            print(f"[DEBUG] cooldown bypass active → key={key[:10]}…")
        return True
    last = state.get(key, 0.0)
    cooled = (now - last) >= COOLDOWN_SEC
    if DEBUG:
        delta = now - last
        print(f"[DEBUG] cooldown check key={key[:10]}… delta={int(delta)}s >= {COOLDOWN_SEC}? {cooled}")
    return cooled

def _mark_sent(state: Dict[str, float], key: str, now: float) -> None:
    state[key] = now
    if DEBUG:
        print(f"[DEBUG] mark sent key={key[:10]}… at ts={now}")

# ---------------------------------------------------------------------
# Markdown Escaping (Legacy Markdown)
# ---------------------------------------------------------------------
def _escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    # Hinweis: V1-Markdown ist zickig; wir escapen breit, um Brüche zu vermeiden.
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
        (".", "\\."),  # bleibt erhalten wie im Original
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
                print(f"[DEBUG] Telegram {_fmt_try(i, max_1)} POST → chat_id={payload.get('chat_id')} len(text)={len(str(payload.get('text','')))}")
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

        gid_raw = alarm.get("group_id")
        gid_fallback = alarm.get("group_index")
        group_id_str = str(gid_raw) if gid_raw not in (None, "") else str(gid_fallback)

        payload = {
            "ts": alarm.get("ts"),
            "profile_id": str(alarm.get("profile_id") or ""),
            "group_id": group_id_str,
            "symbol": str(alarm.get("symbol") or ""),
            "interval": str(alarm.get("interval") or ""),
            "reason": alarm.get("reason") or "",
            "reason_code": alarm.get("reason_code") or "",
            # WICHTIG: Liste schicken, nicht json.dumps(...)
            "matched": list(alarm.get("conditions") or []),
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
        if DEBUG:
            print(f"[DEBUG] persist payload group_id={payload['group_id']!r} (type={type(payload['group_id']).__name__})")
            print(f"[DEBUG] persist payload matched_type={type(payload['matched']).__name__} len={len(payload['matched'])}")

        res = _http_json("POST", url, json_body=payload)
        if DEBUG:
            print(f"[DEBUG] Persist alarm -> {'OK' if res is not None else 'FAIL'}")
    except Exception as e:
        print(f"[DEBUG] Persist alarm error: {e}")


def _set_group_active(pid: Any, gid: Any, active: bool) -> bool:
    """
    Hartes Setzen von group.active über API:
      PATCH /notifier/profiles/{pid}/groups/{gid}/active  { "active": <bool> }
    """
    try:
        if pid in (None, "") or gid in (None, ""):
            if DEBUG:
                print(f"[DEBUG] _set_group_active skipped (missing pid/gid) pid={pid!r} gid={gid!r}")
            return False
        url = f"{NOTIFIER_ENDPOINT}/profiles/{pid}/groups/{gid}/active"
        body = {"active": bool(active)}
        if DEBUG:
            print(f"[DEBUG] _set_group_active → PATCH {url} payload={body}")
        res = _http_json("PATCH", url, json_body=body)
        ok = res is not None
        if DEBUG:
            print(f"[DEBUG] _set_group_active result -> {'OK' if ok else 'FAIL'} (pid={pid}, gid={gid}, active={active})")
        return ok
    except Exception as e:
        print(f"[DEBUG] _set_group_active error: {e}")
        return False

def _maybe_auto_deactivate_via_overrides(alarm: Dict[str, Any]) -> bool:
    """
    Setzt forced_off=true via API-Patch wenn deactivate_on-Regel greift.
    Zusätzlich: setzt group.active=false über Profile-API.
    Rückgabe: True, wenn mind. eine der Aktionen angewandt wurde.
    """
    mode = (alarm.get("notify_mode") or "").lower()    # "true" | "any_true" | "always"
    status = (alarm.get("status") or "").upper()       # "FULL" | "PARTIAL" | ...
    pid = alarm.get("profile_id")
    gid = alarm.get("group_id") or alarm.get("group_index")

    if DEBUG:
        print(f"[DEBUG] auto-deactivate check pid={pid} gid={gid} mode={mode} status={status}")

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

    applied_any = False

    # 1) Overrides: forced_off=true
    try:
        url = f"{NOTIFIER_ENDPOINT}/overrides/{pid}/{gid}"
        body = {"forced_off": True}
        if DEBUG:
            print(f"[DEBUG] PATCH overrides → {url} payload={body}")
        res = _http_json("PATCH", url, json_body=body)
        ok = res is not None
        if DEBUG:
            print(f"[DEBUG] overrides forced_off=true -> {'OK' if ok else 'FAIL'} pid={pid} gid={gid}")
        applied_any = applied_any or ok
    except Exception as e:
        print(f"[DEBUG] auto-deactivate overrides error: {e}")

    # 2) Profil-Gruppe aktiv=false (hart)
    try:
        ok2 = _set_group_active(pid, gid, False)
        applied_any = applied_any or ok2
    except Exception as e:
        print(f"[DEBUG] auto-deactivate set active=false error: {e}")

    return applied_any
def _resolve_gid(pid: Any, gid_or_index: Any) -> Optional[str]:
    """
    Liefert die echte gid einer Gruppe. Akzeptiert bereits-gid oder group_index (int/str).
    Holt /notifier/profiles/{pid} und mappt index->gid.
    """
    try:
        if not pid:
            return None
        s = str(gid_or_index) if gid_or_index is not None else ""
        # wenn schon wie eine gid aussieht (nicht rein numerisch), nimm sie direkt
        if s and not s.isdigit():
            return s

        url = f"{NOTIFIER_ENDPOINT}/profiles/{pid}"
        if DEBUG:
            print(f"[DEBUG] _resolve_gid → GET {url}")
        prof = _http_json("GET", url)
        if not prof:
            if DEBUG:
                print("[DEBUG] _resolve_gid: profile fetch failed or empty")
            return None

        groups = prof.get("condition_groups") or []
        idx = int(s) if s and s.isdigit() else None
        if idx is not None and 0 <= idx < len(groups):
            gid = groups[idx].get("gid")
            if DEBUG:
                print(f"[DEBUG] _resolve_gid: index {idx} -> gid {gid}")
            return gid

        # fallback: exakte gid
        for g in groups:
            if str(g.get("gid")) == s:
                return s

        if DEBUG:
            print(f"[DEBUG] _resolve_gid: no match for pid={pid} key={s!r}")
        return None
    except Exception as e:
        print(f"[DEBUG] _resolve_gid error: {e}")
        return None

# ---------------------------------------------------------------------
# Bucketing
# ---------------------------------------------------------------------
def _build_buckets(triggered: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for a in (triggered or []):
        gid = a.get("group_id") or a.get("group_index")
        smode = (a.get("single_mode") or "symbol").strip().lower()
        status = (a.get("status") or "FULL").upper()
        tick_id = a.get("tick_id") or f"{a.get('interval','')}:{a.get('bar_ts') or a.get('ts')}"

        if smode == "everything":
            bkey = f"ALL::{a.get('profile_id')}::{tick_id}::{status}"
        elif smode == "group":
            bkey = f"GRP::{a.get('profile_id')}::{gid}::{tick_id}::{status}"
        else:
            bkey = f"SYM::{a.get('profile_id')}::{gid}::{a.get('symbol')}::{tick_id}::{status}"

        if bkey not in buckets:
            buckets[bkey] = {"proto": a, "items": [a]}
        else:
            buckets[bkey]["items"].append(a)
    return buckets

# ---------------------------------------------------------------------
# Main-API
# ---------------------------------------------------------------------
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
    buckets = _build_buckets(triggered)
    if DEBUG:
        print(f"[DEBUG] built {len(buckets)} buckets")

    # deterministische Reihenfolge (debug-freundlich)
    for bkey in sorted(buckets.keys()):
        bundle = buckets[bkey]
        proto = bundle["proto"]
        items: List[Dict[str, Any]] = bundle["items"]

        # Dedupe-Key pro Bucket
        dedupe_key = _alarm_key({
            "profile_id": proto.get("profile_id"),
            "group_id": proto.get("group_id") or proto.get("group_index"),
            "symbol": proto.get("symbol"),  # bei group/everything egal
            "interval": proto.get("interval"),
            "status": (proto.get("status") or "FULL").upper(),
            "single_mode": (proto.get("single_mode") or "symbol").lower(),
            "tick_id": proto.get("tick_id") or f"{proto.get('interval','')}:{proto.get('bar_ts') or proto.get('ts')}",
        })

        cooled = _is_cooled_down(state, dedupe_key, now)
        if DEBUG:
            print(f"[DEBUG] bucket {bkey[:60]}… cooled={cooled} items={len(items)}")
        if not cooled:
            remaining = COOLDOWN_SEC - int(now - state.get(dedupe_key, 0.0))
            if DEBUG:
                print(f"[DEBUG] skip bucket (cooldown {remaining}s): {bkey}")
            continue

        # Nachricht zusammenbauen: Kopf vom proto, Body aus allen items
        msg_header = format_alarm_message(proto).split("\n\n", 1)[0]
        lines: List[str] = []
        for it in items:
            for c in (it.get("conditions") or []):
                lines.append(_fmt_cond_line(c))
        msg = f"{msg_header}\n\n" + ("\n".join(lines) if lines else "_(keine Detaildaten)_")

        # Overrides pro Bucket aus proto
        chat_override, token_override = _derive_overrides(proto)
        if chat_override:
            recipients: List[int | str] = [chat_override]
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

        # 4) Persistieren
        for it in items:
            try:
                _persist_alarm_via_api(it)
                persisted += 1
            except Exception:
                if DEBUG:
                    print("[DEBUG] persist failed (not counting)")

        # 5) Optional: Auto-Deaktivierung (overrides + active=false)
        for it in items:
            try:
                if _maybe_auto_deactivate_via_overrides(it):
                    deactivated += 1
                    it["deactivate_applied"] = (it.get("notify_mode") or "").lower() in ("true", "any_true")
            except Exception:
                if DEBUG:
                    print("[DEBUG] auto-deactivate failed")

        _mark_sent(state, dedupe_key, now)
        time.sleep(SEND_DELAY_SEC)

    _save_state(state)
    print(f"✅ Versand fertig. {sent} / {n} Nachricht(en) verschickt. Persistiert: {persisted}. Auto-deaktiviert: {deactivated}.")
