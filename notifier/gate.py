# evaluator/gate.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import json, time, hashlib
from pathlib import Path
from typing import Any, Dict, List, Tuple

DEBUG = True

# ENV > config.GATE_STATE_NOTIFIER > neben PROFILES_NOTIFIER > CWD
try:
    from config import GATE_STATE_NOTIFIER as _CFG_GATE
except Exception:
    _CFG_GATE = None
try:
    from config import PROFILES_NOTIFIER as _CFG_PROFILES
except Exception:
    _CFG_PROFILES = None

_env_gate = os.getenv("NOTIFIER_GATE_STATE", "").strip()
if _env_gate:
    GATE_STATE_FILE = Path(_env_gate).expanduser().resolve()
elif _CFG_GATE:
    GATE_STATE_FILE = Path(_CFG_GATE).expanduser().resolve()
elif _CFG_PROFILES:
    GATE_STATE_FILE = Path(_CFG_PROFILES).expanduser().resolve().parent / "evaluator_gate_state.json"
else:
    GATE_STATE_FILE = Path(".evaluator_gate_state.json").expanduser().resolve()

# Ordner sicherstellen + kurzer Debug-Hinweis
try:
    GATE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
except Exception:
    pass
if DEBUG:
    try:
        print(f"[DBG] Gate state file: {GATE_STATE_FILE}")
    except Exception:
        pass

STREAK_GAP_RESET_SEC = 180  # Reset, wenn so lange kein passender Status

def _load_gate_state() -> Dict[str, Dict[str, float]]:
    if not GATE_STATE_FILE.exists():
        return {}
    try:
        with GATE_STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): {"fs": float(v.get("fs",0)), "ps": float(v.get("ps",0)), "last": float(v.get("last",0))}
                    for k,v in data.items() if isinstance(v, dict)}
    except Exception as e:
        if DEBUG: print(f"[DBG] gate state load failed: {e}")
    return {}

def _save_gate_state(state: Dict[str, Dict[str, float]]) -> None:
    try:
        with GATE_STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        if DEBUG: print(f"[DBG] gate state saved: {GATE_STATE_FILE}")
    except Exception as e:
        if DEBUG: print(f"[DBG] gate state save failed: {e}")

def _group_key(ev: Dict[str, Any]) -> str:
    payload = {
        "profile_id": ev.get("profile_id"),
        "group_index": ev.get("group_index"),
        "group_name": ev.get("group_name"),
        "symbol": ev.get("symbol"),
        "interval": ev.get("interval"),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",",":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _upd_streak(st: Dict[str, Dict[str, float]], key: str, now: float, is_full: bool, is_partial: bool) -> Tuple[int,int]:
    e = st.get(key)
    if not e:
        e = {"fs":0.0, "ps":0.0, "last":0.0}
        st[key] = e
    gap = now - float(e.get("last",0.0))
    if gap > STREAK_GAP_RESET_SEC:
        if DEBUG: print(f"[DBG] streak reset gap={gap:.1f}s key={key[:8]}")
        e["fs"] = 0.0
        e["ps"] = 0.0
    # FULL zählt auch als PARTIAL
    e["ps"] = (e.get("ps",0.0) + 1.0) if (is_partial or is_full) else 0.0
    e["fs"] = (e.get("fs",0.0) + 1.0) if is_full else 0.0
    e["last"] = now
    return int(e["fs"]), int(e["ps"])

def gate_and_build_triggers(evals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    evals: Liste von Dicts mit mindestens:
      - profile_id, profile_name, group_index, group_name, symbol, interval, exchange?, description?
      - status ∈ {"FULL","PARTIAL","NONE"}
      - deactivate_on ∈ {"always","true","any_true"}  (UI-Feld; hier als Notify-Mode genutzt)
      - min_true_ticks (optional int)
      - conditions (Liste; wird 1:1 in Payload übernommen)
    """
    out: List[Dict[str, Any]] = []
    if not evals: return out

    st = _load_gate_state()
    now = time.time()

    for ev in evals:
        status = (ev.get("status") or "NONE").upper()
        mode   = (ev.get("deactivate_on") or "true").lower()
        # notify_mode: always → wie "true" behandeln (nur FULL zählt)
        notify_mode = "any_true" if mode == "any_true" else "true"

        is_full    = (status == "FULL")
        is_partial = (status == "PARTIAL")

        key = _group_key(ev)
        fs, ps = _upd_streak(st, key, now, is_full, is_partial)

        mtt_raw = ev.get("min_true_ticks")
        try:
            mtt = int(mtt_raw) if mtt_raw not in (None, "", "null") else 1
        except Exception:
            if DEBUG: print(f"[DBG] invalid min_true_ticks={mtt_raw} → fallback=1")
            mtt = 1
        if mtt < 1: mtt = 1

        # Gate pro Modus
        if notify_mode == "true":
            passed = (is_full and fs >= mtt)
            streak = fs
        else:  # any_true
            passed = ((is_partial or is_full) and ps >= mtt)

            streak = ps

        if DEBUG:
            print(f"[DBG] {ev.get('profile_name')}/{ev.get('group_name')}/{ev.get('symbol')} "
                  f"mode={notify_mode} status={status} streak={streak}/{mtt} → {'PASS' if passed else 'HOLD'}")

        if not passed:
            continue

        # Build Alarm-Payload
        payload = {
            "profile_id":   ev.get("profile_id"),
            "profile_name": ev.get("profile_name"),
            "group_index":  ev.get("group_index"),
            "group_name":   ev.get("group_name"),
            "symbol":       ev.get("symbol"),
            "interval":     ev.get("interval"),
            "exchange":     ev.get("exchange"),
            "description":  ev.get("description"),
            "ts":           ev.get("ts"),
            "status":       "FULL" if is_full else "PARTIAL",
            "notify_mode":  notify_mode,
            "min_true_ticks": mtt,
            "streak":       streak,
            "conditions":   ev.get("conditions") or [],
            # Telegram-Overrides, falls vorhanden
            "telegram_bot_id":  ev.get("telegram_bot_id"),
            "telegram_bot_token": ev.get("telegram_bot_token"),
            "telegram_chat_id": ev.get("telegram_chat_id"),
        }
        out.append(payload)

    _save_gate_state(st)
    return out
