# api/notifier/alarms.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from config import ALARMS_NOTIFIER
from storage import load_json, save_json
from api.notifier.profiles import _norm_symbol  # gleiche Normierung wie bei Profilen

log = logging.getLogger("notifier.alarms")


# ─────────────────────────────────────────────────────────────
# Zeit / Helfer
# ─────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")


def _parse_ts(s: str) -> Optional[float]:
    """
    Parsed TS im Format:
      - "YYYY-mm-dd HH:MM:SS[.ms]Z"
      - "YYYY-mm-ddTHH:MM:SS[.ms]Z"
      - ohne Z → als UTC angenommen
    Gibt Unix-Timestamp (float) oder None zurück.
    """
    if not s:
        return None
    x = s.strip()
    x = x.replace("T", " ").replace("z", "Z")
    if x.endswith("Z"):
        x = x[:-1]
    try:
        dt = datetime.fromisoformat(x)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# Pydantic-Modelle (werden vom API-Layer benutzt)
# ─────────────────────────────────────────────────────────────

class AlarmBase(BaseModel):
    ts: str
    profile_id: str
    group_id: str
    symbol: str
    interval: str = ""
    reason: str = ""
    reason_code: str = ""
    matched: List[Dict[str, Any]] = Field(default_factory=list)
    deactivate_applied: str = ""  # "", "true", "any_true"
    meta: Dict[str, Any] = Field(default_factory=dict)


class AlarmOut(AlarmBase):
    id: str


class AlarmIn(AlarmBase):
    id: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# Low-Level IO
# ─────────────────────────────────────────────────────────────

def load_alarms() -> List[dict]:
    """
    Lädt Alarm-Liste aus ALARMS_NOTIFIER.
    Stellt Default-Felder & Typen sicher.
    """
    data = load_json(ALARMS_NOTIFIER, [])
    out: List[dict] = []

    for a in data or []:
        if not isinstance(a, dict):
            continue

        a.setdefault("id", "")
        a.setdefault("ts", "")
        a.setdefault("profile_id", "")
        a.setdefault("group_id", "")
        a.setdefault("symbol", "")
        a.setdefault("interval", "")
        a.setdefault("reason", "")
        a.setdefault("reason_code", "")
        a.setdefault("matched", [])
        a.setdefault("deactivate_applied", "")
        a.setdefault("meta", {})

        # matched: ggf. von String → List
        if isinstance(a["matched"], str):
            try:
                parsed = json.loads(a["matched"])
                a["matched"] = parsed if isinstance(parsed, list) else []
            except Exception:
                a["matched"] = []

        # meta: ggf. von String → Dict
        if isinstance(a["meta"], str):
            try:
                parsed_meta = json.loads(a["meta"])
                a["meta"] = parsed_meta if isinstance(parsed_meta, dict) else {}
            except Exception:
                a["meta"] = {}

        # deactivate_applied normalisieren
        if a.get("deactivate_applied") not in {"", "true", "any_true"}:
            a["deactivate_applied"] = ""

        out.append(a)

    log.info("Alarms loaded count=%d", len(out))
    try:
        print(f"[ALARMS] load count={len(out)}")
    except Exception:
        pass
    return out


def save_alarms(items: List[dict]) -> None:
    """
    Speichert Alarm-Liste nach ALARMS_NOTIFIER.
    """
    save_json(ALARMS_NOTIFIER, items)
    log.info("Alarms saved count=%d", len(items))
    try:
        print(f"[ALARMS] save count={len(items)}")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# High-Level Operationen (für API-Layer)
# ─────────────────────────────────────────────────────────────

def add_alarm_entry(alarm_payload: Dict[str, Any]) -> str:
    """
    Fügt einen Alarm zur Historie hinzu.
    Erwartet ein bereits validiertes Dict (typisch aus AlarmIn).
    Gibt die Alarm-ID zurück.
    """
    items = load_alarms()
    payload = dict(alarm_payload)

    # matched normalisieren
    m = payload.get("matched", [])
    if isinstance(m, str):
        try:
            m2 = json.loads(m)
            payload["matched"] = m2 if isinstance(m2, list) else []
        except Exception:
            payload["matched"] = []
    elif not isinstance(m, list):
        payload["matched"] = []

    # meta normalisieren
    meta = payload.get("meta", {})
    if isinstance(meta, str):
        try:
            m3 = json.loads(meta)
            payload["meta"] = m3 if isinstance(m3, dict) else {}
        except Exception:
            payload["meta"] = {}
    elif not isinstance(meta, dict):
        payload["meta"] = {}

    # ID
    aid = str(payload.get("id") or "")
    if not aid:
        # einfache Fallback-ID: timestamp + counter (reicht für Historie)
        aid = f"{int(datetime.now(timezone.utc).timestamp())}-{len(items)+1}"
    payload["id"] = aid

    # deactivate_applied
    da = str(payload.get("deactivate_applied") or "").strip().lower()
    if da not in {"", "true", "any_true"}:
        da = ""
    payload["deactivate_applied"] = da

    items.append(payload)
    save_alarms(items)

    log.info("Alarm added id=%s symbol=%s pid=%s gid=%s", aid, payload.get("symbol"), payload.get("profile_id"), payload.get("group_id"))
    try:
        print(f"[ALARMS] add id={aid}")
    except Exception:
        pass

    return aid


def search_alarms(
    items: List[dict],
    limit: int = 100,
    offset: int = 0,
    symbol: Optional[str] = None,
    group_id: Optional[str] = None,
    profile_id: Optional[str] = None,
    since: Optional[str] = None,
) -> List[dict]:
    """
    Filtert in-memory eine Alarm-Liste nach Symbol, Gruppe, Profil und Zeit.
    Gibt die geslicete Liste (offset/limit) zurück.
    """
    filtered = list(items)

    if symbol:
        s = _norm_symbol(symbol)
        filtered = [a for a in filtered if _norm_symbol(a.get("symbol", "")) == s]

    if group_id:
        g = str(group_id).strip()
        filtered = [a for a in filtered if str(a.get("group_id", "")).strip() == g]

    if profile_id:
        p = str(profile_id).strip()
        filtered = [a for a in filtered if str(a.get("profile_id", "")).strip() == p]

    if since:
        ts_min = _parse_ts(str(since))
        if ts_min is not None:
            tmp: List[dict] = []
            for a in filtered:
                ts = _parse_ts(str(a.get("ts", "")))
                if ts is not None and ts >= ts_min:
                    tmp.append(a)
            filtered = tmp

    log.info(
        "Alarms search result_count=%d limit=%d offset=%d",
        len(filtered),
        limit,
        offset,
    )
    try:
        print(f"[ALARMS] search result_count={len(filtered)} limit={limit} offset={offset}")
    except Exception:
        pass

    return filtered[offset : offset + limit]


def delete_alarm_by_id(items: List[dict], alarm_id: str) -> List[dict]:
    """
    Löscht einen Alarm mit gegebener ID aus der Liste.
    Gibt die neue Liste zurück (ohne direkten Save).
    """
    before = len(items)
    remaining = [x for x in items if str(x.get("id")) != str(alarm_id)]
    removed = before - len(remaining)

    log.info("Alarms delete id=%s removed=%d", alarm_id, removed)
    try:
        print(f"[ALARMS] delete id={alarm_id} removed={removed}")
    except Exception:
        pass

    return remaining


def delete_alarms_older_than(items: List[dict], older_than: str) -> List[dict]:
    """
    Löscht alle Alarme mit ts < older_than aus der Liste.
    Gibt die neue Liste zurück.
    """
    ts_min = _parse_ts(str(older_than))
    if ts_min is None:
        # Wenn Timestamp nicht parsebar → nichts löschen
        log.warning("delete_alarms_older_than: invalid older_than=%s", older_than)
        return items

    before = len(items)
    keep: List[dict] = []
    for a in items:
        ts = _parse_ts(str(a.get("ts", "")))
        if ts is None or ts >= ts_min:
            keep.append(a)

    removed = before - len(keep)
    log.info("Alarms cleanup older_than=%s removed=%d", older_than, removed)
    try:
        print(f"[ALARMS] cleanup older_than={older_than} removed={removed}")
    except Exception:
        pass
    return keep


# ─────────────────────────────────────────────────────────────
# Backwards-Compatible Aliasse (falls alter Code Namen erwartet)
# ─────────────────────────────────────────────────────────────

_load_alarms = load_alarms
_save_alarms = save_alarms
_add_alarm_entry = add_alarm_entry
_search_alarms = search_alarms
_delete_alarm_by_id = delete_alarm_by_id
_delete_alarms_older_than = delete_alarms_older_than
