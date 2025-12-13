# api/notifier/alarms.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pathlib import Path

from pydantic import BaseModel, Field

from config import ALARMS_NOTIFIER
from storage import load_json, save_json, atomic_update_json_list

log = logging.getLogger("notifier.alarms")


# ─────────────────────────────────────────────────────────────
# Zeit / Helfer
# ─────────────────────────────────────────────────────────────

def _now_iso() -> str:
    """
    Gibt aktuellen UTC-Timestamp im ISO-Format zurück.
    """
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
    x = str(s).strip()
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


def _norm_symbol(s: str) -> str:
    """
    Normiert Symbole (Ticker) konsistent:
    - Unicode-Normalisierung (NFKC)
    - trim
    - upper()
    """
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s).strip()
    return s.upper()


# ─────────────────────────────────────────────────────────────
# Pydantic v1/v2-tolerantes Basismodell (extra='allow')
# ─────────────────────────────────────────────────────────────

try:
    from pydantic import ConfigDict  # type: ignore
    _IS_PYD_V2 = True
except Exception:  # pragma: no cover
    ConfigDict = None  # type: ignore[assignment]
    _IS_PYD_V2 = False


class ApiModel(BaseModel):
    """
    Basismodell für API-Modelle mit v1/v2-Kompatibilität
    und extra='allow', analog zur alten Monolith-API.
    """
    if _IS_PYD_V2:
        model_config = ConfigDict(extra="allow")  # type: ignore[assignment]
    else:
        class Config:
            extra = "allow"


# ─────────────────────────────────────────────────────────────
# Pydantic-Modelle (werden vom API-Layer benutzt)
# ─────────────────────────────────────────────────────────────

class AlarmBase(ApiModel):
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
    Erzeugt für Einträge ohne ID eine UUID.
    WICHTIG: Persistiert generierte IDs, damit Deletes stabil sind.
    """
    data = load_json(ALARMS_NOTIFIER, [])
    if not isinstance(data, list):
        log.warning("load_alarms: expected list, got %s → fallback", type(data).__name__)
        data = []

    out: List[dict] = []
    changed = False

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
                changed = True
            except Exception:
                a["matched"] = []
                changed = True

        # meta: ggf. von String → Dict
        if isinstance(a["meta"], str):
            try:
                parsed_meta = json.loads(a["meta"])
                a["meta"] = parsed_meta if isinstance(parsed_meta, dict) else {}
                changed = True
            except Exception:
                a["meta"] = {}
                changed = True

        # deactivate_applied normalisieren
        if a.get("deactivate_applied") not in {"", "true", "any_true"}:
            a["deactivate_applied"] = ""
            changed = True

        # ID sicherstellen (Legacy-Einträge ohne ID bekommen eine UUID)
        raw_id = str(a.get("id") or "").strip()
        if not raw_id:
            new_id = str(uuid.uuid4())
            a["id"] = new_id
            changed = True
            log.debug("load_alarms: generated missing id=%s", new_id)
            try:
                print(f"[ALARMS] load: generated id={new_id}")
            except Exception:
                pass

        out.append(a)

    # Persistiere Normalisierung + generierte IDs, sonst sind IDs nicht stabil.
    if changed:
        try:
            save_alarms(out)
            try:
                print(f"[ALARMS] load: persisted normalization (changed=True) count={len(out)}")
            except Exception:
                pass
        except Exception as e:
            log.warning("load_alarms: failed to persist normalization: %s", e)
            try:
                print(f"[ALARMS] load: persist FAILED err={e}")
            except Exception:
                pass

    log.info("Alarms loaded count=%d", len(out))
    try:
        print(f"[ALARMS] load count={len(out)} changed={changed}")
    except Exception:
        pass
    return out


def save_alarms(items: List[dict]) -> None:
    """
    Speichert Alarm-Liste nach ALARMS_NOTIFIER.
    """
    if not isinstance(items, list):
        log.warning("save_alarms: expected list, got %s → coercing []", type(items).__name__)
        items = []
    save_json(ALARMS_NOTIFIER, items)
    log.info("Alarms saved count=%d", len(items))
    try:
        print(f"[ALARMS] save count={len(items)} path={ALARMS_NOTIFIER}")
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

    WICHTIG: atomic write, damit parallele Adds keine Alarme verlieren.
    """
    payload = dict(alarm_payload or {})

    ts_raw = str(payload.get("ts") or "").strip()
    payload["ts"] = ts_raw or _now_iso()

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

    # ID – robust via UUID
    aid = str(payload.get("id") or "").strip()
    if not aid:
        aid = str(uuid.uuid4())
    payload["id"] = aid

    # deactivate_applied normalisieren
    da = str(payload.get("deactivate_applied") or "").strip().lower()
    if da not in {"", "true", "any_true"}:
        da = ""
    payload["deactivate_applied"] = da

    def _transform(current: list):
        items = [x for x in (current or []) if isinstance(x, dict)]
        items.append(payload)
        result = {"status": "added", "id": aid, "count": len(items)}
        return items, result

    try:
        _, outcome = atomic_update_json_list(Path(ALARMS_NOTIFIER), _transform)
        try:
            print(f"[ALARMS] add atomic outcome={outcome}")
        except Exception:
            pass
    except Exception as e:
        # Fallback: altes Verhalten (nicht atomic), aber nicht kaputt
        log.warning("add_alarm_entry atomic_update failed (%s) -> fallback load/save", e)
        try:
            items = load_alarms()
            items.append(payload)
            save_alarms(items)
            try:
                print(f"[ALARMS] add FALLBACK load/save because atomic failed: {e}")
            except Exception:
                pass
        except Exception as ee:
            log.exception("add_alarm_entry fallback failed: %s", ee)
            raise

    log.info(
        "Alarm added id=%s symbol=%s pid=%s gid=%s",
        aid,
        payload.get("symbol"),
        payload.get("profile_id"),
        payload.get("group_id"),
    )
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
    filtered = list(items or [])

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

    return filtered[offset: offset + limit]


def delete_alarm_by_id(items: List[dict], alarm_id: str) -> List[dict]:
    """
    Löscht einen Alarm mit gegebener ID aus der Liste.
    Gibt die neue Liste zurück (ohne direkten Save).
    """
    aid = str(alarm_id).strip()
    before = len(items or [])
    remaining = [x for x in (items or []) if str((x or {}).get("id", "")).strip() != aid]
    removed = before - len(remaining)

    log.info("Alarms delete id=%s removed=%d", aid, removed)
    try:
        print(f"[ALARMS] delete id={aid} removed={removed}")
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
        try:
            print(f"[ALARMS] cleanup skipped: invalid older_than={older_than}")
        except Exception:
            pass
        return items

    before = len(items or [])
    keep: List[dict] = []
    for a in (items or []):
        ts = _parse_ts(str((a or {}).get("ts", "")))
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
