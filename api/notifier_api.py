# api/notifier_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Body

from api.notifier.profiles import (
    ProfileRead,
    ProfileCreate,
    load_profiles_normalized,
    add_or_update_profile_by_name,
    delete_profile_by_id,
)

from api.notifier.status import (
    get_status_snapshot,
    sync_status,  # wichtig: so heißt sie in status.py
)

from api.notifier.control import (
    load_overrides,
    save_overrides,
    load_commands,
    enqueue_command,
)

from api.notifier.alarms import (
    AlarmIn,
    AlarmOut,
    load_alarms,
    save_alarms,
    add_alarm_entry,
    search_alarms,
    delete_alarm_by_id,
    delete_alarms_older_than,
)

log = logging.getLogger("notifier.api")

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

# KEIN Prefix hier – main_notifier hängt den Router unter /notifier ein.
router = APIRouter(tags=["notifier"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_profile_by_id(pid: str) -> Optional[Dict[str, Any]]:
    """Sucht ein Profil in den normalisierten Profilen per ID."""
    profiles = load_profiles_normalized()
    pid_clean = str(pid).strip()

    # Debug (prints bleiben absichtlich; zusätzlich log.debug)
    try:
        print(f"[API] _find_profile_by_id pid='{pid_clean}' profiles={len(profiles)}")
    except Exception:
        pass
    log.debug("[API] _find_profile_by_id pid='%s' profiles=%s", pid_clean, len(profiles))

    for p in profiles:
        cur = str(p.get("id") or "").strip()
        if cur == pid_clean:
            try:
                print(f"[API] _find_profile_by_id hit id='{cur}' name='{p.get('name')}'")
            except Exception:
                pass
            log.debug("[API] _find_profile_by_id HIT id='%s' name='%s'", cur, p.get("name"))
            return p

    try:
        print(f"[API] _find_profile_by_id MISS pid='{pid_clean}'")
    except Exception:
        pass
    log.debug("[API] _find_profile_by_id MISS pid='%s'", pid_clean)
    return None


# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------

@router.get("/profiles", response_model=List[ProfileRead])
def list_profiles() -> List[ProfileRead]:
    """
    Gibt alle Profile (sanitisiert) zurück.
    """
    profiles = load_profiles_normalized()
    try:
        print(f"[API] GET /profiles → count={len(profiles)}")
    except Exception:
        pass
    log.debug("[API] GET /profiles -> count=%s", len(profiles))

    # Explizit casten in ProfileRead (sanitisiert)
    return [ProfileRead(**p) for p in profiles]


@router.post("/profiles", response_model=Dict[str, Any])
def upsert_profile(profile: ProfileCreate) -> Dict[str, Any]:
    """
    Upsert nach Profil-Name (case-insensitive).
    - Wenn Name existiert → aktualisieren (ID bleibt).
    - Sonst neues Profil mit neuer ID.

    Gibt direkt das Outcome von add_or_update_profile_by_name zurück,
    damit das Frontend wie früher mit `status`/`id` arbeiten kann.
    """
    # Pydantic v1/v2 kompatibel in dict umwandeln
    payload = profile.model_dump() if hasattr(profile, "model_dump") else profile.dict()

    # Debug: Eingehende Daten
    try:
        cg_count = len(payload.get("condition_groups") or [])
        print(
            f"[API] POST /profiles name='{payload.get('name')}' "
            f"condition_groups={cg_count}"
        )
    except Exception:
        pass
    log.debug(
        "[API] POST /profiles name='%s' condition_groups=%s",
        payload.get("name"),
        len(payload.get("condition_groups") or []),
    )

    # Speichern per Name (Upsert)
    outcome = add_or_update_profile_by_name(payload)

    # Debug: Speicherausgang
    try:
        print(f"[API] POST /profiles outcome={outcome}")
    except Exception:
        pass
    log.debug("[API] POST /profiles outcome=%s", outcome)

    if not isinstance(outcome, dict) or not outcome.get("id"):
        raise HTTPException(
            status_code=500,
            detail="Profil konnte nicht gespeichert werden (Outcome ohne ID).",
        )

    # WICHTIG: direkt das Outcome zurückgeben, NICHT noch mal wrappen
    return outcome


@router.get("/profiles/{profile_id}", response_model=ProfileRead)
def get_profile(profile_id: str) -> ProfileRead:
    """
    Einzelnes Profil nach ID.
    """
    pid = str(profile_id or "").strip()
    p = _find_profile_by_id(pid)
    if not p:
        raise HTTPException(status_code=404, detail="Profil nicht gefunden.")
    try:
        print(f"[API] GET /profiles/{pid}")
    except Exception:
        pass
    log.debug("[API] GET /profiles/%s", pid)
    return ProfileRead(**p)


@router.delete("/profiles/{profile_id}", response_model=Dict[str, Any])
def api_delete_profile(profile_id: str) -> Dict[str, Any]:
    """
    Löscht ein Profil per ID.
    """
    pid = str(profile_id or "").strip()
    try:
        print(f"[API] DELETE /profiles/{pid}")
    except Exception:
        pass
    log.debug("[API] DELETE /profiles/%s", pid)

    if not pid:
        raise HTTPException(status_code=400, detail="profile_id darf nicht leer sein.")

    try:
        outcome = delete_profile_by_id(pid)

        try:
            print(f"[API] DELETE /profiles/{pid} outcome={outcome}")
        except Exception:
            pass
        log.debug("[API] DELETE /profiles/%s outcome=%s", pid, outcome)

        if not isinstance(outcome, dict) or not outcome.get("deleted"):
            raise HTTPException(status_code=404, detail="Profil nicht gefunden oder bereits gelöscht.")

        return outcome

    except HTTPException:
        raise
    except Exception as e:
        # prints bleiben, aber jetzt auch stacktrace im logger
        try:
            print(f"[API] DELETE /profiles/{pid} ERROR: {e}")
        except Exception:
            pass
        log.exception("[API] DELETE /profiles/%s ERROR", pid)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@router.get("/status", response_model=Dict[str, Any])
def api_get_status(force_fix: bool = Query(False)) -> Dict[str, Any]:
    """
    Gibt den aktuellen Status-Snapshot zurück.
    - force_fix=True → Status wird vorher gegen Profile gesynct.
    """
    snap = get_status_snapshot(force_fix=force_fix)
    try:
        print(f"[API] GET /status force_fix={force_fix} profiles={len((snap.get('profiles') or {}))}")
    except Exception:
        pass
    log.debug(
        "[API] GET /status force_fix=%s profiles=%s",
        force_fix,
        len((snap.get("profiles") or {})),
    )
    return snap


@router.post("/status/sync", response_model=Dict[str, Any])
def api_sync_status(
    profiles: Optional[List[Dict[str, Any]]] = Body(None),
) -> Dict[str, Any]:
    """
    Synchronisiert Status mit Profilen.
    - Wenn profiles=None → lokale Profile verwenden.
    - Wenn profiles übergeben → diese explizit als Basis für den Sync verwenden.
    """
    try:
        print(f"[API] POST /status/sync body_profiles={0 if profiles is None else len(profiles)}")
    except Exception:
        pass
    log.debug(
        "[API] POST /status/sync body_profiles=%s",
        0 if profiles is None else len(profiles),
    )

    if profiles is not None:
        if not isinstance(profiles, list):
            raise HTTPException(status_code=400, detail="profiles muss eine Liste sein oder null.")
        # Drop-in, aber schützt dich vor kaputten Payloads
        for i, item in enumerate(profiles):
            if not isinstance(item, dict):
                raise HTTPException(status_code=400, detail=f"profiles[{i}] muss ein Objekt (dict) sein.")

    snap = sync_status(profiles=profiles)  # status.sync_status
    return snap


# ---------------------------------------------------------------------------
# Overrides
# ---------------------------------------------------------------------------

@router.get("/overrides", response_model=Dict[str, Any])
def api_get_overrides() -> Dict[str, Any]:
    """
    Gibt das Overrides-JSON zurück.
    """
    data = load_overrides()
    try:
        print(f"[API] GET /overrides profiles={len(data.get('overrides', {}))}")
    except Exception:
        pass
    log.debug("[API] GET /overrides profiles=%s", len(data.get("overrides", {})))
    return data


@router.post("/overrides", response_model=Dict[str, Any])
def api_set_overrides(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Überschreibt das Overrides-JSON vollständig.
    """
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload muss ein JSON-Objekt sein.")
    save_overrides(payload)
    try:
        print(f"[API] POST /overrides profiles={len(payload.get('overrides', {}))}")
    except Exception:
        pass
    log.debug("[API] POST /overrides profiles=%s", len(payload.get("overrides", {})))
    return load_overrides()


# ---------------------------------------------------------------------------
# Commands (Queue für Evaluator / Alarm-Worker)
# ---------------------------------------------------------------------------

@router.get("/commands", response_model=Dict[str, Any])
def api_get_commands() -> Dict[str, Any]:
    """
    Gibt die aktuelle Command-Queue zurück.
    """
    data = load_commands()
    try:
        print(f"[API] GET /commands queue_len={len(data.get('queue', []))}")
    except Exception:
        pass
    log.debug("[API] GET /commands queue_len=%s", len(data.get("queue", [])))
    return data


@router.post("/commands/enqueue", response_model=Dict[str, Any])
def api_enqueue_command(
    profile_id: str = Body(..., embed=True),
    group_id: str = Body(..., embed=True),
    rearm: bool = Body(True, embed=True),
    rebaseline: bool = Body(False, embed=True),
) -> Dict[str, Any]:
    """
    Fügt einen Command in die Queue ein (rearm/rebaseline).
    """
    cmd = enqueue_command(
        profile_id=profile_id,
        group_id=group_id,
        rearm=rearm,
        rebaseline=rebaseline,
    )
    try:
        print(f"[API] POST /commands/enqueue pid={profile_id} gid={group_id} rearm={rearm} rebaseline={rebaseline}")
    except Exception:
        pass
    log.debug(
        "[API] POST /commands/enqueue pid=%s gid=%s rearm=%s rebaseline=%s",
        profile_id, group_id, rearm, rebaseline
    )
    return {"enqueued": cmd}


# ---------------------------------------------------------------------------
# Alarms (optional auch über eigenen Router /alarms_api zugänglich)
# ---------------------------------------------------------------------------

@router.get("/alarms", response_model=List[AlarmOut])
def api_list_alarms(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = Query(None),
    group_id: Optional[str] = Query(None),
    profile_id: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
) -> List[AlarmOut]:
    """
    Listet Alarme aus der Historie mit optionalen Filtern.
    """
    items = load_alarms()
    filtered = search_alarms(
        items=items,
        limit=limit,
        offset=offset,
        symbol=symbol,
        group_id=group_id,
        profile_id=profile_id,
        since=since,
    )
    try:
        print(f"[API] GET /alarms result_count={len(filtered)} limit={limit} offset={offset}")
    except Exception:
        pass
    log.debug(
        "[API] GET /alarms result_count=%s limit=%s offset=%s symbol=%s group_id=%s profile_id=%s since=%s",
        len(filtered), limit, offset, symbol, group_id, profile_id, since
    )
    return [AlarmOut(**a) for a in filtered]


@router.post("/alarms", response_model=AlarmOut)
def api_add_alarm(alarm: AlarmIn) -> AlarmOut:
    """
    Fügt einen Alarm hinzu.
    """
    payload = alarm.model_dump() if hasattr(alarm, "model_dump") else alarm.dict()
    aid = add_alarm_entry(payload)

    # Drop-in Verhalten: wir laden wie vorher erneut und suchen den Eintrag
    items = load_alarms()
    created = None
    for a in items:
        if str(a.get("id")) == str(aid):
            created = a
            break

    if not created:
        raise HTTPException(status_code=500, detail="Alarm-ID erstellt, aber Alarm nicht auffindbar.")

    try:
        print(f"[API] POST /alarms id={aid}")
    except Exception:
        pass
    log.debug("[API] POST /alarms id=%s", aid)

    return AlarmOut(**created)


@router.delete("/alarms/{alarm_id}", response_model=Dict[str, Any])
def api_delete_alarm(alarm_id: str) -> Dict[str, Any]:
    """
    Löscht einen Alarm per ID.
    """
    items = load_alarms()
    remaining = delete_alarm_by_id(items, alarm_id)
    save_alarms(remaining)
    removed = len(items) - len(remaining)

    try:
        print(f"[API] DELETE /alarms/{alarm_id} removed={removed}")
    except Exception:
        pass
    log.debug("[API] DELETE /alarms/%s removed=%s", alarm_id, removed)

    if removed == 0:
        raise HTTPException(status_code=404, detail="Alarm nicht gefunden oder bereits gelöscht.")
    return {"removed": removed}


@router.delete("/alarms", response_model=Dict[str, Any])
def api_cleanup_alarms(
    older_than: str = Query(..., description="ISO-Zeitstempel, alle älteren Alarme werden gelöscht."),
) -> Dict[str, Any]:
    """
    Löscht alle Alarme, deren ts < older_than ist.
    """
    items = load_alarms()
    remaining = delete_alarms_older_than(items, older_than)
    removed = len(items) - len(remaining)
    save_alarms(remaining)

    try:
        print(f"[API] DELETE /alarms?older_than={older_than} removed={removed}")
    except Exception:
        pass
    log.debug("[API] DELETE /alarms older_than=%s removed=%s remaining=%s", older_than, removed, len(remaining))

    return {"removed": removed, "remaining": len(remaining)}
