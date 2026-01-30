# api/notifier/control.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict
import uuid  # für eindeutige Command-IDs

from config import OVERRIDES_NOTIFIER, COMMANDS_NOTIFIER
from storage import load_json_any, save_json_any

log = logging.getLogger("notifier.control")


# ─────────────────────────────────────────────────────────────
# Templates / Zeit-Helper
# ─────────────────────────────────────────────────────────────

_OVR_TEMPLATE: Dict[str, Any] = {"overrides": {}, "updated_ts": None}
_CMD_TEMPLATE: Dict[str, Any] = {"queue": []}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")


def _atomic_update_json_dict(path: str, transform, default: Dict[str, Any]) -> Dict[str, Any]:
    """
    Atomic-ish update for JSON dict files.

    Reads dict (or uses default), applies transform(d) -> (new_dict, outcome),
    then writes via save_json_any.

    NOTE:
    - This keeps the on-disk format stable: always a DICT (not a list-wrapper).
    - True atomicity depends on save_json_any implementation.
    """
    cur = load_json_any(path, deepcopy(default))
    if not isinstance(cur, dict):
        log.warning(
            "[CTRL] atomic_update_json_dict: file is not dict (%s) -> reset default",
            type(cur).__name__,
        )
        cur = deepcopy(default)

    try:
        new_doc, outcome = transform(deepcopy(cur))
    except Exception as e:
        log.exception("[CTRL] atomic_update_json_dict transform failed: %s", e)
        try:
            print(f"[CTRL] atomic_update_json_dict transform ERROR: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise

    if not isinstance(new_doc, dict):
        raise ValueError("atomic_update_json_dict: transform must return a dict as new_doc")

    save_json_any(path, new_doc)

    try:
        print(f"[CTRL] atomic_update_json_dict saved path={path} keys={list(new_doc.keys())[:10]}")
    except Exception:
        pass

    return outcome


# ─────────────────────────────────────────────────────────────
# Overrides (forced_off / snooze / note)
# ─────────────────────────────────────────────────────────────

def load_overrides() -> Dict[str, Any]:
    """
    Lädt das Overrides-JSON aus OVERRIDES_NOTIFIER.
    Stellt sicher, dass die Struktur mindestens {"overrides": {}} ist.
    """
    d = load_json_any(OVERRIDES_NOTIFIER, deepcopy(_OVR_TEMPLATE))
    if not isinstance(d, dict) or "overrides" not in d:
        log.warning(
            "load_overrides: invalid structure (%s) → using template",
            type(d).__name__,
        )
        d = deepcopy(_OVR_TEMPLATE)

    # härtung: overrides muss dict sein
    if not isinstance(d.get("overrides"), dict):
        d["overrides"] = {}

    try:
        print(
            f"[OVR] load profiles={len(d.get('overrides', {}))} "
            f"ts={d.get('updated_ts')}"
        )
    except Exception:
        pass

    return d


def save_overrides(d: Dict[str, Any]) -> None:
    """
    Speichert Overrides nach OVERRIDES_NOTIFIER.
    updated_ts wird immer neu gesetzt.
    """
    payload = deepcopy(d)
    if not isinstance(payload, dict):
        payload = deepcopy(_OVR_TEMPLATE)

    payload.setdefault("overrides", {})
    if not isinstance(payload.get("overrides"), dict):
        payload["overrides"] = {}

    payload["updated_ts"] = _now_iso()
    save_json_any(OVERRIDES_NOTIFIER, payload)
    log.info("Overrides saved. profiles=%d", len(payload.get("overrides", {})))
    try:
        print(
            f"[OVR] save profiles={len(payload.get('overrides', {}))} "
            f"ts={payload.get('updated_ts')}"
        )
    except Exception:
        pass


def ensure_override_slot(
    ovr: Dict[str, Any],
    profile_id: str,
    group_id: str,
) -> Dict[str, Any]:
    """
    Sorgt dafür, dass es für (profile_id, group_id) einen Override-Eintrag gibt.
    Struktur:
      ovr["overrides"][pid][gid] = {
        "forced_off": bool,
        "snooze_until": str|None,
        "note": str|None,
      }
    """
    ovr.setdefault("overrides", {})
    if not isinstance(ovr.get("overrides"), dict):
        ovr["overrides"] = {}

    ovr["overrides"].setdefault(profile_id, {})
    if not isinstance(ovr["overrides"].get(profile_id), dict):
        ovr["overrides"][profile_id] = {}

    ovr["overrides"][profile_id].setdefault(
        group_id,
        {"forced_off": False, "snooze_until": None, "note": None},
    )

    try:
        print(
            f"[OVR] ensure-slot pid={profile_id} gid={group_id} "
            f"forced_off={ovr['overrides'][profile_id][group_id]['forced_off']} "
            f"snooze_until={ovr['overrides'][profile_id][group_id]['snooze_until']}"
        )
    except Exception:
        pass

    return ovr["overrides"][profile_id][group_id]


# ─────────────────────────────────────────────────────────────
# Commands (Queue für Evaluator / Alarm-Worker)
# ─────────────────────────────────────────────────────────────

def load_commands() -> Dict[str, Any]:
    """
    Lädt die Command-Queue aus COMMANDS_NOTIFIER.
    Struktur: {"queue": [ ... ]}
    """
    d = load_json_any(COMMANDS_NOTIFIER, deepcopy(_CMD_TEMPLATE))
    if not isinstance(d, dict) or "queue" not in d:
        log.warning(
            "load_commands: invalid structure (%s) → using template",
            type(d).__name__,
        )
        d = deepcopy(_CMD_TEMPLATE)

    # härtung: queue muss list sein
    if not isinstance(d.get("queue"), list):
        d["queue"] = []

    try:
        print(f"[CMD] load queue_len={len(d.get('queue', []))}")
    except Exception:
        pass

    return d


def save_commands(d: Dict[str, Any]) -> None:
    """
    Speichert die Command-Queue nach COMMANDS_NOTIFIER.
    """
    payload = deepcopy(d)
    if not isinstance(payload, dict):
        payload = deepcopy(_CMD_TEMPLATE)

    payload.setdefault("queue", [])
    if not isinstance(payload.get("queue"), list):
        payload["queue"] = []

    save_json_any(COMMANDS_NOTIFIER, payload)
    log.info("Commands saved. queue_len=%d", len(payload.get("queue", [])))
    try:
        print(f"[CMD] save queue_len={len(payload.get('queue', []))}")
    except Exception:
        pass


def enqueue_command(
    profile_id: str,
    group_id: str,
    rearm: bool = True,
    rebaseline: bool = False,
) -> Dict[str, Any]:
    """
    Fügt einen Befehl für den Evaluator / Alarm-Worker in die Queue ein.

    Felder:
      - id         → eindeutige UUID
      - profile_id
      - group_id
      - rearm      → Gruppe neu scharf stellen
      - rebaseline → History/threshold_state neu setzen
    """
    item = {
        "id": str(uuid.uuid4()),
        "profile_id": str(profile_id),
        "group_id": str(group_id),
        "rearm": bool(rearm),
        "rebaseline": bool(rebaseline),
        "ts": _now_iso(),
    }

    try:
        print(
            f"[CMD] enqueue request pid={profile_id} gid={group_id} "
            f"rearm={rearm} rebaseline={rebaseline}"
        )
    except Exception:
        pass

    def _transform(doc: Dict[str, Any]):
        doc = deepcopy(doc)
        doc.setdefault("queue", [])
        if not isinstance(doc.get("queue"), list):
            doc["queue"] = []

        doc["queue"].append(item)

        result = {
            "status": "enqueued",
            "id": item["id"],
            "queue_len": len(doc["queue"]),
        }
        return doc, result

    # Dict-only update. If COMMANDS_NOTIFIER is not dict on disk, we reset to template and continue.
    outcome = _atomic_update_json_dict(
        COMMANDS_NOTIFIER,
        _transform,
        default=deepcopy(_CMD_TEMPLATE),
    )

    log.info(
        "Command enqueued id=%s pid=%s gid=%s rearm=%s rebaseline=%s queue_len=%s",
        item["id"],
        profile_id,
        group_id,
        rearm,
        rebaseline,
        outcome.get("queue_len"),
    )
    try:
        print(f"[CMD] enqueue outcome={outcome}")
    except Exception:
        pass

    return item


# ─────────────────────────────────────────────────────────────
# Activation-Routine (nach Profil-Aktivierung) – NEW SCHEMA
# ─────────────────────────────────────────────────────────────

def run_activation_routine(
    profile_obj: Dict[str, Any],
    activate_flag: bool,
    rebaseline: bool,
) -> None:
    """
    Entspricht grob dem alten _run_activation_routine:

    - Wenn activate_flag False → NO-OP (nur Logging/Debug)
    - Nimmt ein Profilobjekt im NEW SCHEMA (mit groups)
    - Für jede aktive Gruppe:
        - forced_off=False, snooze_until=None im Overrides-JSON
        - Command-Queue-Eintrag (rearm + optional rebaseline)
    """
    if not activate_flag:
        log.info("run_activation_routine called with activate_flag=False → no-op")
        try:
            print("[ACTIVATE] skip: activate_flag=False")
        except Exception:
            pass
        return

    pid = str(profile_obj.get("id") or "").strip()
    if not pid:
        log.warning("run_activation_routine called without profile_id")
        try:
            print("[ACTIVATE] skip: missing profile_id")
        except Exception:
            pass
        return

    # NEW SCHEMA: groups
    groups = profile_obj.get("groups") or []
    if not isinstance(groups, list):
        log.warning("run_activation_routine: groups is not a list for pid=%s", pid)
        try:
            print(f"[ACTIVATE] skip: bad groups type pid={pid} type={type(groups).__name__}")
        except Exception:
            pass
        return

    ovr = load_overrides()
    changed = 0
    enq = 0

    try:
        print(f"[ACTIVATE] start pid={pid} groups_in={len(groups)} rebaseline={rebaseline}")
    except Exception:
        pass

    for g in groups:
        if not isinstance(g, dict):
            continue

        gid = str(g.get("gid") or "").strip()
        if not gid:
            continue

        if not bool(g.get("active", True)):
            # Inaktive Gruppen werden nicht scharf geschaltet
            try:
                print(f"[ACTIVATE] skip group pid={pid} gid={gid} active=False")
            except Exception:
                pass
            continue

        slot = ensure_override_slot(ovr, pid, gid)
        slot["forced_off"] = False
        slot["snooze_until"] = None
        changed += 1

        enqueue_command(pid, gid, rearm=True, rebaseline=rebaseline)
        enq += 1

    if changed > 0:
        save_overrides(ovr)

    log.info(
        "Activation routine pid=%s groups_changed=%d enqueued=%d rebaseline=%s",
        pid,
        changed,
        enq,
        rebaseline,
    )
    try:
        print(f"[ACTIVATE] done pid={pid} changed={changed} enq={enq} rebaseline={rebaseline}")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Backwards-Compatible Aliasse (falls alter Code sie erwartet)
# ─────────────────────────────────────────────────────────────

_load_overrides = load_overrides
_save_overrides = save_overrides
_ensure_ovr_slot = ensure_override_slot
_load_commands = load_commands
_save_commands = save_commands
_enqueue_command = enqueue_command
_run_activation_routine = run_activation_routine
