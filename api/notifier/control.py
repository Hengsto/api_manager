# api/notifier/control.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict

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
        d = deepcopy(_OVR_TEMPLATE)
    return d


def save_overrides(d: Dict[str, Any]) -> None:
    """
    Speichert Overrides nach OVERRIDES_NOTIFIER.
    updated_ts wird immer neu gesetzt.
    """
    payload = deepcopy(d)
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
    ovr["overrides"].setdefault(profile_id, {})
    ovr["overrides"][profile_id].setdefault(
        group_id,
        {"forced_off": False, "snooze_until": None, "note": None},
    )
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
        d = deepcopy(_CMD_TEMPLATE)
    return d


def save_commands(d: Dict[str, Any]) -> None:
    """
    Speichert die Command-Queue nach COMMANDS_NOTIFIER.
    """
    payload = deepcopy(d)
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
      - profile_id
      - group_id
      - rearm      → Gruppe neu scharf stellen
      - rebaseline → History/true_ticks neu setzen
    """
    cmds = load_commands()
    item = {
        "profile_id": str(profile_id),
        "group_id": str(group_id),
        "rearm": bool(rearm),
        "rebaseline": bool(rebaseline),
        "ts": _now_iso(),
    }
    cmds.setdefault("queue", [])
    cmds["queue"].append(item)
    save_commands(cmds)

    log.info(
        "Command enqueued pid=%s gid=%s rearm=%s rebaseline=%s",
        profile_id,
        group_id,
        rearm,
        rebaseline,
    )
    try:
        print(
            f"[CMD] enqueue pid={profile_id} gid={group_id} "
            f"rearm={rearm} rebaseline={rebaseline}"
        )
    except Exception:
        pass

    return item


# ─────────────────────────────────────────────────────────────
# Activation-Routine (nach Profil-Aktivierung)
# ─────────────────────────────────────────────────────────────

def run_activation_routine(
    profile_obj: Dict[str, Any],
    activate_flag: bool,
    rebaseline: bool,
) -> None:
    """
    Entspricht grob dem alten _run_activation_routine:

    - Nimmt ein *bereits saniertes* Profilobjekt (mit condition_groups)
    - Für jede aktive Gruppe:
        - forced_off=False, snooze_until=None im Overrides-JSON
        - Command-Queue-Eintrag (rearm + optional rebaseline)
    """
    pid = str(profile_obj.get("id") or "").strip()
    if not pid:
        log.warning("run_activation_routine called without profile_id")
        print("[ACTIVATE] skip: missing profile_id")
        return

    groups = profile_obj.get("condition_groups") or []
    if not isinstance(groups, list):
        log.warning("run_activation_routine: condition_groups is not a list for pid=%s", pid)
        print(f"[ACTIVATE] skip: bad groups type pid={pid}")
        return

    ovr = load_overrides()
    changed = 0
    enq = 0

    for g in groups:
        if not isinstance(g, dict):
            continue
        gid = str(g.get("gid") or "").strip()
        if not gid:
            continue
        if not bool(g.get("active", True)):
            # Inaktive Gruppen werden nicht scharf geschaltet
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
        print(
            f"[ACTIVATE] pid={pid} changed={changed} "
            f"enq={enq} rebaseline={rebaseline}"
        )
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
