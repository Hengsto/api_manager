# api/notifier/status.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import STATUS_NOTIFIER
from storage import load_json_any, save_json_any
from api.notifier.profiles import (
    load_profiles_normalized,
    profiles_fingerprint,
    _sanitize_profiles,
    _normalize_deactivate_value,
)

log = logging.getLogger("notifier.status")


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")


def _label_only_conditions(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Erzeugt eine einfache, UI-taugliche Cond-Liste (ohne Werte) für den Status-Snapshot.
    """
    out: List[Dict[str, Any]] = []
    for c in (group.get("conditions") or []):
        if not isinstance(c, dict):
            continue
        left = (c.get("left") or "").strip() or "—"
        right = (c.get("right") or "").strip()

        if not right:
            rsym = (c.get("right_symbol") or "").strip()
            rinv = (c.get("right_interval") or "").strip()
            rout = (c.get("right_output") or "").strip()
            if rsym:
                parts = [rsym]
                if rinv:
                    parts.append(f"@{rinv}")
                if rout:
                    parts.append(f":{rout}")
                right = "".join(parts)
        right = right or "—"
        op = (c.get("op") or "gt").strip().lower()

        out.append(
            {
                "left": left,
                "right": right,
                "left_spec": None,
                "right_spec": None,
                "left_output": None,
                "right_output": None,
                "left_col": None,
                "right_col": None,
                "op": op,
                "passed": False,
                "left_value": None,
                "right_value": None,
                "left_ts": None,
                "right_ts": None,
                "eval_ms": None,
                "error": None,
            }
        )
    return out


# ─────────────────────────────────────────────────────────────
# Status-IO
# ─────────────────────────────────────────────────────────────

def load_status_any() -> Dict[str, Any]:
    """
    Lädt den aktuellen Status-Snapshot aus STATUS_NOTIFIER.
    Stellt sicher, dass Struktur & Typen halbwegs passen.
    """
    default = {
        "version": 1,
        "flavor": "notifier-api",
        "updated_ts": _now_iso(),
        "profiles": {},
    }
    d = load_json_any(STATUS_NOTIFIER, default)
    if not isinstance(d, dict):
        d = deepcopy(default)

    if "profiles" not in d or not isinstance(d["profiles"], dict):
        d["profiles"] = {}

    d.setdefault("profiles_fp", "")
    try:
        d["version"] = int(d.get("version", 1))
    except Exception:
        d["version"] = 1
    d.setdefault("flavor", "notifier-api")
    return d


def save_status_any(data: Dict[str, Any]) -> None:
    """
    Speichert einen Status-Snapshot nach STATUS_NOTIFIER.
    updated_ts/version/flavor werden stabilisiert.
    """
    data = deepcopy(data)
    data["updated_ts"] = _now_iso()
    try:
        data["version"] = int(data.get("version", 1))
    except Exception:
        data["version"] = 1
    data.setdefault("flavor", "notifier-api")
    save_json_any(STATUS_NOTIFIER, data)


# ─────────────────────────────────────────────────────────────
# Skeleton aus Profilen
# ─────────────────────────────────────────────────────────────

def build_status_skeleton_from_profiles(profiles: list[dict]) -> Dict[str, Any]:
    """
    Baut aus einer Profil-Liste ein "leeres" Status-Skeleton:
    - Struktur/IDs/Groups/Conditions sind drin
    - Runtime-Felder sind frisch (True/False/None/0)
    """
    sanitized = _sanitize_profiles(profiles or [])
    profiles_map: dict[str, dict] = {}

    for p in sanitized:
        pid = str(p.get("id") or "")
        if not pid:
            continue

        gmap: dict[str, dict] = {}
        for g in (p.get("condition_groups") or []):
            gid = str(g.get("gid") or "")
            if not gid:
                continue

            g_entry = {
                "name": g.get("name") or gid,
                "group_active": bool(g.get("active", True)),
                "effective_active": bool(g.get("active", True)),
                "blockers": [],
                "auto_disabled": False,
                "cooldown_until": None,
                "fresh": True,
                "aggregate": {
                    "min_true_ticks": g.get("min_true_ticks"),
                    "deactivate_on": (
                        _normalize_deactivate_value(g.get("deactivate_on"))
                        or ("true" if g.get("auto_deactivate") else "always")
                    ),
                },
                "runtime": {
                    "true_ticks": None,
                    "met": 0,
                    "total": len(g.get("conditions") or []),
                    "details": [],
                },
                "last_eval_ts": None,
                "last_bar_ts": None,
                "conditions": _label_only_conditions(g),
                "conditions_status": [],
                # Für UI sichtbar
                "symbols": list(g.get("symbols") or []),
                "profiles": list(g.get("profiles") or []),
            }

            gmap[gid] = g_entry

        profiles_map[pid] = {
            "id": pid,
            "name": p.get("name") or pid,
            "profile_active": bool(p.get("enabled", True)),
            "groups": gmap,
        }

    skeleton = {
        "version": 1,
        "flavor": "notifier-api",
        "updated_ts": _now_iso(),
        "profiles": profiles_map,
    }

    try:
        print(
            f"[STATUS] skeleton built profiles={len(profiles_map)} "
            f"groups={sum(len(v.get('groups', {})) for v in profiles_map.values())}"
        )
    except Exception:
        pass

    return skeleton


# ─────────────────────────────────────────────────────────────
# Merge: Skeleton + alter Status (Runtime behalten)
# ─────────────────────────────────────────────────────────────

def merge_status_keep_runtime(old: Dict[str, Any], skel: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merged ein neues Skeleton in einen alten Status:
    - neue/gelöschte Profile/Groups werden berücksichtigt
    - Runtime-Daten (true_ticks, conditions_status, etc.) werden bestmöglich behalten
    """
    old_profiles = old.get("profiles") if isinstance(old.get("profiles"), dict) else {}
    skel_profiles = skel.get("profiles") if isinstance(skel.get("profiles"), dict) else {}

    new_out: Dict[str, Any] = {
        "version": int(old.get("version", 1)) if isinstance(old.get("version"), int) else 1,
        "flavor": "notifier-api",
        "profiles": {},
    }

    for pid, p_s in (skel_profiles or {}).items():
        old_p = old_profiles.get(pid) or {}
        new_p = {
            "id": p_s.get("id", pid),
            "name": p_s.get("name") or old_p.get("name") or pid,
            "profile_active": bool(p_s.get("profile_active", old_p.get("profile_active", True))),
            "groups": {},
        }

        old_groups = old_p.get("groups") if isinstance(old_p.get("groups"), dict) else {}
        skel_groups = p_s.get("groups") if isinstance(p_s.get("groups"), dict) else {}

        for gid, g_s in (skel_groups or {}).items():
            old_g = old_groups.get(gid) or {}
            agg_old = old_g.get("aggregate") if isinstance(old_g.get("aggregate"), dict) else {}
            rt_old = old_g.get("runtime") if isinstance(old_g.get("runtime"), dict) else {}
            agg_s = g_s.get("aggregate") if isinstance(g_s.get("aggregate"), dict) else {}

            # Legacy-Fallback: notify_mode (alt) -> deactivate_on (neu)
            old_deactivate_on = (
                agg_old.get("deactivate_on")
                if "deactivate_on" in (agg_old or {})
                else (agg_old.get("notify_mode") if isinstance(agg_old, dict) else None)
            )
            new_deactivate_on = (
                agg_s.get("deactivate_on")
                if "deactivate_on" in (agg_s or {})
                else old_deactivate_on
            ) or "always"

            new_g = {
                "name": g_s.get("name") or old_g.get("name") or gid,
                "group_active": bool(g_s.get("group_active", old_g.get("group_active", True))),
                "effective_active": bool(
                    g_s.get("effective_active", old_g.get("effective_active", True))
                ),
                "blockers": old_g.get("blockers", []) if isinstance(old_g.get("blockers"), list) else [],
                "auto_disabled": bool(old_g.get("auto_disabled", False)),
                "cooldown_until": old_g.get("cooldown_until", None),
                "fresh": bool(old_g.get("fresh", True)),
                "aggregate": {
                    "min_true_ticks": agg_s.get("min_true_ticks", agg_old.get("min_true_ticks")),
                    "deactivate_on": new_deactivate_on,
                },
                # Runtime unverändert beibehalten
                "runtime": rt_old,
                # Aktuelle Bedingungen aus dem Skeleton
                "conditions": g_s.get("conditions", []),
                "conditions_status": old_g.get("conditions_status", [])
                if isinstance(old_g.get("conditions_status"), list)
                else [],
                "last_eval_ts": old_g.get("last_eval_ts", None),
                "last_bar_ts": old_g.get("last_bar_ts", None),
            }

            # Sichtbare Felder aus Skeleton übernehmen
            for _k in ("symbols", "profiles"):
                if _k in g_s:
                    new_g[_k] = list(g_s.get(_k) or [])

            if "min_tick" in g_s:
                new_g["min_tick"] = g_s.get("min_tick")
            if "single_mode" in g_s:
                new_g["single_mode"] = g_s.get("single_mode")

            new_p["groups"][gid] = new_g

        new_out["profiles"][pid] = new_p

    # Diagnostics: was wurde gepruned?
    old_pids = set(old_profiles.keys())
    new_pids = set(new_out["profiles"].keys())
    pruned_pids = sorted(list(old_pids - new_pids))

    pruned_groups_total = 0
    details_groups: List[str] = []
    for pid in sorted(list(old_pids & new_pids)):
        old_gids = (
            set((old_profiles.get(pid, {}) or {}).get("groups", {}).keys())
            if isinstance((old_profiles.get(pid, {}) or {}).get("groups"), dict)
            else set()
        )
        new_gids = (
            set((new_out["profiles"].get(pid, {}) or {}).get("groups", {}).keys())
            if isinstance((new_out["profiles"].get(pid, {}) or {}).get("groups"), dict)
            else set()
        )
        gone = sorted(list(old_gids - new_gids))
        if gone:
            pruned_groups_total += len(gone)
            preview = ", ".join(gone[:5]) + ("..." if len(gone) > 5 else "")
            details_groups.append(f"{pid}: {preview}")

    log.info(
        "Status merged (pruned). profiles=%d pruned_profiles=%d pruned_groups=%d",
        len(new_out["profiles"]),
        len(pruned_pids),
        pruned_groups_total,
    )
    print(f"[STATUS] merged pruned profiles={len(new_out['profiles'])}")
    if pruned_pids:
        print(
            f"[STATUS] pruned profile IDs: {pruned_pids[:5]}"
            f"{'...' if len(pruned_pids) > 5 else ''}"
        )
    if pruned_groups_total:
        for line in details_groups[:10]:
            print(f"[STATUS] pruned groups -> {line}")
        if len(details_groups) > 10:
            print(
                f"[STATUS] pruned groups (more): "
                f"{len(details_groups) - 10} pid-lines omitted"
            )

    return new_out


# ─────────────────────────────────────────────────────────────
# High-Level: Auto-Fix / Sync / Get
# ─────────────────────────────────────────────────────────────

def status_autofix_merge() -> None:
    """
    Lädt Profile → baut Skeleton → merged in aktuellen Status → speichert.
    Nutzt profiles_fingerprint, damit /status sehen kann, ob was veraltet ist.
    """
    profiles = load_profiles_normalized()
    skeleton = build_status_skeleton_from_profiles(profiles)
    current = load_status_any()
    merged = merge_status_keep_runtime(current, skeleton)
    merged["profiles_fp"] = profiles_fingerprint(profiles)
    save_status_any(merged)
    log.info(
        "Status auto-fix merge done. profiles_fp=%s",
        merged.get("profiles_fp", "")[:8],
    )
    print(f"[STATUS] autofix done fp={merged.get('profiles_fp', '')[:8]}")


# Legacy-Name für alten Code
_status_autofix_merge = status_autofix_merge


def sync_status(profiles: Optional[list] = None) -> Dict[str, Any]:
    """
    Entspricht grob dem alten POST /status/sync:
    - optional Profile von außen (Body) → sonst local load
    - Skeleton + Merge + Save
    """
    if not isinstance(profiles, list):
        profiles = load_profiles_normalized()
    skeleton = build_status_skeleton_from_profiles(profiles)
    current = load_status_any()
    merged = merge_status_keep_runtime(current, skeleton)
    merged["profiles_fp"] = profiles_fingerprint(profiles)
    save_status_any(merged)
    log.info("status sync profiles=%d", len(merged.get("profiles", {})))
    print(f"[STATUS] sync profiles={len(merged.get('profiles', {}))}")
    return merged


def get_status_snapshot(force_fix: bool = False) -> Dict[str, Any]:
    """
    Logik hinter GET /status:
    - prüft, ob Status zu Profilen passt
    - auto-fix, wenn nötig oder force_fix=True
    """
    snap = load_status_any()
    try:
        need_fix = bool(force_fix)
        profiles = load_profiles_normalized()
        skeleton = build_status_skeleton_from_profiles(profiles)
        fp = profiles_fingerprint(profiles)

        if not need_fix:
            if not snap.get("profiles"):
                need_fix = True
            else:
                if snap.get("profiles_fp", "") != fp:
                    need_fix = True
                else:
                    for pid, p_s in (skeleton.get("profiles") or {}).items():
                        sp = (snap.get("profiles") or {}).get(pid, {})
                        s_groups = sp.get("groups") or {}
                        for gid in (p_s.get("groups") or {}).keys():
                            if gid not in s_groups:
                                need_fix = True
                                break
                        if need_fix:
                            break

        if need_fix:
            merged = merge_status_keep_runtime(snap, skeleton)
            merged["profiles_fp"] = fp
            save_status_any(merged)
            log.info(
                "get_status_snapshot fixed profiles=%d",
                len(merged.get("profiles", {})),
            )
            print(f"[STATUS] get fixed profiles={len(merged.get('profiles', {}))}")
            return merged

        log.info("get_status_snapshot ok (no fix)")
        return snap
    except Exception as e:
        log.exception("get_status_snapshot failed: %s", e)
        # Upstream (API-Layer) macht daraus HTTP 500.
        raise
