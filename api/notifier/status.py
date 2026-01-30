# api/notifier/status.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import STATUS_NOTIFIER
from storage import load_json_any, save_json_any
import json


from api.notifier.profiles import (
    list_profiles as profiles_list_profiles,
    profiles_fingerprint,
)

log = logging.getLogger("notifier.status")


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")


def _safe_strip(v: Any) -> str:
    """
    Robust: macht aus None/int/float/etc. einen sauberen String.
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    try:
        return str(v).strip()
    except Exception:
        return ""


def _fmt_indicator(ind: Any) -> str:
    """
    Formatiert ein Indicator-Objekt aus dem NEUEN Schema:
      {name, output, symbol|null, interval|null, params}
    Null bleibt Null-semantisch; hier wird nur gelabelt, NICHT aufgelöst.
    """
    if not isinstance(ind, dict):
        return "—"

    name = _safe_strip(ind.get("name")) or "—"
    out = _safe_strip(ind.get("output")) or ""
    sym = ind.get("symbol", None)  # kann None sein -> bewusst
    inv = ind.get("interval", None)

    parts: List[str] = [name]
    if out:
        parts.append(f":{out}")

    # Override sichtbar machen, aber null NICHT ersetzen
    if sym is not None:
        sym_s = _safe_strip(sym)
        if sym_s:
            parts.append(f" [{sym_s}]")

    if inv is not None:
        inv_s = _safe_strip(inv)
        if inv_s:
            parts.append(f" @{inv_s}")

    # value-indikator knapp anzeigen
    if name == "value":
        params = ind.get("params") if isinstance(ind.get("params"), dict) else {}
        if isinstance(params, dict) and "value" in params:
            parts.append(f"={params.get('value')}")

    return "".join(parts) if parts else "—"


def _label_only_conditions(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Erzeugt eine einfache, UI-taugliche Cond-Liste (ohne Werte) für den Status-Snapshot.
    NEW SCHEMA ONLY.
    """
    out: List[Dict[str, Any]] = []

    for c in (group.get("conditions") or []):
        if not isinstance(c, dict):
            continue

        left = _fmt_indicator(c.get("left"))
        right = _fmt_indicator(c.get("right"))
        op = (_safe_strip(c.get("op")) or "gt").lower()

        thr = c.get("threshold", None)
        thr_label = None
        if isinstance(thr, dict):
            ttype = _safe_strip(thr.get("type"))
            tparams = thr.get("params") if isinstance(thr.get("params"), dict) else {}
            if ttype:
                thr_label = {"type": ttype, "params": tparams}

        out.append(
            {
                "rid": _safe_strip(c.get("rid")) or None,
                "logic": (_safe_strip(c.get("logic")) or "and").lower(),
                "left": left,
                "right": right,
                "op": op,
                "threshold": thr_label,  # optional, rein declarativ
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
# Skeleton aus Profilen (NEW SCHEMA)
# ─────────────────────────────────────────────────────────────

def build_status_skeleton_from_profiles(profiles: list[dict], debug_print: bool = True) -> Dict[str, Any]:
    """
    Baut aus einer Profil-Liste ein "leeres" Status-Skeleton (NEW SCHEMA):
    - Struktur/IDs/Groups/Conditions sind drin
    - Runtime-Felder sind frisch (True/False/None/0)
    - KEINE Migration / KEIN Sanitizer / KEINE Symbol-Expansion
    """
    profiles_map: dict[str, dict] = {}

    if debug_print:
        try:
            print(f"[STATUS] skeleton: profiles_in={len(profiles or [])}")
        except Exception:
            pass

    for p in (profiles or []):
        if not isinstance(p, dict):
            continue

        pid = _safe_strip(p.get("id"))
        if not pid:
            # Profile ohne ID sind kaputt -> ignorieren (oder hart fail, wenn du willst)
            if debug_print:
                try:
                    print(f"[STATUS] skeleton: skip profile without id keys={list(p.keys())[:20]}")
                except Exception:
                    pass
            continue

        groups_in = p.get("groups") or []
        if not isinstance(groups_in, list):
            groups_in = []

        gmap: dict[str, dict] = {}

        for g in groups_in:
            if not isinstance(g, dict):
                continue

            gid = _safe_strip(g.get("gid"))
            if not gid:
                continue

            # NEW schema group fields (keep as-is)
            group_active = bool(g.get("active", True))

            g_entry = {
                "name": g.get("name") or gid,
                "group_active": group_active,
                "effective_active": group_active,
                "blockers": [],
                "auto_disabled": False,
                "cooldown_until": None,
                "fresh": True,
                "runtime": {
                    "threshold_state": {},  # evaluator füllt das später
                    "met": 0,
                    "total": len(g.get("conditions") or []),
                    "details": [],
                },
                "last_eval_ts": None,
                "last_bar_ts": None,

                # Declarative labels for UI
                "conditions": _label_only_conditions(g),
                "conditions_status": [],

                # Sichtbar für UI (NEU): symbol sources bleiben getrennt
                "symbol_group": g.get("symbol_group", None),
                "symbols": g.get("symbols", None),

                # Group settings
                "exchange": g.get("exchange", ""),
                "interval": g.get("interval", ""),
                "telegram_id": g.get("telegram_id", None),
                "single_mode": g.get("single_mode", None),
                "deactivate_on": g.get("deactivate_on", None),
            }

            if debug_print:
                try:
                    print(
                        f"[STATUS] skeleton: pid={pid} gid={gid} "
                        f"active={group_active} interval={g_entry.get('interval')!r} "
                        f"symbol_group={g_entry.get('symbol_group')!r} symbols={g_entry.get('symbols')!r} "
                        f"conds={len(g.get('conditions') or [])}"
                    )
                except Exception:
                    pass

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

    if debug_print:
        try:
            print(
                f"[STATUS] skeleton built profiles={len(profiles_map)} "
                f"groups={sum(len(v.get('groups', {})) for v in profiles_map.values())}"
            )
        except Exception:
            pass

    log.debug(
        "skeleton built profiles=%d groups=%d",
        len(profiles_map),
        sum(len(v.get("groups", {})) for v in profiles_map.values()),
    )

    return skeleton


# ─────────────────────────────────────────────────────────────
# Merge: Skeleton + alter Status (Runtime behalten)
# ─────────────────────────────────────────────────────────────

def merge_status_keep_runtime(old: Dict[str, Any], skel: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merged ein neues Skeleton in einen alten Status:
    - neue/gelöschte Profile/Groups werden berücksichtigt
    - Runtime-Daten (threshold_state, conditions_status, etc.) werden bestmöglich behalten
    """
    old_profiles = old.get("profiles") if isinstance(old.get("profiles"), dict) else {}
    skel_profiles = skel.get("profiles") if isinstance(skel.get("profiles"), dict) else {}

    try:
        version_int = int(old.get("version", 1))
    except Exception:
        version_int = 1

    new_out: Dict[str, Any] = {
        "version": version_int,
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

            rt_old = old_g.get("runtime") if isinstance(old_g.get("runtime"), dict) else {}
            cond_status_old = old_g.get("conditions_status", [])
            if not isinstance(cond_status_old, list):
                cond_status_old = []

            new_g = dict(g_s)
            # runtime behalten
            new_g["runtime"] = rt_old
            new_g["conditions_status"] = cond_status_old

            # timestamps behalten
            new_g["last_eval_ts"] = old_g.get("last_eval_ts", None)
            new_g["last_bar_ts"] = old_g.get("last_bar_ts", None)

            # blockers/cooldown/fresh behalten
            new_g["blockers"] = old_g.get("blockers", []) if isinstance(old_g.get("blockers"), list) else []
            new_g["auto_disabled"] = bool(old_g.get("auto_disabled", False))
            new_g["cooldown_until"] = old_g.get("cooldown_until", None)
            new_g["fresh"] = bool(old_g.get("fresh", True))

            new_p["groups"][gid] = new_g

        new_out["profiles"][pid] = new_p

    # Diagnostics
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
    try:
        print(f"[STATUS] merged pruned profiles={len(new_out['profiles'])}")
    except Exception:
        pass

    if pruned_pids:
        try:
            print(
                f"[STATUS] pruned profile IDs: {pruned_pids[:5]}"
                f"{'...' if len(pruned_pids) > 5 else ''}"
            )
        except Exception:
            pass
    if pruned_groups_total:
        for line in details_groups[:10]:
            try:
                print(f"[STATUS] pruned groups -> {line}")
            except Exception:
                pass
        if len(details_groups) > 10:
            try:
                print(
                    f"[STATUS] pruned groups (more): "
                    f"{len(details_groups) - 10} pid-lines omitted"
                )
            except Exception:
                pass

    return new_out


# ─────────────────────────────────────────────────────────────
# High-Level: Auto-Fix / Sync / Get
# ─────────────────────────────────────────────────────────────

def status_autofix_merge() -> None:
    """
    Lädt Profile → baut Skeleton → merged in aktuellen Status → speichert.
    Nutzt profiles_fingerprint, damit /status sehen kann, ob was veraltet ist.
    """
    profiles = profiles_list_profiles()
    skeleton = build_status_skeleton_from_profiles(profiles)
    current = load_status_any()
    merged = merge_status_keep_runtime(current, skeleton)
    merged["profiles_fp"] = profiles_fingerprint(profiles)
    save_status_any(merged)
    log.info(
        "Status auto-fix merge done. profiles_fp=%s",
        (merged.get("profiles_fp", "") or "")[:8],
    )
    try:
        print(f"[STATUS] autofix done fp={(merged.get('profiles_fp', '') or '')[:8]}")
    except Exception:
        pass


# Legacy-Name für alten Code
_status_autofix_merge = status_autofix_merge


def sync_status(profiles: Optional[list] = None) -> Dict[str, Any]:
    """
    Entspricht grob dem alten POST /status/sync:
    - optional Profile von außen (Body) → sonst local load
    - Skeleton + Merge + Save

    WICHTIG: profiles müssen NEW SCHEMA sein (profiles[].groups[]...)
    """
    if not isinstance(profiles, list):
        profiles = profiles_list_profiles()

    skeleton = build_status_skeleton_from_profiles(profiles)
    current = load_status_any()
    merged = merge_status_keep_runtime(current, skeleton)
    merged["profiles_fp"] = profiles_fingerprint(profiles)
    save_status_any(merged)

    log.info("status sync profiles=%d", len(merged.get("profiles", {})))
    try:
        print(f"[STATUS] sync profiles={len(merged.get('profiles', {}))}")
    except Exception:
        pass
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
        reason = "force_fix" if force_fix else ""

        profiles = profiles_list_profiles()
        skeleton = build_status_skeleton_from_profiles(profiles)
        fp = profiles_fingerprint(profiles)

        if not need_fix:
            if not snap.get("profiles"):
                need_fix = True
                reason = reason or "empty_status"
            else:
                if snap.get("profiles_fp", "") != fp:
                    need_fix = True
                    reason = reason or "fp_mismatch"
                else:
                    # prüfen, ob Gruppen fehlen
                    for pid, p_s in (skeleton.get("profiles") or {}).items():
                        sp = (snap.get("profiles") or {}).get(pid, {})
                        s_groups = sp.get("groups") or {}
                        for gid in (p_s.get("groups") or {}).keys():
                            if gid not in s_groups:
                                need_fix = True
                                reason = reason or "missing_group"
                                break
                        if need_fix:
                            break

        if need_fix:
            merged = merge_status_keep_runtime(snap, skeleton)
            merged["profiles_fp"] = fp
            save_status_any(merged)
            log.info(
                "get_status_snapshot fixed profiles=%d reason=%s",
                len(merged.get("profiles", {})),
                reason,
            )
            try:
                print(
                    f"[STATUS] get fixed profiles={len(merged.get('profiles', {}))} "
                    f"reason={reason}"
                )
            except Exception:
                pass
            return merged

        log.info("get_status_snapshot ok (no fix)")
        try:
            print("[STATUS] get ok (no fix needed)")
        except Exception:
            pass
        return snap
    except Exception as e:
        log.exception("get_status_snapshot failed: %s", e)
        raise

