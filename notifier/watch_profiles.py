# notifier/watch_profiles.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import time, json, hashlib
from pathlib import Path
from typing import Any, Dict, List, Tuple

import config as cfg
from .telegram_client import refresh_settings, debug_status, is_ready, send_message

SETTLE_SECONDS_DEFAULT = float((getattr(cfg, "WATCH_SETTLE_SEC", None) or 1.5))
POLL_SECONDS_DEFAULT   = float((getattr(cfg, "WATCH_INTERVAL",  None) or 1.0))
NOTIFY_ON_CHANGES      = bool((getattr(cfg, "WATCH_NOTIFY",     None) or True))

MeaningfulCondKeys = (
    "left","op","right","right_symbol","right_interval",
    "left_output","right_output","logic","left_params","right_params",
    "left_symbol","left_interval"
)

def _load_profiles(path: Path) -> List[Dict[str, Any]]:
    try:
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        if isinstance(data, list):
            return data
    except FileNotFoundError:
        print(f"[WATCH] Datei fehlt: {path}")
    except Exception as e:
        print(f"[WATCH] JSON-Fehler: {e}")
    return []

def _is_meaningful_condition(c: Dict[str, Any]) -> bool:
    left = (c.get("left") or "").strip()
    op   = (c.get("op") or "gt").strip()
    if not left: return False
    return op in {"eq","ne","gt","gte","lt","lte"}

def _normalize_profiles(raw: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    norm = []
    for p in raw or []:
        if not isinstance(p, dict): continue
        pid = str(p.get("id") or "")
        name = str(p.get("name") or "")
        enabled = bool(p.get("enabled", True))
        groups_in = p.get("condition_groups") or []
        groups_out = []
        for g in groups_in:
            if not isinstance(g, dict): continue
            gname = str(g.get("name") or "")
            interval = str(g.get("interval") or "")
            symbols = [str(s) for s in (g.get("symbols") or []) if s]
            symbols.sort()
            conds_in = g.get("conditions") or []
            conds_out = []
            for c in conds_in:
                if not isinstance(c, dict): continue
                if not _is_meaningful_condition(c): continue
                co = {k: c.get(k) for k in MeaningfulCondKeys if k in c}
                conds_out.append(co)
            groups_out.append({"name": gname, "interval": interval, "symbols": symbols, "conditions": conds_out})
        norm.append({"id": pid, "name": name, "enabled": enabled, "groups": groups_out})
    # stabile Ordnung
    norm.sort(key=lambda x: (x["name"], x["id"]))
    for p in norm:
        p["groups"].sort(key=lambda g: g["name"])
        for g in p["groups"]:
            g["conditions"].sort(key=lambda c: json.dumps(c, sort_keys=True, ensure_ascii=False))
    return norm

def _fingerprint(obj: Any) -> Tuple[str,int]:
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",",":"))
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]
    return h, len(s)

def _summarize(norm_profiles: List[Dict[str,Any]]) -> Dict[str, Dict[str,int]]:
    out = {}
    for p in norm_profiles:
        m = sum(len(g["conditions"]) for g in p["groups"])
        out[p["id"]] = {"groups": len(p["groups"]), "conds": m, "name": p["name"]}
    return out

def _build_change_message(prev: List[Dict[str,Any]], curr: List[Dict[str,Any]]) -> str:
    prev_idx = {p["id"]: p for p in prev}
    curr_idx = {p["id"]: p for p in curr}
    prev_sum = _summarize(prev)
    curr_sum = _summarize(curr)

    added = [curr_idx[i] for i in curr_idx.keys() - prev_idx.keys()]
    removed = [prev_idx[i] for i in prev_idx.keys() - curr_idx.keys()]
    changed = []
    for i in curr_idx.keys() & prev_idx.keys():
        if json.dumps(prev_idx[i], sort_keys=True) != json.dumps(curr_idx[i], sort_keys=True):
            changed.append((prev_idx[i], curr_idx[i]))

    lines = ["🛎 *Profile geändert*"]
    if added:
        for p in added:
            s = curr_sum.get(p["id"], {})
            lines.append(f"➕ _Neu_: *{p['name']}* · Gruppen {s.get('groups',0)}, Bedingungen {s.get('conds',0)}")
    if removed:
        for p in removed:
            s = prev_sum.get(p["id"], {})
            lines.append(f"➖ _Gelöscht_: *{p['name']}* · Gruppen {s.get('groups',0)}, Bedingungen {s.get('conds',0)}")
    for (p_old, p_new) in changed:
        so = prev_sum.get(p_old["id"], {})
        sn = curr_sum.get(p_new["id"], {})
        if so != sn:
            lines.append(f"✏️ *{p_new['name']}* · Gruppen {so.get('groups',0)}→{sn.get('groups',0)}, Bedingungen {so.get('conds',0)}→{sn.get('conds',0)}")
        else:
            lines.append(f"✏️ *{p_new['name']}* aktualisiert")

    if len(lines) == 1:
        lines.append("_(keine signifikanten Änderungen)_")
    return "\n".join(lines)

def run(interval_sec: float = POLL_SECONDS_DEFAULT,
        path_override: str | None = None,
        settle_seconds: float = SETTLE_SECONDS_DEFAULT) -> None:
    refresh_settings(); debug_status()
    watch_path = Path(path_override or cfg.PROFILES_NOTIFIER)
    print(f"[WATCH] JSON-Scan: {watch_path} (every {interval_sec}s) – API-frei")
    prev_norm: List[Dict[str,Any]] | None = None
    prev_fp: str | None = None
    first = True

    while True:
        try:
            if not watch_path.exists():
                time.sleep(max(0.2, interval_sec)); continue

            stat = watch_path.stat()
            mtime = stat.st_mtime
            now = time.time()

            # Warte, bis Datei seit settle_seconds stabil
            if (now - mtime) < float(settle_seconds):
                time.sleep(max(0.2, interval_sec)); continue

            raw = _load_profiles(watch_path)
            norm = _normalize_profiles(raw)
            fp, size = _fingerprint(norm)

            if first:
                print(f"[WATCH] initial hash={fp} size={size}")
                prev_norm, prev_fp, first = norm, fp, False
            else:
                if fp != prev_fp:
                    print(f"[WATCH] change detected: {prev_fp} -> {fp}")
                    if NOTIFY_ON_CHANGES and is_ready():
                        send_message(_build_change_message(prev_norm or [], norm))
                    prev_norm, prev_fp = norm, fp
        except Exception as e:
            print(f"[WATCH] Loop-Fehler: {e}")
        time.sleep(max(0.2, float(interval_sec)))
