# api/notifier_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional, Union
from pathlib import Path
from copy import deepcopy

import json
import uuid
import os
import time
import tempfile
import hashlib
import random
import string
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# Konfig: EIN Unified-File als Source of Truth
# ─────────────────────────────────────────────────────────────
try:
    from config import NOTIFIER_UNIFIED as _NOTIFIER_UNIFIED_CFG
except Exception:
    _NOTIFIER_UNIFIED_CFG = None

# Optional: Registry beibehalten (UI-Kompatibilität)
try:
    from notifier.indicator_registry import REGISTERED, SIMPLE_SIGNALS
except Exception:
    REGISTERED, SIMPLE_SIGNALS = {}, []

router = APIRouter(prefix="/notifier")


# ─────────────────────────────────────────────────────────────
# Pfad-Utils / Locking / Zeit / Hash
# ─────────────────────────────────────────────────────────────
def _to_path(p: Any) -> Path:
    if isinstance(p, Path):
        return p
    return Path(str(p)).expanduser().resolve()

def _ensure_parent_dir(p: Path) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"💥 mkdir failed for {p.parent}: {e}")

def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def _sha256_str(s: str) -> str:
    return _sha256_bytes(s.encode("utf-8"))

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _rand_id(n: int = 6) -> str:
    return "".join(random.choices("0123456789abcdef", k=n))

# Lock-Verzeichnis
_ENV_LOCK_DIR = os.environ.get("NOTIFIER_LOCK_DIR", "").strip()
_LOCK_DIR = _to_path(_ENV_LOCK_DIR) if _ENV_LOCK_DIR else _to_path(tempfile.gettempdir()) / "notifier_locks"
_LOCK_DIR.mkdir(parents=True, exist_ok=True)
print(f"[DEBUG] Using lock dir: {_LOCK_DIR}")

def _lock_path(path: Path) -> Path:
    return _LOCK_DIR / (Path(path).name + ".lock")

class FileLock:
    def __init__(self, path: Path, timeout: float = 10.0, poll: float = 0.1):
        self.lockfile = _lock_path(path); self.timeout = timeout; self.poll = poll; self._acquired = False
    def acquire(self):
        start = time.time()
        while True:
            try:
                fd = os.open(str(self.lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd); self._acquired = True
                print(f"[DEBUG] Acquired lock {self.lockfile}"); return
            except FileExistsError:
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lockfile}")
                time.sleep(self.poll)
    def release(self):
        if self._acquired:
            try: os.unlink(self.lockfile); print(f"[DEBUG] Released lock {self.lockfile}")
            except FileNotFoundError: pass
            finally: self._acquired = False
    def __enter__(self): self.acquire(); return self
    def __exit__(self, exc_type, exc, tb): self.release()

# ─────────────────────────────────────────────────────────────
# Unified-Datei bestimmen
# ─────────────────────────────────────────────────────────────
def _default_unified_path() -> Path:
    # lokale Default-Struktur unter ./data/profiles/notifier/notifier.json
    here = Path.cwd()
    p = here / "data" / "profiles" / "notifier" / "notifier.json"
    return _to_path(p)

NOTIFIER_UNIFIED: Path = _to_path(_NOTIFIER_UNIFIED_CFG) if _NOTIFIER_UNIFIED_CFG else _default_unified_path()
_ensure_parent_dir(NOTIFIER_UNIFIED)
print(f"[DEBUG] Unified path: {NOTIFIER_UNIFIED}")

# ─────────────────────────────────────────────────────────────
# Pydantic v1/v2 kompatibles Basismodell (extra='allow')
# ─────────────────────────────────────────────────────────────
try:
    from pydantic import ConfigDict
    _IS_PYDANTIC_V2 = True
except Exception:
    ConfigDict = None
    _IS_PYDANTIC_V2 = False

class ApiModel(BaseModel):
    if _IS_PYDANTIC_V2:
        model_config = ConfigDict(extra="allow")
    else:
        class Config:
            extra = "allow"

def model_to_dict(m: BaseModel | dict | list | Any) -> dict | list | Any:
    if isinstance(m, BaseModel):
        if hasattr(m, "model_dump"):
            return m.model_dump(exclude_unset=False, exclude_none=False)
        return m.dict(exclude_unset=False)
    if isinstance(m, list):
        return [model_to_dict(x) for x in m]
    if isinstance(m, dict):
        return {k: model_to_dict(v) for k, v in m.items()}
    return m

# ─────────────────────────────────────────────────────────────
# Unified-Struktur + IO
# ─────────────────────────────────────────────────────────────
_UNIFIED_TEMPLATE: Dict[str, Any] = {
    "version": 1,
    "flavor": "notifier-api",
    "updated_ts": None,
    "profiles": [],             # Liste
    "profiles_fp": "",          # Fingerprint über Config
    "storage": {"layout": "unified-config-runtime", "notes": "hash is config-only; runtime is volatile"},
    "alarms": [],               # Liste
    "overrides": {"overrides": {}, "updated_ts": None},  # overrides.overrides[pid][gid] = {forced_off, snooze_until, note}
    "commands": {"queue": []},  # Liste
}

def _load_unified() -> Dict[str, Any]:
    if not NOTIFIER_UNIFIED.exists():
        print(f"[DEBUG] load_unified: not found -> template")
        data = deepcopy(_UNIFIED_TEMPLATE)
        data["updated_ts"] = _now_iso()
        _save_unified(data)
        return data
    try:
        txt = NOTIFIER_UNIFIED.read_text(encoding="utf-8")
        data = json.loads(txt)
        if not isinstance(data, dict):
            print("[DEBUG] load_unified: non-dict -> reset to template")
            data = deepcopy(_UNIFIED_TEMPLATE)
        # ensure keys
        data.setdefault("version", 1)
        data.setdefault("flavor", "notifier-api")
        data.setdefault("updated_ts", _now_iso())
        data.setdefault("profiles", [])
        data.setdefault("profiles_fp", "")
        data.setdefault("storage", {"layout": "unified-config-runtime", "notes": "hash is config-only; runtime is volatile"})
        data.setdefault("alarms", [])
        data.setdefault("overrides", {"overrides": {}, "updated_ts": None})
        data.setdefault("commands", {"queue": []})
        return data
    except Exception as e:
        print(f"💥 load_unified error: {e}")
        data = deepcopy(_UNIFIED_TEMPLATE); data["updated_ts"] = _now_iso()
        _save_unified(data)
        return data

def _save_unified(data: Dict[str, Any]) -> None:
    data = deepcopy(data)
    data["updated_ts"] = _now_iso()
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    tmp = NOTIFIER_UNIFIED.with_suffix(".json.tmp")
    try:
        with FileLock(NOTIFIER_UNIFIED):
            with open(tmp, "wb") as f:
                f.write(payload); f.flush(); os.fsync(f.fileno())
            os.replace(tmp, NOTIFIER_UNIFIED)
            try:
                if hasattr(os, "O_DIRECTORY"):
                    dfd = os.open(str(NOTIFIER_UNIFIED.parent), os.O_DIRECTORY)
                    try: os.fsync(dfd)
                    finally: os.close(dfd)
            except Exception as e:
                print(f"[DEBUG] dir fsync skipped: {e}")
        print(f"[DEBUG] save_unified -> {NOTIFIER_UNIFIED}")
    except Exception as e:
        print(f"💥 save_unified error: {e}")
        try:
            if tmp.exists(): tmp.unlink()
        except Exception: pass
        raise

# ─────────────────────────────────────────────────────────────
# Sanitizer / Normalisierer
# ─────────────────────────────────────────────────────────────
_ALLOWED_OPS = {"eq","ne","gt","gte","lt","lte"}
_ALLOWED_LOGIC = {"and","or"}
_ALLOWED_DEACT = {"always","true","any_true"}

def _trim_str(x: Any, dash_to_empty: bool = True) -> str:
    if x is None: return ""
    s = str(x).strip()
    return "" if (dash_to_empty and s == "—") else s

def _name_key(x: Any) -> str:
    return _trim_str(x).lower()

def _normalize_deactivate_value(v: Any) -> Optional[str]:
    if v is None: return None
    if isinstance(v, bool): return "true" if v else None
    s = _trim_str(v).lower()
    if not s: return None
    if s == "always": return "always"
    if s in {"true","full","match"}: return "true"
    if s in {"any_true","any","partial"}: return "any_true"
    return None

def _normalize_slope_params_dict(p: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(p, dict): return {}
    out = dict(p)
    bp = {k[3:]: v for k, v in p.items() if isinstance(k, str) and k.startswith("bp.") and v not in (None, "")}
    if bp:
        nested = dict(p.get("base_params") or {})
        nested.update(bp)
        out["base_params"] = nested
    return out

def _sanitize_condition(c: dict) -> dict:
    c = dict(c or {})
    c.setdefault("rid", _rand_id())
    c.setdefault("left", ""); c.setdefault("op", "gt"); c.setdefault("right", "")
    c.setdefault("left_output",""); c.setdefault("right_output","")
    c.setdefault("left_symbol",""); c.setdefault("left_interval","")
    c.setdefault("right_symbol",""); c.setdefault("right_interval","")
    c.setdefault("logic","and")
    c.setdefault("left_params", {}); c.setdefault("right_params", {})

    # trim / enforce types
    for k in ("left","right_symbol","right_interval","left_output","right_output","logic","op","left_symbol","left_interval"):
        c[k] = _trim_str(c.get(k))
    if c["op"] not in _ALLOWED_OPS: c["op"] = "gt"
    if c["logic"] not in _ALLOWED_LOGIC: c["logic"] = "and"
    rv = c.get("right")
    if isinstance(rv, (int,float)): c["right"] = str(rv)
    elif rv is None: c["right"] = ""
    else: c["right"] = _trim_str(rv)

    if not isinstance(c["left_params"], dict): c["left_params"] = {}
    if not isinstance(c["right_params"], dict): c["right_params"] = {}

    if _trim_str(c.get("left"), False).lower() == "slope":
        c["left_params"] = _normalize_slope_params_dict(c["left_params"])
    if _trim_str(c.get("right"), False).lower() == "slope":
        c["right_params"] = _normalize_slope_params_dict(c["right_params"])

    rid = _trim_str(c.get("rid")) or _rand_id()
    c["rid"] = rid
    return c

def _sanitize_group(g: dict) -> dict:
    g = dict(g or {})
    g.setdefault("gid", _rand_id())
    g.setdefault("conditions", [])
    g.setdefault("active", True)
    g.setdefault("symbols", [])
    g.setdefault("interval","")
    g.setdefault("exchange","")
    g.setdefault("name","")
    g.setdefault("telegram_bot_id","")
    g.setdefault("description","")
    # deactivate + min_true_ticks
    if "deactivate_on" not in g: g["deactivate_on"] = None
    if "auto_deactivate" in g:  # legacy
        if g.get("deactivate_on") is None:
            g["deactivate_on"] = _normalize_deactivate_value(g.get("auto_deactivate"))
        g.pop("auto_deactivate", None)
    # min_true_ticks
    mtt = g.get("min_true_ticks", None)
    try:
        if mtt not in (None, "", "null"):
            mtt = int(mtt)
            if mtt < 1: mtt = None
    except Exception:
        mtt = None
    g["min_true_ticks"] = mtt

    # strings trim
    for k in ("interval","exchange","name","telegram_bot_id","description"):
        g[k] = _trim_str(g.get(k))

    # symbols
    if not isinstance(g["symbols"], list):
        g["symbols"] = []
    else:
        g["symbols"] = [s for s in (g["symbols"] or []) if isinstance(s, str) and _trim_str(s)]

    # conds
    conds = []
    for raw in g.get("conditions") or []:
        if isinstance(raw, dict):
            conds.append(_sanitize_condition(raw))
    # unique rid
    seen = set()
    for c in conds:
        if c["rid"] in seen: c["rid"] = _rand_id()
        seen.add(c["rid"])
    g["conditions"] = conds

    gid = _trim_str(g.get("gid")) or _rand_id()
    g["gid"] = gid

    # normalize deactivate_on
    before = g.get("deactivate_on")
    norm = _normalize_deactivate_value(before)
    g["deactivate_on"] = norm if norm in _ALLOWED_DEACT else None
    return g

def _sanitize_profiles_list(data: list) -> list:
    out = []
    for p in data or []:
        if not isinstance(p, dict): continue
        p = dict(p)
        p.setdefault("name", "Unnamed")
        p.setdefault("enabled", True)
        p.setdefault("groups", [])        # unified name
        p.setdefault("created_ts", _now_iso())
        p.setdefault("updated_ts", _now_iso())
        p["id"] = str(p.get("id") or uuid.uuid4())
        # groups sanitize
        groups = []
        for g in p.get("groups") or p.get("condition_groups") or []:
            if isinstance(g, dict): groups.append(_sanitize_group(g))
        # unique gid
        seen = set()
        for g in groups:
            if g["gid"] in seen: g["gid"] = _rand_id()
            seen.add(g["gid"])
        p["groups"] = groups
        out.append(p)
    return out

def _profiles_fingerprint(profiles_cfg_only: list[dict]) -> str:
    try:
        # config-only: nur config unter group.config? -> hier: unser p hat 'groups' deren payload config ist
        norm = deepcopy(profiles_cfg_only)
        # entferne runtime keys, falls vorhanden
        for p in norm:
            p.pop("runtime", None)
            for g in (p.get("groups") or []):
                g.pop("runtime", None)
                # conditions runtime liegt bei uns in group["config"]["conditions"][i]["rt"] NICHT – wir halten runtime getrennt
        dumped = json.dumps(norm, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return _sha256_str(dumped)
    except Exception as e:
        print(f"[DEBUG] fingerprint error: {e}")
        return ""

# ─────────────────────────────────────────────────────────────
# Unified: Profile CRUD (UI-kompatible Endpoints)
# ─────────────────────────────────────────────────────────────
class ConditionOut(ApiModel):
    rid: str
    left: str
    op: Literal["eq","ne","gt","gte","lt","lte"]
    right: str = ""
    left_output: str = ""
    right_output: str = ""
    logic: Literal["and","or"] = "and"
    left_params: Dict[str, Any] = Field(default_factory=dict)
    right_params: Dict[str, Any] = Field(default_factory=dict)
    left_symbol: str = ""
    left_interval: str = ""
    right_symbol: str = ""
    right_interval: str = ""

class GroupOut(ApiModel):
    gid: str
    hash: Optional[str] = None
    config: Dict[str, Any]
    runtime: Dict[str, Any]

class ProfileRead(ApiModel):
    id: str
    name: str
    enabled: bool = True
    created_ts: str
    updated_ts: str
    groups: List[GroupOut]

# WRITE (tolerant)
class ConditionIn(ApiModel):
    rid: Optional[str] = None
    left: Optional[str] = ""
    op: Optional[Literal["eq","ne","gt","gte","lt","lte"]] = "gt"
    right: Union[str, float, int, None] = ""
    left_output: Optional[str] = ""
    right_output: Optional[str] = ""
    logic: Optional[Literal["and","or"]] = "and"
    left_params: Dict[str, Any] = Field(default_factory=dict)
    right_params: Dict[str, Any] = Field(default_factory=dict)
    left_symbol: Optional[str] = ""
    left_interval: Optional[str] = ""
    right_symbol: Optional[str] = ""
    right_interval: Optional[str] = ""

class GroupConfigIn(ApiModel):
    name: str = ""
    active: bool = True
    symbols: List[str] = Field(default_factory=list)
    interval: str = ""
    exchange: str = ""
    telegram_bot_id: str = ""
    description: str = ""
    deactivate_on: Optional[Literal["always","true","any_true"]] = None
    min_true_ticks: Optional[int] = None
    conditions: List[ConditionIn] = Field(default_factory=list)

class GroupIn(ApiModel):
    gid: Optional[str] = None
    config: GroupConfigIn
    runtime: Optional[Dict[str, Any]] = None  # wird ignoriert/überschrieben
    hash: Optional[str] = None

class ProfileCreate(ApiModel):
    id: Optional[str] = None
    name: str
    enabled: bool = True
    groups: List[GroupIn] = Field(default_factory=list)

class ProfileUpdate(ProfileCreate):
    pass

def _hash_group_config(cfg_only: Dict[str, Any]) -> str:
    dumped = json.dumps(cfg_only, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return "sha256-" + _sha256_str(dumped)

def _default_group_runtime_from_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "created_ts": _now_iso(),
        "updated_ts": _now_iso(),
        "effective_active": bool(cfg.get("active", True)),
        "blockers": [] if cfg.get("active", True) else ["group_inactive"],
        "auto_disabled": False,
        "cooldown_until": None,
        "last_eval_ts": None,
        "last_bar_ts": None,
        "aggregate": {"logic": "AND", "passed": False},
        "counters": {"met": 0, "total": len(cfg.get("conditions") or []), "true_ticks": 0},
        "dedupe": {"last_alarm_tick_id": None, "last_alarm_ts": None},
    }

def _sanitize_incoming_profile(p: dict, preserve_ids_from: Optional[dict]=None) -> dict:
    p = dict(p or {})
    p.setdefault("name", "Unnamed")
    p.setdefault("enabled", True)
    p["id"] = str(p.get("id") or uuid.uuid4())
    p.setdefault("groups", [])

    out_groups = []
    for idx, g in enumerate(p["groups"]):
        if not isinstance(g, dict): continue
        gid_in = _trim_str(g.get("gid")) or None

        cfg_in = model_to_dict(g.get("config") or {})
        cfg_norm = _sanitize_group({
            "gid": gid_in or _rand_id(),
            "name": cfg_in.get("name",""),
            "active": cfg_in.get("active", True),
            "symbols": cfg_in.get("symbols") or [],
            "interval": cfg_in.get("interval",""),
            "exchange": cfg_in.get("exchange",""),
            "telegram_bot_id": cfg_in.get("telegram_bot_id",""),
            "description": cfg_in.get("description",""),
            "deactivate_on": cfg_in.get("deactivate_on"),
            "min_true_ticks": cfg_in.get("min_true_ticks"),
            "conditions": [model_to_dict(c) for c in (cfg_in.get("conditions") or [])],
        })
        # Hash nur über cfg_norm
        gh = _hash_group_config({
            k: cfg_norm.get(k) for k in [
                "name","active","symbols","interval","exchange","telegram_bot_id","description",
                "deactivate_on","min_true_ticks","conditions"
            ]
        })
        # runtime
        rt = _default_group_runtime_from_cfg(cfg_norm)

        # ID-Stabilisierung gegen altes Profil (positionsbasiert)
        if preserve_ids_from:
            old_groups = preserve_ids_from.get("groups") or []
            if idx < len(old_groups):
                og = old_groups[idx]
                if _trim_str(og.get("gid")):
                    cfg_norm["gid"] = og["gid"]
                # rids positionsbasiert
                old_conds = (og.get("config") or {}).get("conditions") or []
                for ci, nc in enumerate(cfg_norm["conditions"]):
                    try:
                        oc = old_conds[ci]
                        if _trim_str(oc.get("rid")):
                            nc["rid"] = oc["rid"]
                    except Exception:
                        pass

        out_groups.append({
            "gid": cfg_norm["gid"],
            "hash": gh,
            "config": cfg_norm,
            "runtime": rt,
        })

    # unique gid
    seen = set()
    for g in out_groups:
        if g["gid"] in seen:
            g["gid"] = _rand_id()
        seen.add(g["gid"])

    p["groups"] = out_groups
    # timestamps
    p.setdefault("created_ts", _now_iso())
    p["updated_ts"] = _now_iso()
    return p

def _recompute_profiles_fp(unified: Dict[str, Any]) -> None:
    cfg_only = []
    for p in (unified.get("profiles") or []):
        cfg_only.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "enabled": p.get("enabled", True),
            "groups": [ {"gid": g.get("gid"), "config": g.get("config")} for g in (p.get("groups") or []) ],
        })
    unified["profiles_fp"] = _profiles_fingerprint(cfg_only)

# ─────────────────────────────────────────────────────────────
# Endpunkte: PROFILES
# ─────────────────────────────────────────────────────────────
@router.get("/profiles", response_model=List[ProfileRead])
def get_profiles():
    u = _load_unified()
    print(f"[DEBUG] GET /profiles -> {len(u.get('profiles', []))} items")
    return u.get("profiles", [])

@router.get("/profiles/{pid}", response_model=ProfileRead)
def get_profile(pid: str):
    u = _load_unified()
    for p in (u.get("profiles") or []):
        if str(p.get("id")) == str(pid):
            print(f"[DEBUG] GET /profiles/{pid} -> found")
            return p
    print(f"[DEBUG] GET /profiles/{pid} -> 404")
    raise HTTPException(status_code=404, detail="Profil nicht gefunden")

# Upsert by Name (Case-insensitive): gleicher Name überschreibt
@router.post("/profiles", response_model=dict)
def add_profile(p: ProfileCreate):
    u = _load_unified()
    incoming = model_to_dict(p)
    name_key = _name_key(incoming.get("name"))
    idx = next((i for i, it in enumerate(u["profiles"]) if _name_key(it.get("name")) == name_key), None)

    if idx is not None:
        # preserve IDs/rids positionsbasiert
        before = deepcopy(u["profiles"][idx])
        merged = _sanitize_incoming_profile(incoming, preserve_ids_from=before)
        merged["id"] = before.get("id") or merged["id"]
        if json.dumps(before, sort_keys=True, ensure_ascii=False) == json.dumps(merged, sort_keys=True, ensure_ascii=False):
            print(f"[DEBUG] POST /profiles upsert -> NO CHANGE name='{incoming.get('name')}' id={merged['id']}")
            return {"status": "ok", "id": merged["id"], "updated": False, "upserted_by_name": True}
        u["profiles"][idx] = merged
        _recompute_profiles_fp(u)
        _save_unified(u)
        print(f"[DEBUG] POST /profiles upsert -> updated id={merged['id']}")
        return {"status": "updated", "id": merged["id"], "updated": True, "upserted_by_name": True}

    # neu
    created = _sanitize_incoming_profile(incoming)
    u["profiles"].append(created)
    _recompute_profiles_fp(u)
    _save_unified(u)
    print(f"[DEBUG] POST /profiles -> created id={created['id']}")
    return {"status": "ok", "id": created["id"], "created": True, "upserted_by_name": False}

@router.put("/profiles/{pid}", response_model=dict)
def update_profile(pid: str, p: ProfileUpdate):
    u = _load_unified()
    incoming = model_to_dict(p)
    idx = next((i for i, it in enumerate(u["profiles"]) if str(it.get("id")) == str(pid)), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Profil nicht gefunden")

    before = deepcopy(u["profiles"][idx])
    merged = _sanitize_incoming_profile({**incoming, "id": pid}, preserve_ids_from=before)

    if json.dumps(before, sort_keys=True, ensure_ascii=False) == json.dumps(merged, sort_keys=True, ensure_ascii=False):
        print(f"[DEBUG] PUT /profiles/{pid} -> NO CHANGE")
        return {"status": "ok", "id": pid}

    u["profiles"][idx] = merged
    _recompute_profiles_fp(u)
    _save_unified(u)
    print(f"[DEBUG] PUT /profiles/{pid} -> updated")
    return {"status": "updated", "id": pid}

class GroupActivePatch(ApiModel):
    active: bool

@router.patch("/profiles/{pid}/groups/{gid}/active", response_model=dict)
def set_group_active(pid: str, gid: str, body: GroupActivePatch = Body(...)):
    u = _load_unified()
    changed = False
    for p in (u.get("profiles") or []):
        if str(p.get("id")) != str(pid): continue
        for g in (p.get("groups") or []):
            if str(g.get("gid")) == str(gid):
                cfg = g.get("config") or {}
                old = bool(cfg.get("active", True))
                cfg["active"] = bool(body.active)
                g["config"] = _sanitize_group(cfg)  # re-normalize
                # runtime immediate reflect
                rt = g.get("runtime") or {}
                rt["effective_active"] = bool(cfg["active"])
                if not cfg["active"]:
                    blockers = list(rt.get("blockers") or [])
                    if "group_inactive" not in blockers:
                        blockers.append("group_inactive")
                    rt["blockers"] = blockers
                else:
                    rt["blockers"] = [b for b in (rt.get("blockers") or []) if b != "group_inactive"]
                rt["updated_ts"] = _now_iso()
                g["runtime"] = rt
                changed = (old != cfg["active"])
                break
    if changed:
        _recompute_profiles_fp(u)
        _save_unified(u)
        print(f"[DEBUG] PATCH active -> pid={pid} gid={gid} active={bool(body.active)} (saved)")
    else:
        print(f"[DEBUG] PATCH active -> pid={pid} gid={gid} no change")
    return {"status": "ok", "profile_id": pid, "group_id": gid, "active": bool(body.active)}

@router.delete("/profiles/{pid}", response_model=dict)
def delete_profile(pid: str):
    u = _load_unified()
    before = len(u.get("profiles") or [])
    u["profiles"] = [p for p in (u.get("profiles") or []) if str(p.get("id")) != str(pid)]
    after = len(u["profiles"])
    removed = before - after
    if removed:
        _recompute_profiles_fp(u)
        _save_unified(u)
    print(f"[DEBUG] DELETE /profiles/{pid} -> removed={removed}")
    return {"status": "deleted", "id": pid}

# ─────────────────────────────────────────────────────────────
# STATUS (Snapshot aus Unified, Runtime erhalten)
# ─────────────────────────────────────────────────────────────
def _build_status_from_unified(u: Dict[str, Any]) -> Dict[str, Any]:
    # Für UI-Kompat: map auf {version, flavor, updated_ts, profiles:{pid:{...}}}
    out_profiles: Dict[str, Any] = {}
    for p in (u.get("profiles") or []):
        pid = str(p.get("id"))
        entry = {"id": pid, "name": p.get("name"), "profile_active": bool(p.get("enabled", True)), "groups": {}}
        for g in (p.get("groups") or []):
            gid = str(g.get("gid"))
            cfg = g.get("config") or {}
            rt = deepcopy(g.get("runtime") or {})
            entry["groups"][gid] = {
                "name": cfg.get("name") or gid,
                "group_active": bool(cfg.get("active", True)),
                "effective_active": bool(rt.get("effective_active", cfg.get("active", True))),
                "blockers": list(rt.get("blockers") or []),
                "auto_disabled": bool(rt.get("auto_disabled", False)),
                "cooldown_until": rt.get("cooldown_until"),
                "fresh": rt.get("fresh", True),
                "aggregate": {"min_true_ticks": cfg.get("min_true_ticks")},
                "runtime": {
                    "true_ticks": (rt.get("counters") or {}).get("true_ticks", 0),
                    "last_eval_ts": rt.get("last_eval_ts"),
                },
                "last_eval_ts": rt.get("last_eval_ts"),
                "conditions": cfg.get("conditions") or [],
                "conditions_status": [],  # Evaluator kann füllen
            }
        out_profiles[pid] = entry
    return {
        "version": int(u.get("version", 1)),
        "flavor": "notifier-api",
        "updated_ts": _now_iso(),
        "profiles": out_profiles,
        "profiles_fp": u.get("profiles_fp", ""),
    }

@router.get("/status")
def get_status():
    u = _load_unified()
    snap = _build_status_from_unified(u)
    print(f"[DEBUG] GET /status -> profiles={len(snap.get('profiles', {}))}")
    return snap

@router.post("/status/sync", response_model=dict)
def status_sync(body: Dict[str, Any] = Body(default=None)):
    """
    UI kann Profile (Config) senden; wir mappen sie in Unified-Struktur.
    """
    try:
        incoming = body or {}
        if isinstance(incoming.get("profiles"), list):
            # Build a lightweight unified from incoming and merge
            tmp = deepcopy(_load_unified())
            # Ersetze nur die profiles (config), runtime bleibt wie in tmp
            new_profiles = _sanitize_profiles_list(incoming["profiles"])
            # Map in Unified-Objektstruktur (mit runtime default)
            mapped = []
            for p in new_profiles:
                mapped_groups = []
                for g in (p.get("groups") or []):
                    mapped_groups.append({
                        "gid": g["gid"],
                        "hash": _hash_group_config({k:g.get(k) for k in ["name","active","symbols","interval","exchange","telegram_bot_id","description","deactivate_on","min_true_ticks","conditions"]}),
                        "config": g,
                        "runtime": _default_group_runtime_from_cfg(g),
                    })
                mapped.append({
                    "id": p["id"], "name": p["name"], "enabled": p.get("enabled", True),
                    "created_ts": p.get("created_ts", _now_iso()),
                    "updated_ts": _now_iso(),
                    "groups": mapped_groups,
                })
            tmp["profiles"] = mapped
            _recompute_profiles_fp(tmp)
            _save_unified(tmp)
            print(f"[DEBUG] /status/sync -> replaced profiles from UI payload (count={len(mapped)})")
            return {"status": "ok", "profiles": len(mapped)}
        else:
            # nur Timestamp bump
            u = _load_unified()
            _save_unified(u)
            print(f"[DEBUG] /status/sync -> touched unified (no profiles in body)")
            return {"status": "ok", "profiles": len(u.get("profiles") or [])}
    except Exception as e:
        print(f"💥 /status/sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────
# ALARMS (im Unified: u['alarms'])
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
    deactivate_applied: Literal["", "true", "any_true"] = ""
    meta: Dict[str, Any] = Field(default_factory=dict)

class AlarmOut(AlarmBase):
    id: str

class AlarmIn(AlarmBase):
    id: Optional[str] = None

@router.get("/alarms", response_model=List[AlarmOut])
def list_alarms():
    u = _load_unified()
    items = u.get("alarms") or []
    # härten
    out = []
    for a in items:
        if not isinstance(a, dict): continue
        a = dict(a)
        a.setdefault("id", _rand_id())
        for k, typ, default in [
            ("ts", str, ""), ("profile_id", str, ""), ("group_id", str, ""), ("symbol", str, ""),
            ("interval", str, ""), ("reason", str, ""), ("reason_code", str, ""),
        ]:
            v = a.get(k)
            a[k] = v if isinstance(v, typ) else default
        if isinstance(a.get("matched"), str):
            try: a["matched"] = json.loads(a["matched"])
            except Exception: a["matched"] = []
        if not isinstance(a.get("matched"), list): a["matched"] = []
        if isinstance(a.get("meta"), str):
            try: a["meta"] = json.loads(a["meta"])
            except Exception: a["meta"] = {}
        if not isinstance(a.get("meta"), dict): a["meta"] = {}
        da = a.get("deactivate_applied","")
        if da not in {"","true","any_true"}: a["deactivate_applied"] = ""
        out.append(a)
    print(f"[DEBUG] GET /alarms -> {len(out)} items")
    return out

@router.post("/alarms", response_model=dict)
def add_alarm(a: AlarmIn):
    u = _load_unified()
    payload = model_to_dict(a)
    # normalize lists/dicts
    m = payload.get("matched", [])
    if isinstance(m, str):
        try: payload["matched"] = json.loads(m)
        except Exception: payload["matched"] = []
    elif not isinstance(m, list):
        payload["matched"] = []
    meta = payload.get("meta", {})
    if isinstance(meta, str):
        try: payload["meta"] = json.loads(meta)
        except Exception: payload["meta"] = {}
    elif not isinstance(meta, dict):
        payload["meta"] = {}
    aid = payload.get("id") or _rand_id()
    payload["id"] = aid
    # defensive normalization
    da = payload.get("deactivate_applied","")
    if da not in {"","true","any_true"}: payload["deactivate_applied"] = ""
    u.setdefault("alarms", [])
    u["alarms"].append(payload)
    _save_unified(u)
    print(f"[DEBUG] POST /alarms -> saved id={aid} total={len(u['alarms'])}")
    return {"status": "ok", "id": aid}

@router.delete("/alarms/{aid}", response_model=dict)
def delete_alarm(aid: str):
    u = _load_unified()
    before = len(u.get("alarms") or [])
    u["alarms"] = [x for x in (u.get("alarms") or []) if str(x.get("id")) != str(aid)]
    after = len(u["alarms"])
    _save_unified(u)
    print(f"[DEBUG] DELETE /alarms/{aid} -> removed={before-after}")
    return {"status": "deleted", "id": aid}

# ─────────────────────────────────────────────────────────────
# Overrides & Commands (im Unified)
# ─────────────────────────────────────────────────────────────
@router.get("/overrides", response_model=Dict[str, Any])
def get_overrides():
    u = _load_unified()
    o = u.get("overrides") or {"overrides": {}, "updated_ts": None}
    print(f"[DEBUG] GET /overrides -> profiles={len((o.get('overrides') or {}))}")
    return o

class OverridePatch(ApiModel):
    forced_off: Optional[bool] = None
    snooze_until: Optional[Union[str, None]] = None
    note: Optional[str] = None

def _ensure_ovr_slot(u: Dict[str, Any], pid: str, gid: str) -> Dict[str, Any]:
    u.setdefault("overrides", {"overrides": {}, "updated_ts": None})
    u["overrides"].setdefault("overrides", {})
    u["overrides"]["overrides"].setdefault(pid, {})
    u["overrides"]["overrides"][pid].setdefault(gid, {"forced_off": False, "snooze_until": None, "note": None})
    return u["overrides"]["overrides"][pid][gid]

@router.patch("/overrides/{profile_id}/{group_id}", response_model=dict)
def patch_override(profile_id: str, group_id: str, body: OverridePatch):
    u = _load_unified()
    slot = _ensure_ovr_slot(u, profile_id, group_id)
    payload = model_to_dict(body)
    changed = False
    if "forced_off" in payload and payload["forced_off"] is not None:
        slot["forced_off"] = bool(payload["forced_off"]); changed = True
    if "snooze_until" in payload:
        slot["snooze_until"] = payload["snooze_until"]; changed = True
    if "note" in payload and payload["note"] is not None:
        slot["note"] = str(payload["note"]); changed = True
    if changed:
        u["overrides"]["updated_ts"] = _now_iso()
        _save_unified(u)
        print(f"[DEBUG] PATCH overrides -> saved pid={profile_id} gid={group_id}")
    else:
        print(f"[DEBUG] PATCH overrides -> no changes pid={profile_id} gid={group_id}")

    # Wenn forced_off True -> Gruppe sofort auf active=False
    try:
        if payload.get("forced_off") is True:
            print(f"[DEBUG] overrides: force deactivate pid={profile_id} gid={group_id}")
            set_group_active(profile_id, group_id, GroupActivePatch(active=False))
    except Exception as e:
        print(f"[DEBUG] overrides: force deactivate failed: {e}")
    return {"status": "ok", "profile_id": profile_id, "group_id": group_id}

class CommandPost(ApiModel):
    rearm: Optional[bool] = True
    rebaseline: Optional[bool] = False

@router.post("/overrides/{profile_id}/{group_id}/commands", response_model=dict)
def post_command(profile_id: str, group_id: str, body: CommandPost):
    u = _load_unified()
    pb = model_to_dict(body)
    rearm = bool(pb.get("rearm", True))
    rebaseline = bool(pb.get("rebaseline", False))
    u.setdefault("commands", {"queue": []})
    item = {"profile_id": profile_id, "group_id": group_id, "rearm": rearm, "rebaseline": rebaseline, "ts": _now_iso(), "id": _rand_id(8)}
    print(f"[DEBUG] ENQUEUE command -> {item}")
    u["commands"]["queue"].append(item)
    _save_unified(u)
    return {"status": "ok", "profile_id": profile_id, "group_id": group_id, "rearm": rearm, "rebaseline": rebaseline}

# ─────────────────────────────────────────────────────────────
# Registry (UI-Kompat)
# ─────────────────────────────────────────────────────────────
@router.get("/registry/indicators")
def registry_indicators(
    scope: Optional[str] = Query(None),
    include_deprecated: bool = Query(False),
    include_hidden: bool = Query(False),
    expand_presets: bool = Query(False),
):
    items = []
    reg = REGISTERED or {}
    if not expand_presets:
        for key, spec in reg.items():
            s = deepcopy(spec)
            if not s.get("enabled", True): continue
            if scope is not None and scope not in (s.get("scopes") or []): continue
            if not include_deprecated and s.get("deprecated", False): continue
            if not include_hidden and s.get("ui_hidden", False): continue
            items.append(s)
        print(f"[DEBUG] /registry/indicators -> {len(items)} raw specs")
        return items
    for key, s in reg.items():
        if not s.get("enabled", True): continue
        if scope is not None and scope not in (s.get("scopes") or []): continue
        if not include_deprecated and s.get("deprecated", False): continue
        if not include_hidden and s.get("ui_hidden", False): continue
        for p in (s.get("presets") or []):
            label = p.get("label")
            if not isinstance(label, str) or not label: continue
            items.append({
                "display_name": label,
                "base": s.get("name"),
                "params": deepcopy(p.get("params", {})),
                "locked_params": list(p.get("locked_params", [])),
                "outputs": list(s.get("outputs", [])),
            })
    print(f"[DEBUG] /registry/indicators (expanded) -> {len(items)} presets")
    return items

@router.get("/indicators")
def notifier_indicators(include_deprecated: bool = Query(False), include_hidden: bool = Query(False)):
    items = []
    reg = REGISTERED or {}
    for key, s in reg.items():
        if not s.get("enabled", True): continue
        if "notifier" not in (s.get("scopes") or []): continue
        if not include_deprecated and s.get("deprecated", False): continue
        if not include_hidden and s.get("ui_hidden", False): continue
        for p in (s.get("presets") or []):
            label = p.get("label")
            if not isinstance(label, str) or not label: continue
            items.append({
                "display_name": label,
                "base": s.get("name"),
                "params": deepcopy(p.get("params", {})),
                "locked_params": list(p.get("locked_params", [])),
                "outputs": list(s.get("outputs", [])),
            })
    print(f"[DEBUG] /notifier/indicators -> {len(items)} items")
    return items

@router.get("/registry/simple-signals", response_model=List[str])
def registry_simple_signals():
    out = list(SIMPLE_SIGNALS or [])
    print(f"[DEBUG] /registry/simple-signals -> {len(out)} items")
    return out

@router.get("/health")
def health():
    def _stat(p: Path) -> dict:
        try:
            st = _to_path(p).stat(); return {"exists": True, "size": st.st_size, "mtime": st.st_mtime}
        except FileNotFoundError:
            return {"exists": False, "size": 0, "mtime": None}
    u = _load_unified()
    return {
        "unified": _stat(NOTIFIER_UNIFIED),
        "profiles": len(u.get("profiles") or []),
        "alarms": len(u.get("alarms") or []),
        "overrides_profiles": len((u.get("overrides") or {}).get("overrides") or {}),
        "commands_queued": len((u.get("commands") or {}).get("queue") or []),
        "lock_dir": str(_LOCK_DIR)
    }
