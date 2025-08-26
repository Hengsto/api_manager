# api/notifier_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
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

from config import PROFILES_NOTIFIER, ALARMS_NOTIFIER
from notifier.indicator_registry import REGISTERED, SIMPLE_SIGNALS

router = APIRouter()

# ─────────────────────────────────────────────────────────────
# Verzeichnisse
# ─────────────────────────────────────────────────────────────
PROFILES_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)
ALARMS_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)

print(f"[DEBUG] Profiles path: {PROFILES_NOTIFIER}")
print(f"[DEBUG] Alarms   path: {ALARMS_NOTIFIER}")

def _is_relative_to(p: Path, base: Path) -> bool:
    try:
        p.resolve().relative_to(base.resolve()); return True
    except Exception:
        return False

try:
    _CWD = Path.cwd()
    if _is_relative_to(PROFILES_NOTIFIER, _CWD) or _is_relative_to(ALARMS_NOTIFIER, _CWD):
        print("⚠️  WARN: JSONs liegen im Projektbaum → Hot-Reload-Risiko. Lege sie besser außerhalb ab.")
except Exception as _e:
    print(f"[DEBUG] cwd check skipped: {_e}")

# ─────────────────────────────────────────────────────────────
# Pydantic v1/v2 kompatibel (extra='allow')
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

# ─────────────────────────────────────────────────────────────
# IDs & Utils
# ─────────────────────────────────────────────────────────────
def _rand_id(n: int = 6) -> str:
    return "".join(random.choices(string.hexdigits.lower(), k=n))

def _trim_str(x: Any, dash_to_empty: bool = True) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if dash_to_empty and s == "—":
        return ""
    return s

# Slope-Params: bp.* in base_params heben (bp.* hat Vorrang)
def _normalize_slope_params_dict(p: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(p, dict):
        return {}
    out = dict(p)
    bp = {k[3:]: v for k, v in p.items()
          if isinstance(k, str) and k.startswith("bp.") and v not in (None, "")}
    if bp:
        nested = dict(p.get("base_params") or {})
        nested.update(bp)  # bp.* > base_params
        out["base_params"] = nested
    return out

# ─────────────────────────────────────────────────────────────
# READ-Modelle (strikt) – enthalten gid/rid
# ─────────────────────────────────────────────────────────────
class ConditionOut(ApiModel):
    rid: str
    left: str
    op: Literal["eq","ne","gt","gte","lt","lte"]
    right: str = ""
    right_symbol: str = ""
    right_interval: str = ""
    left_output: str = ""
    right_output: str = ""
    logic: Literal["and","or"] = "and"
    left_params: Dict[str, Any] = Field(default_factory=dict)
    right_params: Dict[str, Any] = Field(default_factory=dict)
    # explizit unterstützen (UI nutzt das)
    left_symbol: str = ""
    left_interval: str = ""

class GroupOut(ApiModel):
    gid: str
    conditions: List[ConditionOut]
    active: bool
    symbols: List[str]
    interval: str = ""
    exchange: str = ""
    name: str = ""
    telegram_bot_id: str = ""
    description: str = ""

class ProfileBaseOut(ApiModel):
    name: str
    enabled: bool = True
    condition_groups: List[GroupOut]

class ProfileRead(ProfileBaseOut):
    id: str

# ─────────────────────────────────────────────────────────────
# WRITE-Modelle (tolerant) – Drafts erlaubt, gid/rid optional
# ─────────────────────────────────────────────────────────────
class ConditionIn(ApiModel):
    rid: Optional[str] = None
    left: Optional[str] = ""
    op: Optional[Literal["eq","ne","gt","gte","lt","lte"]] = "gt"
    right: Union[str, float, int, None] = ""
    right_symbol: Optional[str] = ""
    right_interval: Optional[str] = ""
    left_output: Optional[str] = ""
    right_output: Optional[str] = ""
    logic: Optional[Literal["and","or"]] = "and"
    left_params: Dict[str, Any] = Field(default_factory=dict)
    right_params: Dict[str, Any] = Field(default_factory=dict)
    # UI-Felder
    left_symbol: Optional[str] = ""
    left_interval: Optional[str] = ""

class GroupIn(ApiModel):
    gid: Optional[str] = None
    conditions: List[ConditionIn] = Field(default_factory=list)
    active: bool = True
    symbols: List[str] = Field(default_factory=list)
    interval: str = ""
    exchange: str = ""
    name: str = ""
    telegram_bot_id: str = ""
    description: str = ""

class ProfileBaseIn(ApiModel):
    name: str
    enabled: bool = True
    condition_groups: List[GroupIn] = Field(default_factory=list)

class ProfileCreate(ProfileBaseIn):
    id: Optional[str] = None

class ProfileUpdate(ProfileBaseIn):
    pass

# ─────────────────────────────────────────────────────────────
# Locks (außerhalb Projektbaum)
# ─────────────────────────────────────────────────────────────
_ENV_LOCK_DIR = os.environ.get("NOTIFIER_LOCK_DIR", "").strip()
_LOCK_DIR = Path(_ENV_LOCK_DIR) if _ENV_LOCK_DIR else Path(tempfile.gettempdir()) / "notifier_locks"
_LOCK_DIR.mkdir(parents=True, exist_ok=True)
print(f"[DEBUG] Using external lock dir: {_LOCK_DIR}")

def _lock_path(path: Path) -> Path:
    return _LOCK_DIR / (path.name + ".lock")

class FileLock:
    def __init__(self, path: Path, timeout: float = 10.0, poll: float = 0.1):
        self.lockfile = _lock_path(path)
        self.timeout = timeout
        self.poll = poll
        self._acquired = False
    def acquire(self):
        start = time.time()
        while True:
            try:
                fd = os.open(str(self.lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                self._acquired = True
                print(f"[DEBUG] Acquired lock {self.lockfile}"); return
            except FileExistsError:
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lockfile}")
                time.sleep(self.poll)
    def release(self):
        if self._acquired:
            try:
                os.unlink(self.lockfile); print(f"[DEBUG] Released lock {self.lockfile}")
            except FileNotFoundError:
                pass
            finally:
                self._acquired = False
    def __enter__(self): self.acquire(); return self
    def __exit__(self, exc_type, exc, tb): self.release()

# ─────────────────────────────────────────────────────────────
# JSON IO (write-only-on-change + atomar)
# ─────────────────────────────────────────────────────────────
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

def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def load_json(path: Path, fallback: list) -> list:
    if not path.exists():
        print(f"[DEBUG] load_json -> {path} not found; returning fallback ({len(fallback)} items)")
        return fallback
    try:
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        print(f"[DEBUG] load_json <- {path} ({len(data)} items)")
        return data
    except Exception as e:
        print(f"⚠️ Fehler beim Lesen {path}: {e}")
        return fallback

def save_json(path: Path, data: list):
    payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    try:
        if path.exists():
            cur = path.read_bytes()
            if len(cur) == len(payload) and _sha256_bytes(cur) == _sha256_bytes(payload):
                print(f"[DEBUG] save_json -> SKIP (content unchanged) {path}")
                return
    except Exception as e:
        print(f"[DEBUG] save_json compare failed ({e}); continue write")
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with FileLock(path):
            with open(tmp, "wb") as f:
                f.write(payload); f.flush(); os.fsync(f.fileno())
            os.replace(tmp, path)
            try:
                if hasattr(os, "O_DIRECTORY"):
                    dfd = os.open(str(path.parent), os.O_DIRECTORY)
                    try: os.fsync(dfd)
                    finally: os.close(dfd)
            except Exception as e:
                print(f"[DEBUG] dir fsync skipped: {e}")
        print(f"[DEBUG] save_json -> {path} ({len(data)} items)")
    except Exception as e:
        print(f"💥 Fehler beim Schreiben {path}: {e}")
        try:
            if tmp.exists(): tmp.unlink()
        except Exception: pass
        raise

# ─────────────────────────────────────────────────────────────
# Sanitize + ID-Pflege
# ─────────────────────────────────────────────────────────────
_ALLOWED_OPS = {"eq","ne","gt","gte","lt","lte"}
_ALLOWED_LOGIC = {"and","or"}

def _sanitize_condition(c: dict) -> dict:
    # Defaults
    c.setdefault("rid", _rand_id())
    c.setdefault("left", "")
    c.setdefault("op", "gt")
    c.setdefault("right", "")
    c.setdefault("right_symbol", "")
    c.setdefault("right_interval", "")
    c.setdefault("left_output", "")
    c.setdefault("right_output", "")
    c.setdefault("logic", "and")
    c.setdefault("left_params", {})
    c.setdefault("right_params", {})
    # NEU: explizit unterstützen
    c.setdefault("left_symbol", "")
    c.setdefault("left_interval", "")

    # Typkorrekturen / Trim
    for k in ("left","right_symbol","right_interval","left_output","right_output","logic","op","left_symbol","left_interval"):
        c[k] = _trim_str(c.get(k))

    # Operator/Logic validieren
    if c["op"] not in _ALLOWED_OPS:
        print(f"[DEBUG] sanitize_condition: invalid op='{c['op']}' -> 'gt'")
        c["op"] = "gt"
    if c["logic"] not in _ALLOWED_LOGIC:
        print(f"[DEBUG] sanitize_condition: invalid logic='{c['logic']}' -> 'and'")
        c["logic"] = "and"

    # Schlucke Altlasten
    for k in ("right_absolut","right_absolute","right_change"):
        if k in c:
            print(f"[DEBUG] sanitize_condition: drop legacy '{k}'")
            c.pop(k, None)

    # right → String normieren (UI vergleicht Strings/Indicator-Namen)
    rv = c.get("right")
    if isinstance(rv, (int, float)):
        c["right"] = str(rv)
    elif rv is None:
        c["right"] = ""
    else:
        c["right"] = _trim_str(rv)

    # Params: Dict erzwingen
    if not isinstance(c["left_params"], dict):  c["left_params"]  = {}
    if not isinstance(c["right_params"], dict): c["right_params"] = {}

    # Slope-Params normalisieren (bp.* nach base_params, bp.* > base_params)
    if _trim_str(c.get("left"), dash_to_empty=False).lower() == "slope":
        c["left_params"] = _normalize_slope_params_dict(c["left_params"])
    if _trim_str(c.get("right"), dash_to_empty=False).lower() == "slope":
        c["right_params"] = _normalize_slope_params_dict(c["right_params"])

    # rid stabilisieren/kürzen
    rid = _trim_str(c.get("rid"))
    if not rid: rid = _rand_id()
    c["rid"] = rid
    return c

def _sanitize_group(g: dict) -> dict:
    g.setdefault("gid", _rand_id())
    g.setdefault("conditions", [])
    g.setdefault("active", True)
    g.setdefault("symbols", [])
    g.setdefault("interval", "")
    g.setdefault("exchange", "")
    g.setdefault("name", "")
    g.setdefault("telegram_bot_id", "")
    g.setdefault("description", "")

    # Trim Strings
    for k in ("interval","exchange","name","telegram_bot_id","description"):
        g[k] = _trim_str(g.get(k))

    # Symbols Liste
    if not isinstance(g["symbols"], list):
        g["symbols"] = []
    else:
        g["symbols"] = [s for s in (g["symbols"] or []) if isinstance(s, str) and _trim_str(s)]

    # Conditions
    conds = []
    for raw in g.get("conditions") or []:
        if isinstance(raw, dict):
            conds.append(_sanitize_condition(raw))
    g["conditions"] = conds

    gid = _trim_str(g.get("gid"))
    if not gid: gid = _rand_id()
    g["gid"] = gid

    # Duplikate rid in einer Gruppe vermeiden
    seen = set()
    for c in g["conditions"]:
        if c["rid"] in seen:
            c["rid"] = _rand_id()
        seen.add(c["rid"])

    return g

def _sanitize_profiles(data: list) -> list:
    out = []
    for p in data or []:
        if not isinstance(p, dict): continue
        p.setdefault("name", "Unnamed")
        p.setdefault("enabled", True)
        p.setdefault("condition_groups", [])
        p["id"] = str(p.get("id") or uuid.uuid4())

        groups = []
        for g in p.get("condition_groups") or []:
            if isinstance(g, dict): groups.append(_sanitize_group(g))

        # gid-Duplikate vermeiden
        seen = set()
        for g in groups:
            if g["gid"] in seen:
                g["gid"] = _rand_id()
            seen.add(g["gid"])

        p["condition_groups"] = groups
        out.append(p)
    return out

def _merge_ids(old_p: dict, new_p: dict) -> dict:
    """
    Übernimmt fehlende gid/rid aus old_p → new_p positionsbasiert.
    """
    old_groups = old_p.get("condition_groups") or []
    new_groups = new_p.get("condition_groups") or []

    for gi, ng in enumerate(new_groups):
        og = old_groups[gi] if gi < len(old_groups) else None
        # Gruppe: gid
        if not _trim_str(ng.get("gid")):
            if og and _trim_str(og.get("gid")):
                ng["gid"] = og["gid"]
            else:
                ng["gid"] = _rand_id()
        # Bedingungen: rid je Position
        old_conds = (og.get("conditions") if og else []) or []
        new_conds = ng.get("conditions") or []
        for ci, nc in enumerate(new_conds):
            oc = old_conds[ci] if ci < len(old_conds) else None
            if not _trim_str(nc.get("rid")):
                if oc and _trim_str(oc.get("rid")):
                    nc["rid"] = oc["rid"]
                else:
                    nc["rid"] = _rand_id()
        # innerhalb Gruppe Duplikate rid bereinigen
        seen = set()
        for c in new_conds:
            if c["rid"] in seen:
                c["rid"] = _rand_id()
            seen.add(c["rid"])
    # Gruppen-gid Duplikate
    seen_g = set()
    for g in new_groups:
        if g["gid"] in seen_g:
            g["gid"] = _rand_id()
        seen_g.add(g["gid"])

    return new_p

# ─────────────────────────────────────────────────────────────
# Endpunkte
# ─────────────────────────────────────────────────────────────
@router.get("/profiles", response_model=List[ProfileRead])
def get_profiles():
    data = load_json(PROFILES_NOTIFIER, [])
    sanitized = _sanitize_profiles(data)
    print(f"[DEBUG] get_profiles -> {len(sanitized)} profiles")
    return sanitized

@router.get("/profiles/{pid}", response_model=ProfileRead)
def get_profile(pid: str):
    data = load_json(PROFILES_NOTIFIER, [])
    for p in _sanitize_profiles(data):
        if str(p.get("id")) == str(pid):
            print(f"[DEBUG] get_profile -> id={pid} found")
            return p
    print(f"[DEBUG] get_profile -> id={pid} not found")
    raise HTTPException(status_code=404, detail="Profil nicht gefunden")

@router.post("/profiles", response_model=dict)
def add_profile(p: ProfileCreate):
    profs = load_json(PROFILES_NOTIFIER, [])
    incoming = model_to_dict(p)

    pid = incoming.get("id") or str(uuid.uuid4())
    incoming["id"] = pid

    # sanitize + IDs
    new_groups = []
    for g in incoming.get("condition_groups", []) or []:
        if isinstance(g, dict): new_groups.append(_sanitize_group(g))
    incoming["condition_groups"] = new_groups

    print(f"[DEBUG] add_profile <- {json.dumps(incoming, ensure_ascii=False)[:800]}...")
    profs.append(incoming)
    save_json(PROFILES_NOTIFIER, profs)
    print(f"[DEBUG] add_profile -> created id={pid} (total={len(profs)})")
    return {"status": "ok", "id": pid}

@router.put("/profiles/{pid}", response_model=dict)
def update_profile(pid: str, p: ProfileUpdate):
    profs = load_json(PROFILES_NOTIFIER, [])
    incoming = model_to_dict(p)

    # sanitize tolerant
    new_groups = []
    for g in incoming.get("condition_groups", []) or []:
        if isinstance(g, dict): new_groups.append(_sanitize_group(g))
    incoming["condition_groups"] = new_groups

    idx = next((i for i, it in enumerate(profs) if str(it.get("id")) == str(pid)), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Profil nicht gefunden")

    incoming["id"] = pid

    # IDs mergen (vor Compare, damit Stabilität erhalten bleibt)
    old_norm = _sanitize_profiles([profs[idx]])[0]
    merged   = _merge_ids(old_norm, deepcopy(incoming))

    old_norm_json = json.dumps(old_norm, sort_keys=True, ensure_ascii=False)
    new_norm_json = json.dumps(_sanitize_profiles([merged])[0], sort_keys=True, ensure_ascii=False)

    if old_norm_json == new_norm_json:
        print(f"[DEBUG] update_profile -> NO CHANGE (skip save) id={pid}")
        return {"status": "ok", "id": pid}  # <-- WICHTIG: 'ok' statt 'unchanged' für UI

    profs[idx] = merged
    save_json(PROFILES_NOTIFIER, profs)
    print(f"[DEBUG] update_profile -> updated id={pid}")
    return {"status": "updated", "id": pid}

@router.delete("/profiles/{pid}", response_model=dict)
def delete_profile(pid: str):
    profs = load_json(PROFILES_NOTIFIER, [])
    before = len(profs)
    profs = [p for p in profs if str(p.get("id")) != str(pid)]
    after = len(profs)
    if before == after:
        print(f"[DEBUG] delete_profile -> id={pid} not found (no-op)")
    else:
        print(f"[DEBUG] delete_profile -> removed id={pid}")
    save_json(PROFILES_NOTIFIER, profs)
    return {"status": "deleted", "id": pid}

# Registry
@router.get("/registry/indicators")
def registry_indicators(
    scope: Optional[str] = Query(None, description="Filter: notifier|chart|backtest"),
    include_deprecated: bool = Query(False),
    include_hidden: bool = Query(False),
    expand_presets: bool = Query(False),
):
    items = []
    if not expand_presets:
        for key, spec in REGISTERED.items():
            s = deepcopy(spec)
            if not s.get("enabled", True): continue
            if scope is not None and scope not in (s.get("scopes") or []): continue
            if not include_deprecated and s.get("deprecated", False): continue
            if not include_hidden and s.get("ui_hidden", False): continue
            items.append(s)
        print(f"[DEBUG] /registry/indicators -> {len(items)} raw specs")
        return items
    for key, spec in REGISTERED.items():
        s = spec
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

@router.get("/notifier/indicators")
def notifier_indicators(include_deprecated: bool = Query(False), include_hidden: bool = Query(False)):
    items = []
    for key, spec in REGISTERED.items():
        s = spec
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
    print(f"[DEBUG] /notifier/indicators -> {len(items)} items (presets)")
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
            st = p.stat(); return {"exists": True, "size": st.st_size, "mtime": st.st_mtime}
        except FileNotFoundError:
            return {"exists": False, "size": 0, "mtime": None}
    return {"profiles": _stat(PROFILES_NOTIFIER), "alarms": _stat(ALARMS_NOTIFIER), "lock_dir": str(_LOCK_DIR)}
