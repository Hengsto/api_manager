from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Literal, Optional, Any
from pathlib import Path
from copy import deepcopy

import json
import uuid
import os
import time

from config import PROFILES_NOTIFIER, ALARMS_NOTIFIER
from notifier.indicator_registry import REGISTERED, SIMPLE_SIGNALS


router = APIRouter()

# ─────────────────────────────────────────────────────────────
# Verzeichnisse sicherstellen
# ─────────────────────────────────────────────────────────────
PROFILES_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)
ALARMS_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)

print(f"[DEBUG] Profiles path: {PROFILES_NOTIFIER}")
print(f"[DEBUG] Alarms path:   {ALARMS_NOTIFIER}")

# ─────────────────────────────────────────────────────────────
# Models (wie früher – bewusst simpel/locker)
# ─────────────────────────────────────────────────────────────
class Condition(BaseModel):
    left: str
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte"]
    right: str = ""
    right_absolut: Optional[float] = None
    right_change: Optional[float] = None
    right_symbol: str = ""
    right_interval: str = ""
    logic: Literal["and", "or"] = "and"

class Group(BaseModel):
    conditions: List[Condition]
    active: bool
    symbols: List[str]
    interval: str = ""
    exchange: str = ""
    name: str = ""
    telegram_bot_id: str = ""
    description: str = ""

class ProfileBase(BaseModel):
    name: str
    enabled: bool = True
    condition_groups: List[Group]

class ProfileCreate(ProfileBase):
    id: Optional[str] = None

class ProfileUpdate(ProfileBase):
    pass

class ProfileRead(ProfileBase):
    id: str

class Alarm(BaseModel):
    ts: str
    profile_id: str
    profile_name: str
    symbol: str
    condition: Any
    value_left: float
    value_right: float

# ─────────────────────────────────────────────────────────────
# Utils (simple Lock, keine .bak)
# ─────────────────────────────────────────────────────────────
def model_to_dict(m: BaseModel) -> dict:
    return m.model_dump() if hasattr(m, "model_dump") else m.dict()

def _lock_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".lock")

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
                print(f"[DEBUG] Acquired lock {self.lockfile}")
                return
            except FileExistsError:
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lockfile}")
                time.sleep(self.poll)

    def release(self):
        if self._acquired:
            try:
                os.unlink(self.lockfile)
                print(f"[DEBUG] Released lock {self.lockfile}")
            except FileNotFoundError:
                pass
            finally:
                self._acquired = False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()

def load_json(path: Path, fallback: list) -> list:
    if not path.exists():
        print(f"[DEBUG] load_json -> {path} not found; returning fallback ({len(fallback)} items)")
        return fallback
    try:
        with FileLock(path):
            data = json.loads(path.read_text(encoding="utf-8"))
        print(f"[DEBUG] load_json <- {path} ({len(data)} items)")
        return data
    except Exception as e:
        print(f"⚠️ Fehler beim Lesen {path}: {e}")
        return fallback

def save_json(path: Path, data: list):
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        payload = json.dumps(data, indent=2, ensure_ascii=False)
        with FileLock(path):
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        print(f"[DEBUG] save_json -> {path} ({len(data)} items)")
    except Exception as e:
        print(f"💥 Fehler beim Schreiben {path}: {e}")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        raise

# ─────────────────────────────────────────────────────────────
# Sanitize/Migration (tolerant & deterministisch)
# ─────────────────────────────────────────────────────────────
def _sanitize_condition(c: dict) -> dict:
    # Felder sicherstellen
    c.setdefault("left", "")
    c.setdefault("op", "gt")
    c.setdefault("right", "")
    c.setdefault("right_absolut", None)
    c.setdefault("right_change", None)
    c.setdefault("right_symbol", "")
    c.setdefault("right_interval", "")
    c.setdefault("logic", "and")

    # Genau EIN right* – deterministische Priorität
    flags = [
        ("right_absolut", c.get("right_absolut") is not None),
        ("right", bool(str(c.get("right") or "").strip())),
        ("right_change", c.get("right_change") is not None),
    ]
    n_set = sum(1 for _, ok in flags if ok)
    if n_set == 0:
        c["right"] = ""
    elif n_set > 1:
        # Priorität: right_absolut > right > right_change
        keep = None
        if c.get("right_absolut") is not None:
            keep = "right_absolut"
        elif str(c.get("right") or "").strip():
            keep = "right"
        else:
            keep = "right_change"
        if keep != "right_absolut":
            c["right_absolut"] = None
        if keep != "right":
            c["right"] = ""
        if keep != "right_change":
            c["right_change"] = None

    # Symbollogik: wenn right_symbol gesetzt, sollte right (Output-Key) existieren (zur Sicherheit leer erlauben)
    if c.get("right_symbol") and c.get("right") is None:
        c["right"] = ""

    return c

def _sanitize_group(g: dict) -> dict:
    g.setdefault("conditions", [])
    g.setdefault("active", True)
    g.setdefault("symbols", [])
    g.setdefault("interval", "")
    g.setdefault("exchange", "")
    g.setdefault("name", "")
    g.setdefault("telegram_bot_id", "")
    g.setdefault("description", "")

    if not isinstance(g["symbols"], list):
        g["symbols"] = []

    # Conditions normalisieren
    conds = []
    for raw in g["conditions"] or []:
        if isinstance(raw, dict):
            conds.append(_sanitize_condition(raw))
    g["conditions"] = conds
    return g

def _sanitize_profiles(data: list) -> list:
    out = []
    for p in data or []:
        if not isinstance(p, dict):
            continue
        p.setdefault("name", "Unnamed")
        p.setdefault("enabled", True)
        p.setdefault("condition_groups", [])
        # id als str
        if not p.get("id"):
            p["id"] = str(uuid.uuid4())
        else:
            p["id"] = str(p["id"])

        groups = []
        for g in p.get("condition_groups") or []:
            if isinstance(g, dict):
                groups.append(_sanitize_group(g))
        p["condition_groups"] = groups
        out.append(p)
    return out

# ─────────────────────────────────────────────────────────────
# Endpunkte
# ─────────────────────────────────────────────────────────────
@router.get("/profiles", response_model=List[ProfileRead])
def get_profiles():
    data = load_json(PROFILES_NOTIFIER, [])
    data = _sanitize_profiles(data)

    # fehlende Group-Felder waren oben schon ergänzt; nur speichern, wenn sich was geändert haben könnte
    save_json(PROFILES_NOTIFIER, data)
    return data

@router.post("/profiles", response_model=dict)
def add_profile(p: ProfileCreate):
    profs = load_json(PROFILES_NOTIFIER, [])
    new_profile = model_to_dict(p)
    pid = new_profile.get("id") or str(uuid.uuid4())
    new_profile["id"] = pid
    # defensive sanitize
    new_profile = _sanitize_profiles([new_profile])[0]
    profs.append(new_profile)
    save_json(PROFILES_NOTIFIER, profs)
    print(f"[DEBUG] add_profile -> created id={pid}")
    return {"status": "ok", "id": pid}

@router.put("/profiles/{pid}", response_model=dict)
def update_profile(pid: str, p: ProfileUpdate):
    profs = load_json(PROFILES_NOTIFIER, [])
    updated = False
    for i, item in enumerate(profs):
        if str(item.get("id")) == str(pid):
            updated_item = model_to_dict(p)
            updated_item["id"] = str(pid)
            updated_item = _sanitize_profiles([updated_item])[0]
            profs[i] = updated_item
            updated = True
            break
    if not updated:
        raise HTTPException(status_code=404, detail="Profil nicht gefunden")
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

# ─────────────────────────────────────────────────────────────
# Registry-Endpoints (Indikatoren & Simple Signals)
# ─────────────────────────────────────────────────────────────
@router.get("/registry/indicators")
def registry_indicators(
    scope: Optional[str] = Query(None, description="Filter: notifier|chart|backtest"),
    include_deprecated: bool = Query(False, description="Auch deprecated liefern"),
    include_hidden: bool = Query(False, description="Auch ui_hidden liefern"),
    expand_presets: bool = Query(False, description="Presets zu flachen UI-Einträgen expandieren"),
):
    items = []
    if not expand_presets:
        for key, spec in REGISTERED.items():
            s = deepcopy(spec)
            if not s.get("enabled", True):
                continue
            if scope is not None and scope not in (s.get("scopes") or []):
                continue
            if not include_deprecated and s.get("deprecated", False):
                continue
            if not include_hidden and s.get("ui_hidden", False):
                continue
            items.append(s)
        print(f"[DEBUG] /registry/indicators -> {len(items)} raw specs")
        return items

    for key, spec in REGISTERED.items():
        s = spec
        if not s.get("enabled", True):
            continue
        if scope is not None and scope not in (s.get("scopes") or []):
            continue
        if not include_deprecated and s.get("deprecated", False):
            continue
        if not include_hidden and s.get("ui_hidden", False):
            continue

        for p in (s.get("presets") or []):
            label = p.get("label")
            if not isinstance(label, str) or not label:
                continue
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
def notifier_indicators(
    include_deprecated: bool = Query(False),
    include_hidden: bool = Query(False),
):
    items = []
    for key, spec in REGISTERED.items():
        s = spec
        if not s.get("enabled", True):
            continue
        scopes = s.get("scopes") or []
        if "notifier" not in scopes:
            continue
        if not include_deprecated and s.get("deprecated", False):
            continue
        if not include_hidden and s.get("ui_hidden", False):
            continue

        for p in (s.get("presets") or []):
            label = p.get("label")
            if not isinstance(label, str) or not label:
                continue
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
