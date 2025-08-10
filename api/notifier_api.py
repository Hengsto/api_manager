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

# Zusätzliche Debug-Info
print(f"[DEBUG] Profiles path: {PROFILES_NOTIFIER}")
print(f"[DEBUG] Alarms path:   {ALARMS_NOTIFIER}")

# ─────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────
class Condition(BaseModel):
    left: str
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte"]
    right: str = ""
    right_absolut: Optional[float] = None  # Beibehaltener Feldname
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
    # neu:
    name: str = ""                # Gruppenname
    telegram_bot_id: str = ""     # Telegram Bot ID
    description: str = ""         # Freitext-Beschreibung


class ProfileBase(BaseModel):
    name: str
    enabled: bool = True
    condition_groups: List[Group]

class ProfileCreate(ProfileBase):
    # id optional beim Create
    id: Optional[str] = None

class ProfileUpdate(ProfileBase):
    # id wird im Update-Body ignoriert; Quelle ist URL-Pfad
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
# Utils
# ─────────────────────────────────────────────────────────────

def model_to_dict(m: BaseModel) -> dict:
    # v1/v2 kompatibel
    if hasattr(m, "model_dump"):
        return m.model_dump()
    return m.dict()

def _lock_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".lock")

class FileLock:
    """Einfacher plattformkompatibler Lock über Lock-Datei (exclusive create).
    Release ist robust, Timeout verhindert Hänger.
    """
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
                # Debug
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
    # Bestehender Kommentar bleibt: (keine Änderung)
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
    # Bestehender Kommentar bleibt: (keine Änderung)
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
        # tmp aufräumen, falls vorhanden
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        raise

# ─────────────────────────────────────────────────────────────
# Endpunkte
# ─────────────────────────────────────────────────────────────

@router.get("/profiles", response_model=List[ProfileRead])
def get_profiles():
    data = load_json(PROFILES_NOTIFIER, [])

    # Migration/Normalisierung: fehlende Group-Felder auffüllen
    changed = False
    for p in data:
        groups = p.get("condition_groups") or []
        for g in groups:
            if "name" not in g:
                g["name"] = ""
                changed = True
            if "telegram_bot_id" not in g:
                g["telegram_bot_id"] = ""
                changed = True
            if "description" not in g:
                g["description"] = ""
                changed = True

    if changed:
        print("[DEBUG] get_profiles -> normalized missing group fields; saving back")
        save_json(PROFILES_NOTIFIER, data)

    return data


@router.post("/profiles", response_model=dict)
def add_profile(p: ProfileCreate):
    profs = load_json(PROFILES_NOTIFIER, [])
    new_profile = model_to_dict(p)
    # ID generieren, falls leer
    pid = new_profile.get("id") or str(uuid.uuid4())
    new_profile["id"] = pid
    profs.append(new_profile)
    save_json(PROFILES_NOTIFIER, profs)
    print(f"[DEBUG] add_profile -> created id={pid}")
    return {"status": "ok", "id": pid}

@router.put("/profiles/{pid}", response_model=dict)
def update_profile(pid: str, p: ProfileUpdate):
    profs = load_json(PROFILES_NOTIFIER, [])
    updated = False
    for i, item in enumerate(profs):
        if item.get("id") == pid:
            updated_item = model_to_dict(p)
            updated_item["id"] = pid  # URL ist Quelle der Wahrheit
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
    profs = [p for p in profs if p.get("id") != pid]
    after = len(profs)
    if before == after:
        # Optional: 404 statt silent success
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
    """
    Liefert rohe Specs aus der Registry (optional gefiltert) oder – falls expand_presets=True –
    flache Preset-Einträge (display_name, base, params, locked_params, outputs).
    """
    items = []

    if not expand_presets:
        # Rohe Specs zurückgeben (defensive Kopie)
        for key, spec in REGISTERED.items():
            s = deepcopy(spec)
            # Filter anwenden
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

    # Presets expandieren
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
    """
    UI-fertige Liste NUR für den Notifier-Scope, Presets expandiert.
    """
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
                "display_name": label,                  # z.B. EMA_14
                "base": s.get("name"),                  # z.B. ema
                "params": deepcopy(p.get("params", {})),
                "locked_params": list(p.get("locked_params", [])),
                "outputs": list(s.get("outputs", [])),
            })
    print(f"[DEBUG] /notifier/indicators -> {len(items)} items (presets)")
    return items


@router.get("/registry/simple-signals", response_model=List[str])
def registry_simple_signals():
    """
    Liefert die einfachen (parameterlosen) Signale wie golden_cross, death_cross, ...
    """
    out = list(SIMPLE_SIGNALS or [])
    print(f"[DEBUG] /registry/simple-signals -> {len(out)} items")
    return out

