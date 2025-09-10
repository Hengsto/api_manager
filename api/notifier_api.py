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
from datetime import datetime, timezone

from config import PROFILES_NOTIFIER as _PROFILES_NOTIFIER_CFG, ALARMS_NOTIFIER as _ALARMS_NOTIFIER_CFG

# ✨ NEU: optionale Pfade für Overrides/Commands (Fallbacks, wenn nicht in config definiert)
try:
    from config import OVERRIDES_NOTIFIER as _OVERRIDES_NOTIFIER_CFG  # optional
except Exception:
    _OVERRIDES_NOTIFIER_CFG = None
try:
    from config import COMMANDS_NOTIFIER as _COMMANDS_NOTIFIER_CFG  # optional
except Exception:
    _COMMANDS_NOTIFIER_CFG = None

from notifier.indicator_registry import REGISTERED, SIMPLE_SIGNALS

router = APIRouter(prefix="/notifier")

# ─────────────────────────────────────────────────────────────
# Pfade robust normalisieren (str → Path)
# ─────────────────────────────────────────────────────────────
def _to_path(p: Any) -> Path:
    if isinstance(p, Path):
        return p
    return Path(str(p)).expanduser().resolve()

PROFILES_NOTIFIER: Path = _to_path(_PROFILES_NOTIFIER_CFG)
ALARMS_NOTIFIER:   Path = _to_path(_ALARMS_NOTIFIER_CFG)

# ✨ NEU: Overrides/Commands Pfade (Fallback neben PROFILES_NOTIFIER)
_OVERRIDES_FALLBACK = PROFILES_NOTIFIER.parent / "notifier_overrides.json"
_COMMANDS_FALLBACK  = PROFILES_NOTIFIER.parent / "notifier_commands.json"
OVERRIDES_NOTIFIER: Path = _to_path(_OVERRIDES_NOTIFIER_CFG) if _OVERRIDES_NOTIFIER_CFG else _OVERRIDES_FALLBACK
COMMANDS_NOTIFIER:  Path = _to_path(_COMMANDS_NOTIFIER_CFG)  if _COMMANDS_NOTIFIER_CFG  else _COMMANDS_FALLBACK

# ─────────────────────────────────────────────────────────────
# Verzeichnisse
# ─────────────────────────────────────────────────────────────
try:
    PROFILES_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)
    ALARMS_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)
    OVERRIDES_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)
    COMMANDS_NOTIFIER.parent.mkdir(parents=True, exist_ok=True)
except Exception as _mkerr:
    print(f"💥 mkdir for JSON dirs failed: {_mkerr}")

print(f"[DEBUG] Profiles path: {PROFILES_NOTIFIER}")
print(f"[DEBUG] Alarms   path: {ALARMS_NOTIFIER}")
print(f"[DEBUG] Overrides path: {OVERRIDES_NOTIFIER}")
print(f"[DEBUG] Commands  path: {COMMANDS_NOTIFIER}")

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

def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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

# ▼▼▼ Deactivate-Mode Normalisierung
_ALLOWED_DEACT = {"always", "true", "any_true"}

def _normalize_deactivate_value(v: Any) -> Optional[str]:
    """
    Normalisiert UI/Legacy-Werte auf 'always' | 'true' | 'any_true' | None.
      - Strings:
          'always'                   => 'always'
          'true'|'full'|'match'      => 'true'
          'any_true'|'any'|'partial' => 'any_true'
          ''/None                    => None  (bewusst: keine Migration)
      - Bool:
          True  => 'true'
          False => None
      - Sonst: None
    """
    if v is None:
        return None
    if isinstance(v, bool):
        out = "true" if v else None
        print(f"[DEBUG] _normalize_deactivate_value(bool) -> {out}")
        return out
    s = _trim_str(v).lower()
    if not s:
        print(f"[DEBUG] _normalize_deactivate_value(empty) -> None")
        return None
    if s == "always":
        print(f"[DEBUG] _normalize_deactivate_value('always') -> 'always'")
        return "always"
    if s in {"true", "full", "match"}:
        print(f"[DEBUG] _normalize_deactivate_value('{s}') -> 'true'")
        return "true"
    if s in {"any_true", "any", "partial"}:
        print(f"[DEBUG] _normalize_deactivate_value('{s}') -> 'any_true'")
        return "any_true"
    print(f"[DEBUG] _normalize_deactivate_value('{s}') -> None (invalid)")
    return None

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
    # unterstützt jetzt auch "always"
    deactivate_on: Optional[Literal["always","true","any_true"]] = None
    # ✨ NEU: Mindestanzahl konsekutiver True-Ticks bevor die Gruppe "feuert"
    min_true_ticks: Optional[int] = None

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
    # unterstützt jetzt auch "always"
    deactivate_on: Optional[Literal["always","true","any_true"]] = None
    auto_deactivate: Optional[bool] = None  # Legacy-Eingänge tolerieren
    # ✨ NEU: Mindestanzahl konsekutiver True-Ticks (optional)
    min_true_ticks: Optional[int] = None

class ProfileBaseIn(ApiModel):
    name: str
    enabled: bool = True
    condition_groups: List[GroupIn] = Field(default_factory=list)

class ProfileCreate(ProfileBaseIn):
    id: Optional[str] = None

class ProfileUpdate(ProfileBaseIn):
    # ✨ NEU: Master-Reset Flags tolerieren (Variante B)
    active: Optional[bool] = None  # tolerieren, wird zu enabled gemappt
    activate: Optional[bool] = None
    rebaseline: Optional[bool] = None
    pass

# ─────────────────────────────────────────────────────────────
# Locks (außerhalb Projektbaum)
# ─────────────────────────────────────────────────────────────
_ENV_LOCK_DIR = os.environ.get("NOTIFIER_LOCK_DIR", "").strip()
_LOCK_DIR = Path(_ENV_LOCK_DIR) if _ENV_LOCK_DIR else Path(tempfile.gettempdir()) / "notifier_locks"
_LOCK_DIR.mkdir(parents=True, exist_ok=True)
print(f"[DEBUG] Using external lock dir: {_LOCK_DIR}")

def _lock_path(path: Path) -> Path:
    try:
        name = Path(path).name
    except Exception:
        name = str(path)
    return _LOCK_DIR / (name + ".lock")

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

def load_json(path: Path, fallback: list) -> list:
    path = _to_path(path)
    if not path.exists():
        print(f"[DEBUG] load_json -> {path} not found; returning fallback ({len(fallback)} items)")
        return fallback
    try:
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        if not isinstance(data, list):
            print(f"[DEBUG] load_json <- {path} (non-list root, coercing)")
            data = []
        else:
            print(f"[DEBUG] load_json <- {path} ({len(data)} items)")
        return data
    except Exception as e:
        print(f"⚠️ Fehler beim Lesen {path}: {e}")
        return fallback

def save_json(path: Path, data: list):
    path = _to_path(path)
    payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception as e:
        print(f"[DEBUG] tmp unlink failed (pre): {e}")

    try:
        # Short-circuit bei identischem Inhalt
        if path.exists():
            try:
                cur = path.read_bytes()
                if len(cur) == len(payload) and _sha256_bytes(cur) == _sha256_bytes(payload):
                    print(f"[DEBUG] save_json -> SKIP (content unchanged) {path}")
                    return
            except Exception as e:
                print(f"[DEBUG] save_json compare failed ({e}); continue write")

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

# ✨ NEU: Dict-Wurzel IO (Overrides/Commands)
def load_json_any(path: Path, fallback: Any) -> Any:
    path = _to_path(path)
    if not path.exists():
        print(f"[DEBUG] load_json_any -> {path} not found; returning fallback")
        return deepcopy(fallback)
    try:
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        print(f"[DEBUG] load_json_any <- {path} (type={type(data).__name__})")
        return data
    except Exception as e:
        print(f"⚠️ Fehler beim Lesen {path}: {e}")
        return deepcopy(fallback)

def save_json_any(path: Path, data: Any):
    path = _to_path(path)
    payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception as e:
        print(f"[DEBUG] tmp unlink failed (pre): {e}")
    try:
        if path.exists():
            try:
                cur = path.read_bytes()
                if len(cur) == len(payload) and _sha256_bytes(cur) == _sha256_bytes(payload):
                    print(f"[DEBUG] save_json_any -> SKIP (content unchanged) {path}")
                    return
            except Exception as e:
                print(f"[DEBUG] save_json_any compare failed ({e}); continue write")
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
        print(f"[DEBUG] save_json_any -> {path}")
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

    # right → String normieren
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

    # Slope-Params normalisieren
    if _trim_str(c.get("left"), dash_to_empty=False).lower() == "slope":
        c["left_params"] = _normalize_slope_params_dict(c["left_params"])
    if _trim_str(c.get("right"), dash_to_empty=False).lower() == "slope":
        c["right_params"] = _normalize_slope_params_dict(c["right_params"])

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
    # ▼▼▼ Defaults für neue/legacy Felder
    if "deactivate_on" not in g:
        g["deactivate_on"] = None
    if "auto_deactivate" not in g:
        g["auto_deactivate"] = None
    # ✨ NEU: Default für min_true_ticks (Key immer vorhanden)
    if "min_true_ticks" not in g:
        g["min_true_ticks"] = None

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

    # ✨ NEU: min_true_ticks robust normalisieren (Key IMMER behalten)
    _raw_mtt = g.get("min_true_ticks", None)
    _mtt: Optional[int] = None
    try:
        if _raw_mtt not in (None, "", "null"):
            _mtt = int(_raw_mtt)
    except Exception:
        print(f"[DEBUG] _sanitize_group: invalid min_true_ticks={_raw_mtt!r} -> set None")
        _mtt = None
    if _mtt is not None and _mtt < 1:
        print(f"[DEBUG] _sanitize_group: min_true_ticks={_mtt} < 1 -> set None")
        _mtt = None
    # WICHTIG: Key immer setzen, auch wenn None → erscheint in API
    g["min_true_ticks"] = _mtt

    # ▼▼▼ deactivate_on normalisieren (+ Legacy auto_deactivate)
    before_deact = g.get("deactivate_on")
    before_legacy = g.get("auto_deactivate")
    norm = _normalize_deactivate_value(before_deact)
    if norm is None:
        norm = _normalize_deactivate_value(before_legacy)  # True => "true"
        if before_legacy is not None:
            print(f"[DEBUG] _sanitize_group: legacy auto_deactivate={before_legacy} -> deactivate_on={norm}")
    if norm in _ALLOWED_DEACT:
        if before_deact != norm:
            print(f"[DEBUG] _sanitize_group: normalize deactivate_on '{before_deact}' -> '{norm}'")
        g["deactivate_on"] = norm
    else:
        if before_deact not in (None, ""):
            print(f"[DEBUG] _sanitize_group: drop invalid deactivate_on='{before_deact}'")
        g.pop("deactivate_on", None)
    # Legacy-Feld entfernen
    if "auto_deactivate" in g:
        g.pop("auto_deactivate", None)

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
# ✨ NEU: Overrides + Commands Helpers
# ─────────────────────────────────────────────────────────────
_OVR_TEMPLATE: Dict[str, Dict[str, Dict[str, Any]]] = {"overrides": {}, "updated_ts": None}
_CMD_TEMPLATE: Dict[str, List[Dict[str, Any]]] = {"queue": []}

def _load_overrides() -> Dict[str, Any]:
    data = load_json_any(OVERRIDES_NOTIFIER, deepcopy(_OVR_TEMPLATE))
    if not isinstance(data, dict) or "overrides" not in data:
        print("[DEBUG] _load_overrides: coercing to template")
        data = deepcopy(_OVR_TEMPLATE)
    if "overrides" not in data: data["overrides"] = {}
    return data

def _save_overrides(data: Dict[str, Any]) -> None:
    data = deepcopy(data)
    data["updated_ts"] = _now_iso()
    save_json_any(OVERRIDES_NOTIFIER, data)

def _ensure_ovr_slot(ovr: Dict[str, Any], pid: str, gid: str) -> Dict[str, Any]:
    ovr.setdefault("overrides", {})
    ovr["overrides"].setdefault(pid, {})
    ovr["overrides"][pid].setdefault(gid, {"forced_off": False, "snooze_until": None, "note": None})
    return ovr["overrides"][pid][gid]

def _load_commands() -> Dict[str, Any]:
    data = load_json_any(COMMANDS_NOTIFIER, deepcopy(_CMD_TEMPLATE))
    if not isinstance(data, dict) or "queue" not in data:
        print("[DEBUG] _load_commands: coercing to template")
        data = deepcopy(_CMD_TEMPLATE)
    if "queue" not in data: data["queue"] = []
    return data

def _save_commands(data: Dict[str, Any]) -> None:
    save_json_any(COMMANDS_NOTIFIER, data)

def _enqueue_command(pid: str, gid: str, rearm: bool = True, rebaseline: bool = False) -> None:
    cmds = _load_commands()
    item = {
        "profile_id": pid,
        "group_id": gid,
        "rearm": bool(rearm),
        "rebaseline": bool(rebaseline),
        "ts": _now_iso(),
        "id": _rand_id(8),
    }
    print(f"[DEBUG] _enqueue_command -> {item}")
    cmds["queue"].append(item)
    _save_commands(cmds)

# ─────────────────────────────────────────────────────────────
# Endpunkte: PROFILES
# ─────────────────────────────────────────────────────────────
@router.get(
    "/profiles",
    response_model=List[ProfileRead],
    response_model_exclude_unset=False,
    response_model_exclude_none=False
)
def get_profiles():
    data = load_json(PROFILES_NOTIFIER, [])
    sanitized = _sanitize_profiles(data)
    print(f"[DEBUG] get_profiles -> {len(sanitized)} profiles")
    # Debug: zeig Keys der ersten Gruppe, falls vorhanden
    try:
        if sanitized and (sanitized[0].get("condition_groups") or []):
            print(f"[DEBUG] get_profiles: first group keys -> {list((sanitized[0]['condition_groups'][0]).keys())}")
    except Exception as e:
        print(f"[DEBUG] get_profiles: keys debug failed: {e}")
    return sanitized

@router.get(
    "/profiles/{pid}",
    response_model=ProfileRead,
    response_model_exclude_unset=False,
    response_model_exclude_none=False
)
def get_profile(pid: str):
    data = load_json(PROFILES_NOTIFIER, [])
    for p in _sanitize_profiles(data):
        if str(p.get("id")) == str(pid):
            print(f"[DEBUG] get_profile -> id={pid} found")
            try:
                if (p.get("condition_groups") or []):
                    print(f"[DEBUG] get_profile: group keys -> {list((p['condition_groups'][0]).keys())}")
            except Exception as e:
                print(f"[DEBUG] get_profile: keys debug failed: {e}")
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

    # ✨ NEU: Toleranz für active/enabled (Map auf enabled)
    if "active" in incoming and "enabled" not in incoming:
        incoming["enabled"] = bool(incoming.get("active"))
        print(f"[DEBUG] update_profile: mapped active->{incoming['enabled']} for id={pid}")

    activate_flag   = bool(incoming.get("activate", False))
    rebaseline_flag = bool(incoming.get("rebaseline", False))

    # sanitize tolerant
    new_groups = []
    for g in incoming.get("condition_groups", []) or []:
        if isinstance(g, dict): new_groups.append(_sanitize_group(g))
    incoming["condition_groups"] = new_groups

    idx = next((i for i, it in enumerate(profs) if str(it.get("id")) == str(pid)), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Profil nicht gefunden")

    # Vorherzustand normalisieren (für Compare + Toggle-Erkennung)
    before_norm = _sanitize_profiles([profs[idx]])[0]
    before_enabled = bool(before_norm.get("enabled", True))

    incoming["id"] = pid

    # IDs mergen (vor Compare, damit Stabilität erhalten bleibt)
    merged   = _merge_ids(before_norm, deepcopy(incoming))
    after_norm = _sanitize_profiles([merged])[0]
    after_enabled = bool(after_norm.get("enabled", True))

    # Short-circuit wenn nichts geändert
    if json.dumps(before_norm, sort_keys=True, ensure_ascii=False) == json.dumps(after_norm, sort_keys=True, ensure_ascii=False):
        print(f"[DEBUG] update_profile -> NO CHANGE (skip save) id={pid}")
        # selbst wenn kein Profilsave, kann activate_flag gesetzt sein → Routine dennoch ausführen
        if activate_flag or (not before_enabled and after_enabled):
            print(f"[DEBUG] update_profile -> run activation routine (no profile change) id={pid}")
            _run_activation_routine(after_norm, activate_flag=True, rebaseline=rebaseline_flag)
        return {"status": "ok", "id": pid}

    # Persistiere Profil
    profs[idx] = after_norm
    save_json(PROFILES_NOTIFIER, profs)
    print(f"[DEBUG] update_profile -> updated id={pid}")

    # ✨ NEU: Aktivierungs-Routine nur, wenn aktiviert wurde (Flag ODER false→true)
    should_activate = activate_flag or (not before_enabled and after_enabled)
    if should_activate:
        print(f"[DEBUG] update_profile -> run activation routine id={pid} (activate={activate_flag}, rebaseline={rebaseline_flag})")
        _run_activation_routine(after_norm, activate_flag=True, rebaseline=rebaseline_flag)

    return {"status": "updated", "id": pid}

def _run_activation_routine(profile_obj: dict, activate_flag: bool, rebaseline: bool) -> None:
    """
    Aktiviert alle 'aktiven' Gruppen des Profils deterministisch:
      - overrides: forced_off=false, snooze_until=null
      - commands: enqueue rearm(+rebaseline)
    """
    pid = str(profile_obj.get("id"))
    ovr = _load_overrides()
    groups = profile_obj.get("condition_groups") or []
    changed = 0
    enq = 0

    for g in groups:
        gid = str(g.get("gid") or "")
        if not gid:
            continue
        if not bool(g.get("active", True)):
            print(f"[DEBUG] activation: skip group gid={gid} (inactive)")
            continue

        slot = _ensure_ovr_slot(ovr, pid, gid)
        # Setze nur, wenn sich was ändert → weniger Writes
        if slot.get("forced_off", False) or (slot.get("snooze_until") is not None):
            print(f"[DEBUG] activation: clear overrides pid={pid} gid={gid} (forced_off->False, snooze_until->None)")
        slot["forced_off"] = False
        slot["snooze_until"] = None
        changed += 1

        # enqueue command (idempotent genug auf Evaluator-Seite; hier nicht dedupen)
        _enqueue_command(pid, gid, rearm=True, rebaseline=rebaseline)
        enq += 1

    # Schreibe Overrides nur, wenn sich Slots berührt haben
    if changed > 0:
        _save_overrides(ovr)
        print(f"[DEBUG] activation: overrides saved (changed groups={changed})")
    print(f"[DEBUG] activation: commands enqueued={enq} (rebaseline={rebaseline}) for profile id={pid}")

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
# Endpunkte: REGISTRY
# ─────────────────────────────────────────────────────────────
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

@router.get("/indicators")
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
            st = _to_path(p).stat(); return {"exists": True, "size": st.st_size, "mtime": st.st_mtime}
        except FileNotFoundError:
            return {"exists": False, "size": 0, "mtime": None}
    return {
        "profiles": _stat(PROFILES_NOTIFIER),
        "alarms": _stat(ALARMS_NOTIFIER),
        "overrides": _stat(OVERRIDES_NOTIFIER),
        "commands": _stat(COMMANDS_NOTIFIER),
        "lock_dir": str(_LOCK_DIR)
    }

# ─────────────────────────────────────────────────────────────
# ALARMS: Modelle + Endpunkte (inkl. reason)
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

def _load_alarms() -> List[dict]:
    data = load_json(ALARMS_NOTIFIER, [])
    out = []
    for a in data or []:
        if not isinstance(a, dict):
            continue
        a.setdefault("id", _rand_id())
        a.setdefault("ts", "")
        a.setdefault("profile_id", "")
        a.setdefault("group_id", "")
        a.setdefault("symbol", "")
        a.setdefault("interval", "")
        a.setdefault("reason", "")
        a.setdefault("reason_code", "")
        a.setdefault("matched", [])
        a.setdefault("deactivate_applied", "")
        if a.get("deactivate_applied") not in {"", "true", "any_true"}:
            a["deactivate_applied"] = ""
        a.setdefault("meta", {})
        out.append(a)
    return out

def _save_alarms(items: List[dict]) -> None:
    save_json(ALARMS_NOTIFIER, items)

@router.get("/alarms", response_model=List[AlarmOut])
def list_alarms():
    items = _load_alarms()
    print(f"[DEBUG] GET /notifier/alarms -> {len(items)} items")
    return items

@router.post("/alarms", response_model=dict)
def add_alarm(a: AlarmIn):
    items = _load_alarms()
    payload = model_to_dict(a)
    aid = payload.get("id") or _rand_id()
    payload["id"] = aid

    # defensive Normalisierung
    norm = _normalize_deactivate_value(payload.get("deactivate_applied"))
    payload["deactivate_applied"] = norm if norm in {"", "true", "any_true"} else ""

    print(f"[DEBUG] POST /notifier/alarms <- {json.dumps(payload, ensure_ascii=False)[:600]}...")
    items.append(payload)
    _save_alarms(items)
    print(f"[DEBUG] POST /notifier/alarms -> saved id={aid} (total={len(items)})")
    return {"status": "ok", "id": aid}

@router.delete("/alarms/{aid}", response_model=dict)
def delete_alarm(aid: str):
    items = _load_alarms()
    before = len(items)
    items = [x for x in items if str(x.get("id")) != str(aid)]
    _save_alarms(items)
    after = len(items)
    print(f"[DEBUG] DELETE /notifier/alarms/{aid} -> {(before-after)} removed")
    return {"status": "deleted", "id": aid}

# ─────────────────────────────────────────────────────────────
# ✨ NEU: Overrides & Commands Endpunkte
# ─────────────────────────────────────────────────────────────
@router.get("/overrides", response_model=Dict[str, Any])
def get_overrides():
    ovr = _load_overrides()
    print(f"[DEBUG] GET /notifier/overrides -> profiles={len(ovr.get('overrides', {}))}")
    return ovr

class OverridePatch(ApiModel):
    forced_off: Optional[bool] = None
    snooze_until: Optional[Union[str, None]] = None
    note: Optional[str] = None

@router.patch("/overrides/{profile_id}/{group_id}", response_model=dict)
def patch_override(profile_id: str, group_id: str, body: OverridePatch):
    ovr = _load_overrides()
    slot = _ensure_ovr_slot(ovr, profile_id, group_id)
    payload = model_to_dict(body)
    print(f"[DEBUG] PATCH overrides <- pid={profile_id} gid={group_id} {payload}")

    if "forced_off" in payload and payload["forced_off"] is not None:
        slot["forced_off"] = bool(payload["forced_off"])
    if "snooze_until" in payload:
        slot["snooze_until"] = payload["snooze_until"]  # ISO8601 oder None (UI entscheidet)
    if "note" in payload and payload["note"] is not None:
        slot["note"] = str(payload["note"])

    _save_overrides(ovr)
    print(f"[DEBUG] PATCH overrides -> saved pid={profile_id} gid={group_id}")
    return {"status": "ok", "profile_id": profile_id, "group_id": group_id}

class CommandPost(ApiModel):
    rearm: Optional[bool] = True
    rebaseline: Optional[bool] = False

@router.post("/overrides/{profile_id}/{group_id}/commands", response_model=dict)
def post_command(profile_id: str, group_id: str, body: CommandPost):
    pb = model_to_dict(body)
    rearm = bool(pb.get("rearm", True))
    rebaseline = bool(pb.get("rebaseline", False))
    _enqueue_command(profile_id, group_id, rearm=rearm, rebaseline=rebaseline)
    return {"status": "ok", "profile_id": profile_id, "group_id": group_id, "rearm": rearm, "rebaseline": rebaseline}
