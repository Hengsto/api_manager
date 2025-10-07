# api/notifier_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from fastapi import FastAPI, APIRouter, HTTPException, Query, Body, Path as FPath
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional, Union, Tuple, Set
from pathlib import Path
from copy import deepcopy
from fastapi.responses import JSONResponse

import json
import uuid
import os
import time
import tempfile
import hashlib
import random
import string
import unicodedata
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# Konfiguration (kompatibel zum Evaluator; Sibling-Dateien)
# ─────────────────────────────────────────────────────────────
try:
    from config import (
        PROFILES_NOTIFIER as _PROFILES_NOTIFIER_CFG,
        ALARMS_NOTIFIER   as _ALARMS_NOTIFIER_CFG,
    )
except Exception:
    _PROFILES_NOTIFIER_CFG = None
    _ALARMS_NOTIFIER_CFG   = None

# Optional: eigene Pfade (Fallbacks wenn nicht definiert)
try:
    from config import OVERRIDES_NOTIFIER as _OVERRIDES_NOTIFIER_CFG
except Exception:
    _OVERRIDES_NOTIFIER_CFG = None
try:
    from config import COMMANDS_NOTIFIER as _COMMANDS_NOTIFIER_CFG
except Exception:
    _COMMANDS_NOTIFIER_CFG = None
try:
    from config import STATUS_NOTIFIER as _STATUS_NOTIFIER_CFG
except Exception:
    _STATUS_NOTIFIER_CFG = None

# Für Registry (falls vorhanden)
try:
    from notifier.indicator_registry import REGISTERED, SIMPLE_SIGNALS
except Exception:
    REGISTERED = {}
    SIMPLE_SIGNALS = []

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("notifier.api")

# ─────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────
app = FastAPI(title="Notifier API", version="2.0.0")

_ALLOWED_ORIGINS = os.environ.get("NOTIFIER_CORS_ORIGINS", "").split(",") if os.environ.get("NOTIFIER_CORS_ORIGINS") else ["http://localhost:8050"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _ALLOWED_ORIGINS if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
router = APIRouter(prefix="/notifier")

# ─────────────────────────────────────────────────────────────
# Helpers: Pfade, Locks, JSON-IO
# ─────────────────────────────────────────────────────────────
def atomic_update_json_list(path: Path, transform_fn):
    """
    Atomisches Read→Transform→Write unter EINEM FileLock.
    transform_fn: (current_list: list) -> (new_list: list, result: Any)
    Gibt (new_list, result) zurück und speichert nur bei Änderung.
    """
    path = _to_path(path)
    with FileLock(path):
        current = []
        if path.exists():
            try:
                current = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(current, list):
                    current = []
            except Exception:
                current = []
        new_list, result = transform_fn(deepcopy(current))
        # Nur speichern, wenn tatsächlich geändert
        cur_bytes = json.dumps(current, sort_keys=True, ensure_ascii=False).encode("utf-8")
        new_bytes = json.dumps(new_list, sort_keys=True, ensure_ascii=False).encode("utf-8")
        if _sha256_bytes(cur_bytes) != _sha256_bytes(new_bytes):
            tmp = path.with_suffix(path.suffix + ".tmp")
            with open(tmp, "wb") as f:
                f.write(json.dumps(new_list, indent=2, ensure_ascii=False).encode("utf-8"))
                f.flush(); os.fsync(f.fileno())
            os.replace(tmp, path)
            try:
                if hasattr(os, "O_DIRECTORY"):
                    dfd = os.open(str(path.parent), os.O_DIRECTORY)
                    try: os.fsync(dfd)
                    finally: os.close(dfd)
            except Exception:
                pass
            log.info("JSON atomically saved: %s", path)
        else:
            log.debug("JSON atomic save skipped (no change): %s", path)
        return new_list, result


def _to_path(p: Any) -> Path:
    if isinstance(p, Path):
        return p.expanduser().resolve()
    return Path(str(p)).expanduser().resolve()

def _pick_base_dir() -> Path:
    for cfg in (_PROFILES_NOTIFIER_CFG, _ALARMS_NOTIFIER_CFG, _OVERRIDES_NOTIFIER_CFG, _COMMANDS_NOTIFIER_CFG, _STATUS_NOTIFIER_CFG):
        if cfg:
            try:
                return _to_path(cfg).parent
            except Exception:
                pass
    return Path.cwd()

_BASE = _pick_base_dir()
PROFILES_NOTIFIER: Path = _to_path(_PROFILES_NOTIFIER_CFG) if _PROFILES_NOTIFIER_CFG else (_BASE / "notifier_profiles.json")
ALARMS_NOTIFIER:   Path = _to_path(_ALARMS_NOTIFIER_CFG)   if _ALARMS_NOTIFIER_CFG   else (_BASE / "notifier_alarms.json")
OVERRIDES_NOTIFIER:Path = _to_path(_OVERRIDES_NOTIFIER_CFG)if _OVERRIDES_NOTIFIER_CFG else (_BASE / "notifier_overrides.json")
COMMANDS_NOTIFIER: Path = _to_path(_COMMANDS_NOTIFIER_CFG) if _COMMANDS_NOTIFIER_CFG else (_BASE / "notifier_commands.json")
STATUS_NOTIFIER:   Path = _to_path(_STATUS_NOTIFIER_CFG)   if _STATUS_NOTIFIER_CFG   else (_BASE / "notifier_status.json")

for p in (PROFILES_NOTIFIER, ALARMS_NOTIFIER, OVERRIDES_NOTIFIER, COMMANDS_NOTIFIER, STATUS_NOTIFIER):
    p.parent.mkdir(parents=True, exist_ok=True)

_ENV_LOCK_DIR = os.environ.get("NOTIFIER_LOCK_DIR", "").strip()
_LOCK_DIR = Path(_ENV_LOCK_DIR) if _ENV_LOCK_DIR else Path(tempfile.gettempdir()) / "notifier_locks"
_LOCK_DIR.mkdir(parents=True, exist_ok=True)

def _lock_path(path: Path) -> Path:
    try:
        name = Path(path).name
    except Exception:
        name = str(path)
    return _LOCK_DIR / (name + ".lock")

class FileLock:
    def __init__(self, path: Path, timeout: float = 10.0, poll: float = 0.1, stale_after: float = 300.0):
        self.lockfile = _lock_path(path)
        self.timeout = timeout
        self.poll = poll
        self.stale_after = stale_after
        self._acquired = False

    def _is_stale(self) -> bool:
        try:
            st = self.lockfile.stat()
            return (time.time() - st.st_mtime) > self.stale_after
        except FileNotFoundError:
            return False

    def acquire(self):
        start = time.time()
        while True:
            try:
                fd = os.open(str(self.lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                self._acquired = True
                log.debug("FileLock acquired: %s", self.lockfile)
                return
            except FileExistsError:
                if self._is_stale():
                    try:
                        os.unlink(self.lockfile)
                        log.warning("FileLock stale removed: %s", self.lockfile)
                    except FileNotFoundError:
                        pass
                    continue
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lockfile}")
                time.sleep(self.poll)

    def release(self):
        if self._acquired:
            try:
                os.unlink(self.lockfile)
                log.debug("FileLock released: %s", self.lockfile)
            except FileNotFoundError:
                pass
            finally:
                self._acquired = False

    def __enter__(self): self.acquire(); return self
    def __exit__(self, exc_type, exc, tb): self.release()

def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def load_json(path: Path, fallback: list) -> list:
    path = _to_path(path)
    if not path.exists():
        log.info("JSON load (missing -> fallback): %s", path)
        return fallback
    try:
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        log.info("JSON load: %s (items=%s)", path, len(data) if isinstance(data, list) else "n/a")
        return data if isinstance(data, list) else fallback
    except Exception as e:
        log.error("JSON load failed (%s): %s", path, e)
        return fallback

def save_json(path: Path, data: list):
    path = _to_path(path)
    payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    with FileLock(path):
        try:
            if path.exists():
                cur = path.read_bytes()
                if len(cur) == len(payload) and _sha256_bytes(cur) == _sha256_bytes(payload):
                    log.debug("JSON save skipped (no change): %s", path)
                    return
        except Exception:
            pass
        with open(tmp, "wb") as f:
            f.write(payload); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
        try:
            if hasattr(os, "O_DIRECTORY"):
                dfd = os.open(str(path.parent), os.O_DIRECTORY)
                try: os.fsync(dfd)
                finally: os.close(dfd)
        except Exception:
            pass
    log.info("JSON saved: %s (bytes=%d)", path, len(payload))

def load_json_any(path: Path, fallback: Any) -> Any:
    path = _to_path(path)
    if not path.exists():
        log.info("JSON-any load (missing -> fallback): %s", path)
        return deepcopy(fallback)
    try:
        txt = path.read_text(encoding="utf-8")
        data = json.loads(txt)
        log.info("JSON-any load: %s", path)
        return data
    except Exception as e:
        log.error("JSON-any load failed (%s): %s", path, e)
        return deepcopy(fallback)

def save_json_any(path: Path, data: Any):
    path = _to_path(path)
    payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    with FileLock(path):
        try:
            if path.exists():
                cur = path.read_bytes()
                if len(cur) == len(payload) and _sha256_bytes(cur) == _sha256_bytes(payload):
                    log.debug("JSON-any save skipped (no change): %s", path)
                    return
        except Exception:
            pass
        with open(tmp, "wb") as f:
            f.write(payload); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
        try:
            if hasattr(os, "O_DIRECTORY"):
                dfd = os.open(str(path.parent), os.O_DIRECTORY)
                try: os.fsync(dfd)
                finally: os.close(dfd)
        except Exception:
            pass
    log.info("JSON-any saved: %s (bytes=%d)", path, len(payload))

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")

# ─────────────────────────────────────────────────────────────
# Pydantic v1/v2 tolerant (extra='allow')
# ─────────────────────────────────────────────────────────────
try:
    from pydantic import ConfigDict
    _IS_PYD_V2 = True
except Exception:
    ConfigDict = None
    _IS_PYD_V2 = False

class ApiModel(BaseModel):
    if _IS_PYD_V2:
        model_config = ConfigDict(extra="allow")
    else:
        class Config:
            extra = "allow"

# ---- NEW: pydantic v1/v2 dict shim ------------------------------------------
def _model_to_dict(model: Any) -> Dict[str, Any]:
    """Return a dict from a pydantic model (v1: .dict, v2: .model_dump)."""
    if model is None:
        return {}
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    # Last resort – try to coerce
    try:
        return dict(model)  # type: ignore
    except Exception:
        return {}

# ─────────────────────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────────────────────
def _trim_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()

def _rand_id(n: int = 6) -> str:
    alphabet = "0123456789abcdef"
    return "".join(random.choice(alphabet) for _ in range(n))

def _name_key(x: Any) -> str:
    return _trim_str(x).lower()

def _contains_profile_token(x: Any) -> bool:
    return isinstance(x, str) and x.strip().lower().startswith("profile:")

import re

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_HEX_ID_RE = re.compile(r"^[0-9a-fA-F]{6,16}$")  # für kurze hex-IDs wie d4a5a0

def _looks_like_profile_id(x: Any) -> bool:
    if not isinstance(x, str):
        return False
    s = x.strip()
    if not s:
        return False
    if s.lower().startswith("profile:"):
        return True
    # UUID oder kurze hex-ID
    if _UUID_RE.match(s):
        return True
    if _HEX_ID_RE.match(s) and ":" not in s and "/" not in s:
        return True
    return False

def _extract_profile_id(token: str) -> str:
    s = str(token).strip()
    if s.lower().startswith("profile:"):
        return s.split(":", 1)[1].strip()
    return s

def _split_symbols_and_profiles(values: Any) -> tuple[list[str], list[str]]:
    syms: list[str] = []
    profs: list[str] = []
    if isinstance(values, list):
        for raw in values:
            if not isinstance(raw, str):
                continue
            if _looks_like_profile_id(raw):
                profs.append(_extract_profile_id(raw))
            else:
                syms.append(raw)
    return syms, profs



def _validate_group_strict(g: dict) -> None:
    # Nach unserem Mapping sollten hier keine Profile mehr liegen.
    bad = [s for s in (g.get("symbols") or []) if _contains_profile_token(s) or _looks_like_profile_id(s)]
    if bad:
        log.warning("Group symbols still contain profile-like tokens (will be ignored): %s", bad[:3])


def _validate_condition_strict(c: dict) -> None:
    # Nur noch warnen – _sanitize_condition mappt Profile → *_profiles
    ls = _trim_str(c.get("left_symbol"))
    rs = _trim_str(c.get("right_symbol"))
    if _contains_profile_token(ls) or _looks_like_profile_id(ls):
        log.warning("Condition.left_symbol contains profile-like token; will be moved by sanitizer: %s", ls)
    if _contains_profile_token(rs) or _looks_like_profile_id(rs):
        log.warning("Condition.right_symbol contains profile-like token; will be moved by sanitizer: %s", rs)



_ALLOWED_DEACT = {"always","true","any_true"}
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
    if not isinstance(p, dict):
        return {}
    out = dict(p)
    bp = {k[3:]: v for k, v in p.items() if isinstance(k, str) and k.startswith("bp.") and v not in (None, "")}
    if bp:
        nested = dict(p.get("base_params") or {})
        nested.update(bp)
        out["base_params"] = nested
    return out

_ALLOWED_SINGLE_MODES = {"symbol","group","everything"}

# ─────────────────────────────────────────────────────────────
# Datenmodelle (READ/WRITE)
# ─────────────────────────────────────────────────────────────
# --- GroupOut / GroupIn: profile_labels entfernen
class GroupOut(ApiModel):
    gid: str
    conditions: List[ConditionOut]
    active: bool
    symbols: List[str]  # nur echte Ticker
    profiles: List[str] = Field(default_factory=list)  # nur Profil-IDs
    interval: str = ""
    exchange: str = ""
    name: str = ""
    description: str = ""
    deactivate_on: Optional[Literal["always","true","any_true"]] = None
    min_true_ticks: Optional[int] = None
    single_mode: Optional[Literal["symbol","group","everything"]] = "symbol"

class ProfileBaseOut(ApiModel):
    name: str
    enabled: bool = True
    condition_groups: List[GroupOut]

class ProfileRead(ProfileBaseOut):
    id: str




class GroupIn(ApiModel):
    gid: Optional[str] = None
    conditions: List[ConditionIn] = Field(default_factory=list)
    active: bool = True
    symbols: List[str] = Field(default_factory=list)    # nur Ticker
    profiles: List[str] = Field(default_factory=list)   # nur Profil-IDs
    interval: str = ""
    exchange: str = ""
    name: str = ""
    description: str = ""
    deactivate_on: Optional[Literal["always","true","any_true"]] = None
    auto_deactivate: Optional[bool] = None
    min_true_ticks: Optional[int] = None
    single_mode: Optional[Literal["symbol","group","everything"]] = "symbol"


# --- ConditionOut / ConditionIn: neue Felder left_profiles/right_profiles
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
    left_symbol: str = ""
    left_interval: str = ""
    # NEU: Profile-Listen (IDs)
    left_profiles: List[str] = Field(default_factory=list)
    right_profiles: List[str] = Field(default_factory=list)


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
    left_symbol: Optional[str] = ""
    left_interval: Optional[str] = ""
    # NEU: Profile-Listen (IDs)
    left_profiles: List[str] = Field(default_factory=list)
    right_profiles: List[str] = Field(default_factory=list)



class GroupIn(ApiModel):
    gid: Optional[str] = None
    conditions: List[ConditionIn] = Field(default_factory=list)
    active: bool = True

    # Echte Symbole (Tickers etc.)
    symbols: List[str] = Field(default_factory=list)

    # NEU: Registry-Profile (IDs), die als Quelle dienen
    profiles: List[str] = Field(default_factory=list)

    # Optional nur für UI-Anzeige (nicht für Logik)
    profile_labels: List[str] = Field(default_factory=list)

    interval: str = ""
    exchange: str = ""
    name: str = ""
    description: str = ""
    deactivate_on: Optional[Literal["always","true","any_true"]] = None
    auto_deactivate: Optional[bool] = None
    min_true_ticks: Optional[int] = None
    single_mode: Optional[Literal["symbol","group","everything"]] = "symbol"


class ProfileBaseIn(ApiModel):
    name: str
    enabled: bool = True
    condition_groups: List[GroupIn] = Field(default_factory=list)

class ProfileCreate(ProfileBaseIn):
    id: Optional[str] = None

class ProfileUpdate(ProfileBaseIn):
    active: Optional[bool] = None
    activate: Optional[bool] = None
    rebaseline: Optional[bool] = None

class GroupActivePatch(ApiModel):
    active: bool



# ─────────────────────────────────────────────────────────────
# Sanitize: Condition (einzige gültige Definition, mit Debug-Logs)
# ─────────────────────────────────────────────────────────────
_ALLOWED_OPS = {"eq","ne","gt","gte","lt","lte"}
_ALLOWED_LOGIC = {"and","or"}

def _sanitize_condition(c: dict) -> dict:
    import re

    # lokale Regex (kein globaler Import nötig)
    _UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
    _HEX_ID_RE = re.compile(r"^[0-9a-fA-F]{6,16}$")

    def _looks_like_profile_id(x: object) -> bool:
        if not isinstance(x, str): return False
        s = x.strip()
        if not s: return False
        if s.lower().startswith("profile:"): return True
        if _UUID_RE.match(s): return True
        if _HEX_ID_RE.match(s) and ":" not in s and "/" not in s: return True
        return False

    def _extract_profile_id(token: str) -> str:
        s = str(token).strip()
        if s.lower().startswith("profile:"):
            return s.split(":", 1)[1].strip()
        return s

    # Defaults
    c = dict(c or {})
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
    c.setdefault("left_symbol", "")
    c.setdefault("left_interval", "")
    # Profile-Listen
    c.setdefault("left_profiles", [])
    c.setdefault("right_profiles", [])

    # DEBUG: Eingang
    try:
        print(f"[DEBUG] _sanitize_condition:init rid={c.get('rid')} "
              f"left='{c.get('left')}' right='{c.get('right')}' "
              f"lsym='{c.get('left_symbol')}' rsym='{c.get('right_symbol')}' "
              f"lop='{c.get('left_output')}' rop='{c.get('right_output')}'")
    except Exception:
        pass

    # Strings trimmen
    for k in ("left","right_symbol","right_interval","left_output",
              "right_output","logic","op","left_symbol","left_interval"):
        c[k] = _trim_str(c.get(k))

    # Op/Logic normalisieren
    if c["op"] not in _ALLOWED_OPS: c["op"] = "gt"
    if c["logic"] not in _ALLOWED_LOGIC: c["logic"] = "and"

    # Legacy/typo entfernen
    for k in ("right_absolute",):
        c.pop(k, None)

    # Profile-IDs in *_symbol → *_profiles mappen
    mapped = False
    if _looks_like_profile_id(c.get("left_symbol", "")):
        pid = _extract_profile_id(c.get("left_symbol", ""))
        if pid:
            c.setdefault("left_profiles", [])
            if pid not in c["left_profiles"]:
                c["left_profiles"].append(pid)
            c["left_symbol"] = ""
            mapped = True

    if _looks_like_profile_id(c.get("right_symbol", "")):
        pid = _extract_profile_id(c.get("right_symbol", ""))
        if pid:
            c.setdefault("right_profiles", [])
            if pid not in c["right_profiles"]:
                c["right_profiles"].append(pid)
            c["right_symbol"] = ""
            mapped = True

    if mapped:
        try:
            print(f"[DEBUG] _sanitize_condition:mapped rid={c.get('rid')} "
                  f"left_profiles={c.get('left_profiles')} right_profiles={c.get('right_profiles')}")
        except Exception:
            pass

    # right zu String
    rv = c.get("right")
    if isinstance(rv, (int, float)):
        c["right"] = str(rv)
    elif rv is None:
        c["right"] = ""
    else:
        c["right"] = _trim_str(rv)

    # Param-Dicts sichern
    if not isinstance(c["left_params"], dict):  c["left_params"]  = {}
    if not isinstance(c["right_params"], dict): c["right_params"] = {}

    # slope-Params normalisieren
    if (_trim_str(c.get("left")).lower() == "slope"):
        c["left_params"] = _normalize_slope_params_dict(c["left_params"])
    if (_trim_str(c.get("right")).lower() == "slope"):
        c["right_params"] = _normalize_slope_params_dict(c["right_params"])

    # Listen-Typ sichern
    if not isinstance(c["left_profiles"], list):  c["left_profiles"]  = []
    if not isinstance(c["right_profiles"], list): c["right_profiles"] = []

    # RID stabilisieren
    rid = _trim_str(c.get("rid")) or _rand_id()
    c["rid"] = rid

    # DEBUG: Ergebnis
    try:
        print(f"[DEBUG] _sanitize_condition:done rid={c.get('rid')} "
              f"lsym='{c.get('left_symbol')}' rsym='{c.get('right_symbol')}' "
              f"lprof={c.get('left_profiles')} rprof={c.get('right_profiles')}")
    except Exception:
        pass

    return c




def _norm_symbol(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s).strip()
    return s.upper()



def _sanitize_profiles(data: list) -> list:
    out = []
    for p in data or []:
        if not isinstance(p, dict):
            continue
        p.setdefault("name", "Unnamed")
        p.setdefault("enabled", True)
        p.setdefault("condition_groups", [])
        p["id"] = str(p.get("id") or uuid.uuid4())

        try:
            print(f"[DEBUG] _sanitize_profiles:init id={p['id']} name='{p.get('name')}' "
                  f"groups_in={len(p.get('condition_groups') or [])}")
        except Exception:
            pass

        groups = []
        for g in p.get("condition_groups") or []:
            if isinstance(g, dict):
                sg = _sanitize_group(g)
                groups.append(sg)

        seen = set()
        for g in groups:
            if g["gid"] in seen:
                g["gid"] = _rand_id()
            seen.add(g["gid"])

        p["condition_groups"] = groups

        try:
            syms_total = sum(len(gr.get("symbols") or []) for gr in groups)
            profs_total = sum(len(gr.get("profiles") or []) for gr in groups)
            print(f"[DEBUG] _sanitize_profiles:done id={p['id']} groups_out={len(groups)} "
                  f"sum_symbols={syms_total} sum_profiles={profs_total}")
        except Exception:
            pass

        out.append(p)
    return out


# ─────────────────────────────────────────────────────────────
# Sanitize: Group (ruft _sanitize_condition auf, mit Debug-Logs)
# ─────────────────────────────────────────────────────────────
def _sanitize_group(g: dict) -> dict:
    g = dict(g or {})

    # Defaults
    g.setdefault("gid", _rand_id())
    g.setdefault("conditions", [])
    g.setdefault("active", True)
    g.setdefault("symbols", [])
    g.setdefault("profiles", [])
    g.setdefault("profile_labels", [])
    g.setdefault("interval", "")
    g.setdefault("exchange", "")
    g.setdefault("name", "")
    g.setdefault("description", "")
    g.setdefault("deactivate_on", None)
    g.setdefault("auto_deactivate", None)
    g.setdefault("min_true_ticks", None)
    g.setdefault("single_mode", "symbol")

    # Trim einfache Strings
    for k in ("gid","interval","exchange","name","description","single_mode"):
        if k in g:
            g[k] = _trim_str(g.get(k))

    # Debug: Eingang
    try:
        print(f"[DEBUG] _sanitize_group:init gid={g.get('gid')} name='{g.get('name')}' "
              f"interval='{g.get('interval')}' exchange='{g.get('exchange')}' "
              f"symbols_in={len(g.get('symbols') or [])} profiles_in={len(g.get('profiles') or [])} "
              f"conds_in={len(g.get('conditions') or [])}")
    except Exception:
        pass

    # deactivate_on normalisieren (auto_deactivate als Legacy akzeptieren)
    deact = _normalize_deactivate_value(g.get("deactivate_on"))
    if deact is None and g.get("auto_deactivate") is not None:
        deact = "true" if bool(g.get("auto_deactivate")) else "always"
    g["deactivate_on"] = deact

    # single_mode absichern
    sm = _trim_str(g.get("single_mode")).lower()
    g["single_mode"] = sm if sm in _ALLOWED_SINGLE_MODES else "symbol"

    # Symbols/Profiles splitten (Profile-Tokens aus symbols rausziehen)
    syms_in = list(g.get("symbols") or [])
    profs_in = list(g.get("profiles") or [])

    # Falls in symbols Profile stecken → nach profiles verschieben
    split_syms, split_profs = _split_symbols_and_profiles(syms_in)
    # Profile-Einträge: sowohl vorhandene als auch aus symbols
    profs_all = list(profs_in) + split_profs

    # Nur echte Ticker in symbols, deduplizieren + normalisieren
    clean_syms = []
    seen_s = set()
    for s in split_syms:
        ns = _norm_symbol(s)
        if ns and ns not in seen_s:
            clean_syms.append(ns)
            seen_s.add(ns)

    # Profiles deduplizieren (IDs/Token wurden in _split_symbols_and_profiles schon extrahiert)
    clean_profs = []
    seen_p = set()
    for p in profs_all:
        pid = _trim_str(p)
        if pid and pid not in seen_p:
            clean_profs.append(pid)
            seen_p.add(pid)

    g["symbols"] = clean_syms
    g["profiles"] = clean_profs

    # Debug: nach Split
    try:
        print(f"[DEBUG] _sanitize_group:after-split gid={g.get('gid')} "
              f"symbols={g.get('symbols')} profiles={g.get('profiles')}")
    except Exception:
        pass

    # Conditions sanitisieren
    conds_out = []
    for raw in (g.get("conditions") or []):
        if isinstance(raw, dict):
            sc = _sanitize_condition(raw)
            conds_out.append(sc)
        else:
            # Ignoriere Nicht-Dicts still
            continue

    # Condition RIDs deduplizieren
    seen_rids = set()
    for c in conds_out:
        rid = _trim_str(c.get("rid")) or _rand_id()
        if rid in seen_rids:
            rid = _rand_id()
            c["rid"] = rid
        seen_rids.add(rid)
        # Optionale strikte Validierung nur für Logs/Warnungen
        try:
            _validate_condition_strict(c)
        except Exception:
            pass

    g["conditions"] = conds_out

    # Optionale Gruppen-Validierung (nur Warnungen)
    try:
        _validate_group_strict(g)
    except Exception:
        pass

    # GID stabilisieren
    g["gid"] = _trim_str(g.get("gid")) or _rand_id()

    return g


def _merge_ids(old_p: dict, new_p: dict) -> dict:
    """
    Stabilisiert IDs:
    - Gruppen werden primär per gid gematcht, sekundär per Name, erst dann per Index.
    - Conditions werden primär per rid gematcht; wenn rid fehlt, per (left, op, right, right_symbol, right_interval, left_output, right_output) Signatur.
    - Keine doppelten IDs; fehlende IDs werden neu erzeugt.
    """
    old_groups = old_p.get("condition_groups") or []
    new_groups = new_p.get("condition_groups") or []

    # Indexe bauen
    old_by_gid = {str(g.get("gid")): g for g in old_groups if str(g.get("gid") or "")}
    old_by_name = {}
    for g in old_groups:
        nk = _name_key(g.get("name"))
        if nk and nk not in old_by_name:
            old_by_name[nk] = g

    def _sign(c: dict) -> str:
        # robuste Condition-Signatur
        return json.dumps({
            "left": _trim_str(c.get("left")),
            "op": _trim_str(c.get("op")),
            "right": _trim_str(c.get("right")),
            "right_symbol": _trim_str(c.get("right_symbol")),
            "right_interval": _trim_str(c.get("right_interval")),
            "left_output": _trim_str(c.get("left_output")),
            "right_output": _trim_str(c.get("right_output")),
        }, sort_keys=True, ensure_ascii=False)

    # Gruppen durchgehen und passende alte Gruppe finden
    used_old_groups = set()
    for i, ng in enumerate(new_groups):
        gid = _trim_str(ng.get("gid"))
        match = None
        if gid and gid in old_by_gid:
            match = old_by_gid[gid]
        else:
            nk = _name_key(ng.get("name"))
            if nk and nk in old_by_name:
                match = old_by_name[nk]
        if match is None and i < len(old_groups):
            match = old_groups[i]  # nur als letzte Fallback-Heuristik
        # gid übernehmen/setzen
        if match and _trim_str(match.get("gid")):
            ng["gid"] = _trim_str(match.get("gid"))
        else:
            ng["gid"] = _trim_str(ng.get("gid")) or _rand_id()
        used_old_groups.add(id(match)) if match else None

        # Conditions mappen
        old_conds = (match.get("conditions") if match else []) or []
        old_by_rid = { _trim_str(c.get("rid")): c for c in old_conds if _trim_str(c.get("rid")) }
        old_by_sig = { _sign(c): c for c in old_conds }

        new_conds = ng.get("conditions") or []
        seen_rids = set()
        for nc in new_conds:
            rid = _trim_str(nc.get("rid"))
            if rid and rid in old_by_rid and rid not in seen_rids:
                # vorhandene RID behalten
                pass
            else:
                sig = _sign(nc)
                oc = old_by_sig.get(sig)
                if oc and _trim_str(oc.get("rid")) and _trim_str(oc.get("rid")) not in seen_rids:
                    nc["rid"] = _trim_str(oc.get("rid"))
                else:
                    # neue RID
                    nc["rid"] = _trim_str(nc.get("rid")) or _rand_id()
            if nc["rid"] in seen_rids:
                nc["rid"] = _rand_id()
            seen_rids.add(nc["rid"])

    # gruppenweit doppelte gids vermeiden
    seen_gids = set()
    for ng in new_groups:
        if ng["gid"] in seen_gids:
            ng["gid"] = _rand_id()
        seen_gids.add(ng["gid"])

    new_p["condition_groups"] = new_groups
    return new_p

# ---- NEW: resolve gid (id/index/name) ----------------------------------------
def _resolve_gid_from_profile(profile_obj: dict, gid_or_index: Any) -> Optional[str]:
    """
    Accepts:
      - exact gid (string)
      - integer index (0-based) given as int or numeric string
      - group name (case-insensitive)
    Returns real gid or None.
    """
    groups: List[dict] = list(profile_obj.get("condition_groups") or [])
    # 1) Direct gid hit
    for g in groups:
        gid = str(g.get("gid") or "").strip()
        if gid and str(gid_or_index).strip() == gid:
            return gid
    # 2) Index (0-based)
    try:
        idx = int(str(gid_or_index).strip())
        if 0 <= idx < len(groups):
            real = str(groups[idx].get("gid") or "").strip()
            return real or None
    except Exception:
        pass
    # 3) Name match (ci)
    key = _name_key(gid_or_index)
    if key:
        for g in groups:
            if _name_key(g.get("name")) == key:
                real = str(g.get("gid") or "").strip()
                return real or None
    return None

# ─────────────────────────────────────────────────────────────
# Overrides & Commands Helpers
# ─────────────────────────────────────────────────────────────
_OVR_TEMPLATE: Dict[str, Any] = {"overrides": {}, "updated_ts": None}
_CMD_TEMPLATE: Dict[str, Any] = {"queue": []}

def _load_overrides() -> Dict[str, Any]:
    d = load_json_any(OVERRIDES_NOTIFIER, deepcopy(_OVR_TEMPLATE))
    if not isinstance(d, dict) or "overrides" not in d:
        d = deepcopy(_OVR_TEMPLATE)
    return d

def _save_overrides(d: Dict[str, Any]) -> None:
    d = deepcopy(d)
    d["updated_ts"] = _now_iso()
    save_json_any(OVERRIDES_NOTIFIER, d)

def _ensure_ovr_slot(ovr: Dict[str, Any], pid: str, gid: str) -> Dict[str, Any]:
    ovr.setdefault("overrides", {})
    ovr["overrides"].setdefault(pid, {})
    ovr["overrides"][pid].setdefault(gid, {"forced_off": False, "snooze_until": None, "note": None})
    return ovr["overrides"][pid][gid]

def _load_commands() -> Dict[str, Any]:
    d = load_json_any(COMMANDS_NOTIFIER, deepcopy(_CMD_TEMPLATE))
    if not isinstance(d, dict) or "queue" not in d:
        d = deepcopy(_CMD_TEMPLATE)
    return d

def _save_commands(d: Dict[str, Any]) -> None:
    save_json_any(COMMANDS_NOTIFIER, d)

def _enqueue_command(pid: str, gid: str, rearm: bool = True, rebaseline: bool = False) -> Dict[str, Any]:
    cmds = _load_commands()
    item = {
        "profile_id": pid,
        "group_id": gid,
        "rearm": bool(rearm),
        "rebaseline": bool(rebaseline),
        "ts": _now_iso(),
        "id": _rand_id(8),
    }
    cmds["queue"].append(item)
    _save_commands(cmds)
    log.info("Command enqueued: %s", item)
    print(f"[CMD] enqueue pid={pid} gid={gid} rearm={rearm} rebaseline={rebaseline}")
    return item

# ─────────────────────────────────────────────────────────────
# Legacy → New Migration + Normalized Loader
# ─────────────────────────────────────────────────────────────
def _profile_to_legacy_alias(p: dict) -> dict:
    """Spiegelt condition_groups zusätzlich als legacy 'groups: [{config: ...}]' aus."""
    p = deepcopy(p)
    cgs = p.get("condition_groups") or []
    legacy_groups = []
    for g in cgs:
        deactivate_on = g.get("deactivate_on")
        auto_deactivate = None
        if deactivate_on in ("true", "any_true"):
            auto_deactivate = True
        cfg = deepcopy(g)
        cfg["auto_deactivate"] = auto_deactivate
        legacy_groups.append({"config": cfg})
    p["groups"] = legacy_groups
    return p


def _profiles_with_legacy_aliases(items: list[dict]) -> list[dict]:
    return [_profile_to_legacy_alias(x) for x in items]

def _migrate_legacy_groups_one_profile(p: dict) -> tuple[dict, bool]:
    if not isinstance(p, dict):
        return p, False

    changed = False
    cond_groups = list(p.get("condition_groups") or [])
    legacy = p.get("groups") or []

    if isinstance(legacy, list) and legacy:
        for g in legacy:
            cfg = (g or {}).get("config") or {}
            if not isinstance(cfg, dict):
                continue
            new_g = {
                "gid":          _trim_str(g.get("gid")) or _trim_str(cfg.get("gid")) or None,
                "name":         _trim_str(cfg.get("name")),
                "active":       bool(cfg.get("active", True)),
                "symbols":      list(cfg.get("symbols") or []),
                "profiles":     list(cfg.get("profiles") or []),  # NEU: übernehmen, falls vorhanden
                "interval":     _trim_str(cfg.get("interval")),
                "exchange":     _trim_str(cfg.get("exchange")),
                "telegram_bot_id": cfg.get("telegram_bot_id"),
                "telegram_bot_token": cfg.get("telegram_bot_token"),
                "telegram_chat_id": cfg.get("telegram_chat_id"),
                "description":  _trim_str(cfg.get("description")),
                "deactivate_on": _normalize_deactivate_value(cfg.get("deactivate_on")),
                "min_true_ticks": cfg.get("min_true_ticks"),
                "single_mode":  _trim_str(cfg.get("single_mode") or "symbol"),
                "conditions":   list(cfg.get("conditions") or []),
            }


            cond_groups.append(new_g)
        changed = True

    out = dict(p)
    if changed:
        out["condition_groups"] = cond_groups
    if "groups" in out:
        del out["groups"]
        changed = True or changed

    return out, changed

def _coerce_legacy_profiles(data: list) -> tuple[list, bool]:
    changed_any = False
    out = []
    for p in data or []:
        if not isinstance(p, dict):
            continue
        pp, ch = _migrate_legacy_groups_one_profile(p)
        out.append(pp)
        changed_any = changed_any or ch
    return out, changed_any

def _load_profiles_normalized() -> list[dict]:
    raw = load_json(PROFILES_NOTIFIER, [])
    migrated, changed = _coerce_legacy_profiles(raw)
    if changed:
        save_json(PROFILES_NOTIFIER, migrated)
        log.info("Profiles: legacy → condition_groups migriert & gespeichert (count=%d)", len(migrated))
        print(f"[PROFILES] migrated legacy→flat count={len(migrated)}")
    return _sanitize_profiles(migrated)

# ─────────────────────────────────────────────────────────────
# Status Snapshot (Skeleton + Merge)
# ─────────────────────────────────────────────────────────────
def _build_status_skeleton_from_profiles(profiles: list[dict]) -> dict:
    sanitized = _sanitize_profiles(profiles or [])
    profiles_map: dict[str, dict] = {}
    for p in sanitized:
        pid = str(p.get("id") or "")
        if not pid: continue
        gmap: dict[str, dict] = {}
        for g in (p.get("condition_groups") or []):
            gid = str(g.get("gid") or "")
            if not gid: continue
            gmap[gid] = {
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
                # NEU: für UI sichtbar
                "symbols": list(g.get("symbols") or []),
                "profiles": list(g.get("profiles") or []),
            }


        profiles_map[pid] = {
            "id": pid,
            "name": p.get("name") or pid,
            "profile_active": bool(p.get("enabled", True)),
            "groups": gmap,
        }
    return {
        "version": 1,
        "flavor": "notifier-api",
        "updated_ts": _now_iso(),
        "profiles": profiles_map,
    }


def _label_only_conditions(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in (group.get("conditions") or []):
        if not isinstance(c, dict): continue
        left  = (c.get("left") or "").strip() or "—"
        right = (c.get("right") or "").strip()
        if not right:
            rsym = (c.get("right_symbol") or "").strip()
            rinv = (c.get("right_interval") or "").strip()
            rout = (c.get("right_output") or "").strip()
            if rsym:
                parts = [rsym]
                if rinv: parts.append(f"@{rinv}")
                if rout: parts.append(f":{rout}")
                right = "".join(parts)
        right = right or "—"
        op = (c.get("op") or "gt").strip().lower()
        out.append({
            "left": left, "right": right,
            "left_spec": None, "right_spec": None,
            "left_output": None, "right_output": None,
            "left_col": None, "right_col": None,
            "op": op, "passed": False,
            "left_value": None, "right_value": None,
            "left_ts": None, "right_ts": None,
            "eval_ms": None, "error": None,
        })
    return out

def _load_status_any() -> Dict[str, Any]:
    d = load_json_any(STATUS_NOTIFIER, {"version": 1, "flavor": "notifier-api", "updated_ts": _now_iso(), "profiles": {}})
    if not isinstance(d, dict): d = {"version": 1, "flavor": "notifier-api", "updated_ts": _now_iso(), "profiles": {}}
    if "profiles" not in d or not isinstance(d["profiles"], dict): d["profiles"] = {}
    d.setdefault("profiles_fp", "")
    try:
        d["version"] = int(d.get("version", 1))
    except Exception:
        d["version"] = 1
    d.setdefault("flavor", "notifier-api")
    return d

def _save_status_any(data: Dict[str, Any]) -> None:
    data = deepcopy(data)
    data["updated_ts"] = _now_iso()
    try:
        data["version"] = int(data.get("version", 1))
    except Exception:
        data["version"] = 1
    data.setdefault("flavor", "notifier-api")
    save_json_any(STATUS_NOTIFIER, data)

def _profiles_fingerprint(profiles: list[dict]) -> str:
    try:
        normalized = _sanitize_profiles(deepcopy(profiles))
        payload = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
        return _sha256_bytes(payload.encode("utf-8"))
    except Exception:
        return ""

def _merge_status_keep_runtime(old: Dict[str, Any], skel: Dict[str, Any]) -> Dict[str, Any]:
    # defensive
    old_profiles = old.get("profiles") if isinstance(old.get("profiles"), dict) else {}
    skel_profiles = skel.get("profiles") if isinstance(skel.get("profiles"), dict) else {}

    new_out: Dict[str, Any] = {
        "version": int(old.get("version", 1)) if isinstance(old.get("version"), int) else 1,
        "flavor": "notifier-api",
        "profiles": {}
    }

    for pid, p_s in (skel_profiles or {}).items():
        old_p = old_profiles.get(pid) or {}
        new_p = {
            "id": p_s.get("id", pid),
            "name": p_s.get("name") or old_p.get("name") or pid,
            "profile_active": bool(p_s.get("profile_active", old_p.get("profile_active", True))),
            "groups": {}
        }

        old_groups = old_p.get("groups") if isinstance(old_p.get("groups"), dict) else {}
        skel_groups = p_s.get("groups") if isinstance(p_s.get("groups"), dict) else {}

        for gid, g_s in (skel_groups or {}).items():
            old_g   = old_groups.get(gid) or {}
            agg_old = old_g.get("aggregate") if isinstance(old_g.get("aggregate"), dict) else {}
            rt_old  = old_g.get("runtime")   if isinstance(old_g.get("runtime"), dict)   else {}

            agg_s   = g_s.get("aggregate")   if isinstance(g_s.get("aggregate"), dict)   else {}

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
                "effective_active": bool(g_s.get("effective_active", old_g.get("effective_active", True))),
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

                "conditions_status": old_g.get("conditions_status", []) if isinstance(old_g.get("conditions_status"), list) else [],
                "last_eval_ts": old_g.get("last_eval_ts", None),
                "last_bar_ts": old_g.get("last_bar_ts", None),
            }

            # ⬇️ NEU: Sichtbare Felder für die UI aus dem Skeleton übernehmen
            for _k in ("symbols", "profiles"):

                if _k in g_s:
                    new_g[_k] = list(g_s.get(_k) or [])

            # (Optional) weitere Felder
            if "min_tick" in g_s:
                new_g["min_tick"] = g_s.get("min_tick")
            if "single_mode" in g_s:
                new_g["single_mode"] = g_s.get("single_mode")

            new_p["groups"][gid] = new_g

        new_out["profiles"][pid] = new_p

    # ---- Diagnostics: was wurde gepruned? ----
    old_pids = set(old_profiles.keys())
    new_pids = set(new_out["profiles"].keys())
    pruned_pids = sorted(list(old_pids - new_pids))

    pruned_groups_total = 0
    details_groups: List[str] = []
    for pid in sorted(list(old_pids & new_pids)):
        old_gids = set((old_profiles.get(pid, {}) or {}).get("groups", {}).keys()) if isinstance((old_profiles.get(pid, {}) or {}).get("groups"), dict) else set()
        new_gids = set((new_out["profiles"].get(pid, {}) or {}).get("groups", {}).keys()) if isinstance((new_out["profiles"].get(pid, {}) or {}).get("groups"), dict) else set()
        gone = sorted(list(old_gids - new_gids))
        if gone:
            pruned_groups_total += len(gone)
            preview = ", ".join(gone[:5]) + ("..." if len(gone) > 5 else "")
            details_groups.append(f"{pid}: {preview}")

    log.info(
        "Status merged (pruned). profiles=%d pruned_profiles=%d pruned_groups=%d",
        len(new_out["profiles"]), len(pruned_pids), pruned_groups_total
    )
    print(f"[STATUS] merged pruned profiles={len(new_out['profiles'])}")
    if pruned_pids:
        print(f"[STATUS] pruned profile IDs: {pruned_pids[:5]}{'...' if len(pruned_pids)>5 else ''}")
    if pruned_groups_total:
        for line in details_groups[:10]:
            print(f"[STATUS] pruned groups -> {line}")
        if len(details_groups) > 10:
            print(f"[STATUS] pruned groups (more): {len(details_groups) - 10} pid-lines omitted")

    return new_out





def _status_autofix_merge() -> None:
    profiles = _load_profiles_normalized()
    skeleton = _build_status_skeleton_from_profiles(profiles)
    current  = _load_status_any()
    merged   = _merge_status_keep_runtime(current, skeleton)
    merged["profiles_fp"] = _profiles_fingerprint(profiles)
    _save_status_any(merged)
    log.info("Status auto-fix merge done. profiles_fp=%s", merged.get("profiles_fp","")[:8])
    print(f"[STATUS] autofix done fp={merged.get('profiles_fp','')[:8]}")

# ─────────────────────────────────────────────────────────────
# Profiles Endpoints (flaches Schema, mit Legacy-Migration)
# ─────────────────────────────────────────────────────────────
@router.get(
    "/profiles",
    response_model=List[ProfileRead],
    response_model_exclude_unset=False,
    response_model_exclude_none=False
)
def get_profiles(compat: Optional[str] = Query(default=None), legacy: Optional[int] = Query(default=None)):
    log.info("GET /profiles compat=%s legacy=%s", compat, legacy)
    sanitized = _load_profiles_normalized()
    if (compat or "").lower() in {"legacy", "1", "true"} or bool(legacy):
        items = _profiles_with_legacy_aliases(sanitized)
        # Wichtig: JSONResponse -> kein response_model-Filtering
        return JSONResponse(content=items)
    return sanitized

@router.get(
    "/profiles/{pid}",
    response_model=ProfileRead,
    response_model_exclude_unset=False,
    response_model_exclude_none=False
)
def get_profile(pid: str, compat: Optional[str] = Query(default=None), legacy: Optional[int] = Query(default=None)):
    log.info("GET /profiles/%s compat=%s legacy=%s", pid, compat, legacy)
    data = _load_profiles_normalized()
    for p in data:
        if str(p.get("id")) == str(pid):
            if (compat or "").lower() in {"legacy", "1", "true"} or bool(legacy):
                return _profile_to_legacy_alias(p)  # type: ignore[return-value]
            return p
    raise HTTPException(status_code=404, detail="Profil nicht gefunden")

@router.post("/profiles", response_model=dict)
def add_profile(p: ProfileCreate):
    incoming = deepcopy(_model_to_dict(p))

    # ID setzen + Gruppen sanitisieren + Ganz-Profil sanitisieren
    pid = incoming.get("id") or str(uuid.uuid4())
    incoming["id"] = pid
    incoming["condition_groups"] = [
        _sanitize_group(g) for g in (incoming.get("condition_groups") or []) if isinstance(g, dict)
    ]
    incoming = _sanitize_profiles([incoming])[0]
    target_name_key = _name_key(incoming.get("name"))

    def _transform(current: list):
        # aktuelle (evtl. legacy) Profile laden + sanitizen
        migrated, _ = _coerce_legacy_profiles(current)
        profs = _sanitize_profiles(migrated)
        idx = next((i for i, it in enumerate(profs) if _name_key((it or {}).get("name")) == target_name_key), None)
        if idx is not None:
            before_norm = profs[idx]
            merged = _merge_ids(before_norm, deepcopy(incoming))
            after_norm = _sanitize_profiles([merged])[0]
            after_norm["id"] = before_norm.get("id") or after_norm["id"]

            if json.dumps(before_norm, sort_keys=True, ensure_ascii=False) == json.dumps(after_norm, sort_keys=True, ensure_ascii=False):
                result = {"status": "ok", "id": after_norm["id"], "updated": False, "upserted_by_name": True}
                return profs, result
            profs[idx] = after_norm
            result = {"status": "updated", "id": after_norm["id"], "updated": True, "upserted_by_name": True}
            return profs, result

        profs.append(incoming)
        result = {"status": "ok", "id": pid, "created": True, "upserted_by_name": False}
        return profs, result

    new_list, outcome = atomic_update_json_list(PROFILES_NOTIFIER, _transform)
    _status_autofix_merge()
    print(f"[PROFILES] add/upsert outcome={outcome}")
    return outcome

@router.put("/profiles/{pid}", response_model=dict)
def update_profile(pid: str, p: ProfileUpdate):
    incoming = deepcopy(_model_to_dict(p))
    if "active" in incoming and "enabled" not in incoming:
        incoming["enabled"] = bool(incoming.get("active"))
    activate_flag   = bool(incoming.get("activate", False))
    rebaseline_flag = bool(incoming.get("rebaseline", False))
    incoming["condition_groups"] = [
        _sanitize_group(g) for g in (incoming.get("condition_groups") or []) if isinstance(g, dict)
    ]
    incoming["id"] = pid

    def _transform(current: list):
        migrated, _ = _coerce_legacy_profiles(current)
        profs = _sanitize_profiles(migrated)
        idx = next((i for i, it in enumerate(profs) if str(it.get("id")) == str(pid)), None)
        if idx is None:
            raise HTTPException(status_code=404, detail="Profil nicht gefunden")
        before_norm = profs[idx]
        before_enabled = bool(before_norm.get("enabled", True))
        merged     = _merge_ids(before_norm, deepcopy(incoming))
        after_norm = _sanitize_profiles([merged])[0]
        changed = json.dumps(before_norm, sort_keys=True, ensure_ascii=False) != json.dumps(after_norm, sort_keys=True, ensure_ascii=False)
        if changed:
            profs[idx] = after_norm
        return profs, {"changed": changed, "before_enabled": before_enabled, "after_enabled": bool(after_norm.get("enabled", True)), "profile": after_norm}

    _, res = atomic_update_json_list(PROFILES_NOTIFIER, _transform)
    _status_autofix_merge()
    if activate_flag or (not res["before_enabled"] and res["after_enabled"]):
        _run_activation_routine(res["profile"], activate_flag=True, rebaseline=rebaseline_flag)
    print(f"[PROFILES] put id={pid} changed={res['changed']} activate={activate_flag} rebaseline={rebaseline_flag}")
    return {"status": "updated" if res["changed"] else "ok", "id": pid}


def _set_group_active_in_profiles(profile_id: str, gid_or_index: Any, active: bool) -> Optional[str]:
    profs = _load_profiles_normalized()
    target: Optional[dict] = next((p for p in profs if str(p.get("id")) == str(profile_id)), None)
    if target is None:
        return None

    real_gid = _resolve_gid_from_profile(target, gid_or_index)
    if not real_gid:
        return None

    changed = False
    for g in (target.get("condition_groups") or []):
        if str(g.get("gid")) == str(real_gid):
            old = bool(g.get("active", True))
            g["active"] = bool(active)
            changed = (old != g["active"])
            break

    if changed:
        save_json(PROFILES_NOTIFIER, profs)
        _status_autofix_merge()
        log.info("Group active changed pid=%s gid=%s active=%s", profile_id, real_gid, active)
        print(f"[PROFILES] group-active pid={profile_id} gid={real_gid} active={active}")

    return real_gid if changed else None

@router.patch("/profiles/{pid}/groups/{gid}/active", response_model=dict)
def set_group_active(pid: str, gid: str, body: GroupActivePatch = Body(...)):
    if body is None or body.active is None:
        raise HTTPException(status_code=422, detail="Feld 'active' fehlt.")
    real_gid = _set_group_active_in_profiles(pid, gid, bool(body.active))
    if not real_gid:
        raise HTTPException(status_code=404, detail=f"Gruppe '{gid}' in Profil '{pid}' nicht gefunden oder unverändert")
    return {"status": "ok", "profile_id": pid, "group_id": real_gid, "active": bool(body.active)}

@router.delete("/profiles/{pid}", response_model=dict)
def delete_profile(pid: str):
    def _transform(current: list):
        migrated, _ = _coerce_legacy_profiles(current)
        profs = _sanitize_profiles(migrated)
        before = len(profs)
        profs = [p for p in profs if str(p.get("id")) != str(pid)]
        removed = before - len(profs)
        return profs, removed
    _, removed = atomic_update_json_list(PROFILES_NOTIFIER, _transform)
    _status_autofix_merge()
    log.info("DELETE /profiles/%s removed=%d", pid, removed)
    print(f"[PROFILES] delete id={pid} removed={removed}")
    return {"status": "deleted", "id": pid, "removed": removed}


def _run_activation_routine(profile_obj: dict, activate_flag: bool, rebaseline: bool) -> None:
    pid = str(profile_obj.get("id"))
    ovr = _load_overrides()
    groups = profile_obj.get("condition_groups") or []
    changed = 0; enq = 0
    for g in groups:
        gid = str(g.get("gid") or "")
        if not gid: continue
        if not bool(g.get("active", True)): continue
        slot = _ensure_ovr_slot(ovr, pid, gid)
        slot["forced_off"] = False
        slot["snooze_until"] = None
        changed += 1
        _enqueue_command(pid, gid, rearm=True, rebaseline=rebaseline)
        enq += 1
    if changed > 0: 
        _save_overrides(ovr)
    log.info("Activation routine pid=%s changed=%d enqueued=%d rebaseline=%s", pid, changed, enq, rebaseline)
    print(f"[ACTIVATE] pid={pid} changed={changed} enq={enq} rebaseline={rebaseline}")

# ─────────────────────────────────────────────────────────────
# Registry (optional)
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
        base_locked = list(s.get("locked_params", []))
        for p in (s.get("presets") or []):
            label = p.get("label")
            if not isinstance(label, str) or not label: continue
            preset_locked = list(p.get("locked_params", [])) if isinstance(p.get("locked_params"), (list, tuple)) else []
            items.append({
                "display_name": label,
                "base": s.get("name"),
                "params": deepcopy(p.get("params", {})),
                "locked_params": preset_locked or base_locked,
                "outputs": list(s.get("outputs", [])),
            })
    return items


@router.get("/registry/simple-signals", response_model=List[str])
def registry_simple_signals():
    return list(SIMPLE_SIGNALS or [])

# ─────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────
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
        "status": _stat(STATUS_NOTIFIER),
        "lock_dir": str(_LOCK_DIR),
        "ts": _now_iso(),
    }

# ─────────────────────────────────────────────────────────────
# Alarms
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
        if isinstance(a["matched"], str):
            try:
                parsed = json.loads(a["matched"])
                a["matched"] = parsed if isinstance(parsed, list) else []
            except Exception:
                a["matched"] = []
        a.setdefault("deactivate_applied", "")
        if a.get("deactivate_applied") not in {"", "true", "any_true"}:
            a["deactivate_applied"] = ""
        a.setdefault("meta", {})
        if isinstance(a["meta"], str):
            try:
                parsed_meta = json.loads(a["meta"])
                a["meta"] = parsed_meta if isinstance(parsed_meta, dict) else {}
            except Exception:
                a["meta"] = {}
        out.append(a)
    return out

def _save_alarms(items: List[dict]) -> None:
    save_json(ALARMS_NOTIFIER, items)

@router.get("/alarms", response_model=List[AlarmOut])
def list_alarms():
    items = _load_alarms()
    log.info("GET /alarms count=%d", len(items))
    print(f"[ALARMS] list count={len(items)}")
    return items

@router.get("/alarms/search", response_model=List[AlarmOut])
def search_alarms(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = None,
    group_id: Optional[str] = Query(None, alias="gid"),
    profile_id: Optional[str] = Query(None, alias="pid"),
    since: Optional[str] = None,
):
    from datetime import datetime, timezone

    def _parse_ts(s: str) -> Optional[float]:
        if not s: return None
        x = s.strip()
        # erlaubte Varianten: "YYYY-mm-dd HH:MM:SS[.ms]Z", "YYYY-mm-ddTHH:MM:SS[.ms]Z", ohne Z → UTC annehmen
        x = x.replace("T", " ").replace("z", "Z")
        if x.endswith("Z"):
            x = x[:-1]
        try:
            dt = datetime.fromisoformat(x)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.timestamp()
        except Exception:
            return None

    items = _load_alarms()

    if symbol:
        s = _norm_symbol(symbol)
        items = [a for a in items if _norm_symbol(a.get("symbol","")) == s]
    if group_id:
        g = str(group_id).strip()
        items = [a for a in items if str(a.get("group_id","")) == g]
    if profile_id:
        p = str(profile_id).strip()
        items = [a for a in items if str(a.get("profile_id","")) == p]
    if since:
        ts_min = _parse_ts(str(since))
        if ts_min is not None:
            filtered = []
            for a in items:
                ts = _parse_ts(str(a.get("ts","")))
                if ts is not None and ts >= ts_min:
                    filtered.append(a)
            items = filtered

    log.info("GET /alarms/search count=%d limit=%d offset=%d", len(items), limit, offset)
    print(f"[ALARMS] search result_count={len(items)} limit={limit} offset={offset}")
    return items[offset: offset+limit]


@router.delete("/alarms", response_model=dict)
def delete_alarms_older_than(older_than: str = Query(..., description="ISO UTC, löscht a.ts < older_than")):
    items = _load_alarms()
    keep = [a for a in items if str(a.get("ts","")) >= str(older_than)]
    removed = len(items) - len(keep)
    _save_alarms(keep)
    log.info("DELETE /alarms older_than=%s removed=%d", older_than, removed)
    print(f"[ALARMS] cleanup older_than={older_than} removed={removed}")
    return {"status": "ok", "removed": removed}

@router.post("/alarms", response_model=dict)
def add_alarm(a: AlarmIn):
    items = _load_alarms()
    payload = deepcopy(_model_to_dict(a))

    m = payload.get("matched", [])
    if isinstance(m, str):
        try:
            m2 = json.loads(m)
            payload["matched"] = m2 if isinstance(m2, list) else []
        except Exception:
            payload["matched"] = []
    elif not isinstance(m, list):
        payload["matched"] = []

    meta = payload.get("meta", {})
    if isinstance(meta, str):
        try:
            m3 = json.loads(meta)
            payload["meta"] = m3 if isinstance(m3, dict) else {}
        except Exception:
            payload["meta"] = {}
    elif not isinstance(meta, dict):
        payload["meta"] = {}

    aid = payload.get("id") or _rand_id()
    payload["id"] = aid

    norm = _normalize_deactivate_value(payload.get("deactivate_applied"))
    payload["deactivate_applied"] = norm if norm in {"", "true", "any_true"} else ""

    items.append(payload)
    _save_alarms(items)
    log.info("POST /alarms id=%s", aid)
    print(f"[ALARMS] add id={aid}")
    return {"status": "ok", "id": aid}

@router.delete("/alarms/{aid}", response_model=dict)
def delete_alarm(aid: str):
    items = _load_alarms()
    before = len(items)
    items = [x for x in items if str(x.get("id")) != str(aid)]
    _save_alarms(items)
    after = len(items)
    log.info("DELETE /alarms/%s removed=%d", aid, before-after)
    print(f"[ALARMS] delete id={aid} removed={before-after}")
    return {"status": "deleted", "id": aid, "removed": before-after}

# ─────────────────────────────────────────────────────────────
# Overrides & Commands Endpoints
# ─────────────────────────────────────────────────────────────
class OverridePatch(ApiModel):
    forced_off: Optional[bool] = None
    snooze_until: Optional[Union[str, None]] = None
    note: Optional[str] = None

@router.get("/overrides", response_model=Dict[str, Any])
def get_overrides():
    return _load_overrides()

@router.patch("/overrides/{profile_id}/{group_id}", response_model=dict)
def patch_override(profile_id: str, group_id: str, body: OverridePatch):
    ovr = _load_overrides()
    slot = _ensure_ovr_slot(ovr, profile_id, group_id)
    payload = deepcopy(_model_to_dict(body))

    changed = False
    if "forced_off" in payload and payload["forced_off"] is not None:
        slot["forced_off"] = bool(payload["forced_off"]); changed = True
    if "snooze_until" in payload:
        val = payload["snooze_until"]
        if val in (None, "", "null"):
            slot["snooze_until"] = None
        else:
            s = str(val).strip()
            try:
                dt = datetime.fromisoformat(s.replace("Z","").replace("z",""))
                slot["snooze_until"] = dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%fZ")
            except Exception:
                slot["snooze_until"] = s
                slot["note"] = ((slot.get("note") or "") + " (WARN: invalid snooze_until format)").strip()
        changed = True
    if "note" in payload and payload["note"] is not None:
        slot["note"] = str(payload["note"]); changed = True

    if changed:
        _save_overrides(ovr)

    try:
        if "forced_off" in payload and payload["forced_off"] is True:
            _set_group_active_in_profiles(profile_id, group_id, False)
    except Exception:
        pass

    log.info("PATCH /overrides/%s/%s changed=%s", profile_id, group_id, changed)
    print(f"[OVR] patch pid={profile_id} gid={group_id} changed={changed}")
    return {"status": "ok", "profile_id": profile_id, "group_id": group_id, "override": slot}

class CommandPost(ApiModel):
    rearm: Optional[bool] = True
    rebaseline: Optional[bool] = False

@router.get("/commands")
def get_commands():
    return _load_commands()

@router.post("/overrides/{profile_id}/{group_id}/commands", response_model=dict)
def post_command(profile_id: str, group_id: str, body: CommandPost):
    pb = deepcopy(_model_to_dict(body))
    rearm = bool(pb.get("rearm", True))
    rebaseline = bool(pb.get("rebaseline", False))
    item = _enqueue_command(profile_id, group_id, rearm=rearm, rebaseline=rebaseline)
    return {"status": "ok", "enqueued": item}

# ─────────────────────────────────────────────────────────────
# Status: Sync & Get (Auto-Fix)
# ─────────────────────────────────────────────────────────────
@router.post("/status/sync", response_model=dict)
def status_sync(body: Dict[str, Any] = Body(default=None)):
    incoming = body or {}
    profiles = incoming.get("profiles")
    if not isinstance(profiles, list):
        profiles = _load_profiles_normalized()
    skeleton = _build_status_skeleton_from_profiles(profiles)
    current = _load_status_any()
    merged = _merge_status_keep_runtime(current, skeleton)
    merged["profiles_fp"] = _profiles_fingerprint(profiles)
    _save_status_any(merged)
    log.info("POST /status/sync profiles=%d", len(merged.get("profiles", {})))
    print(f"[STATUS] sync profiles={len(merged.get('profiles', {}))}")
    return {"status": "ok", "profiles": len(merged.get("profiles", {}))}

@router.get("/status")
def get_status(force: Optional[int] = Query(default=0, description="1 = Snapshot aus Profilen neu aufbauen & mergen")):
    snap = _load_status_any()
    try:
        need_fix = bool(force)
        profiles = _load_profiles_normalized()
        skeleton = _build_status_skeleton_from_profiles(profiles)
        fp = _profiles_fingerprint(profiles)

        if not need_fix:
            if not snap.get("profiles"):
                need_fix = True
            else:
                if snap.get("profiles_fp", "") != fp:
                    need_fix = True
                else:
                    for pid, p_s in (skeleton.get("profiles") or {}).items():
                        sp = (snap.get("profiles") or {}).get(pid, {})
                        s_groups = (sp.get("groups") or {})
                        for gid in (p_s.get("groups") or {}).keys():
                            if gid not in s_groups:
                                need_fix = True
                                break
                        if need_fix: break

        if need_fix:
            merged = _merge_status_keep_runtime(snap, skeleton)
            merged["profiles_fp"] = fp
            _save_status_any(merged)
            log.info("GET /status (fixed) profiles=%d", len(merged.get("profiles", {})))
            print(f"[STATUS] get fixed profiles={len(merged.get('profiles', {}))}")
            return merged
        log.info("GET /status ok (no fix)")
        return snap
    except Exception as e:
        log.exception("GET /status failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────
# Zusätzliche sinnvolle Endpoints
# ─────────────────────────────────────────────────────────────
@router.post("/profiles/{pid}/activate", response_model=dict)
def activate_profile(pid: str, rebaseline: bool = Query(default=False)):
    data = _load_profiles_normalized()
    prof = next((p for p in data if str(p.get("id")) == str(pid)), None)
    if not prof:
        raise HTTPException(status_code=404, detail="Profil nicht gefunden")
    _run_activation_routine(prof, activate_flag=True, rebaseline=rebaseline)
    _status_autofix_merge()
    log.info("POST /profiles/%s/activate rebaseline=%s", pid, rebaseline)
    print(f"[PROFILES] activate id={pid} rebaseline={rebaseline}")
    return {"status": "ok", "id": pid, "rebaseline": bool(rebaseline)}

@router.get("/profiles:lookup", response_model=Optional[ProfileRead])
def lookup_profile(name: str = Query(...)):
    key = _name_key(name)
    data = _load_profiles_normalized()
    for p in data:
        if _name_key(p.get("name")) == key:
            return p
    return None

# ─────────────────────────────────────────────────────────────
# Router mounten & Root
# ─────────────────────────────────────────────────────────────
app.include_router(router)

@app.get("/")
def root():
    return {
        "service": "notifier-api",
        "version": "2.0.0",
        "docs": "/docs",
        "endpoints": [
            "/notifier/health",
            "/notifier/profiles  (GET, POST upsert-by-name)",
            "/notifier/profiles/{pid}  (GET, PUT, DELETE)",
            "/notifier/profiles/{pid}/groups/{gid}/active  (PATCH)",
            "/notifier/profiles/{pid}/activate  (POST)",
            "/notifier/profiles:lookup?name=  (GET)",
            "/notifier/overrides  (GET)",
            "/notifier/overrides/{pid}/{gid}  (PATCH)",
            "/notifier/overrides/{pid}/{gid}/commands  (POST)",
            "/notifier/commands  (GET)",
            "/notifier/alarms  (GET, POST)",
            "/notifier/alarms/search  (GET with filters/pagination)",
            "/notifier/alarms/{aid}  (DELETE)",
            "/notifier/alarms?older_than=...  (DELETE cleanup)",
            "/notifier/status  (GET)",
            "/notifier/status/sync  (POST)",
            "/notifier/registry/indicators  (GET)",
            "/notifier/registry/simple-signals  (GET)",
            "/notifier/indicators  (GET)",
        ],
        "files": {
            "profiles": str(PROFILES_NOTIFIER),
            "status":   str(STATUS_NOTIFIER),
            "overrides":str(OVERRIDES_NOTIFIER),
            "commands": str(COMMANDS_NOTIFIER),
            "alarms":   str(ALARMS_NOTIFIER),
        }
    }
