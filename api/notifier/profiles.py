# api/notifier/profiles.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import uuid
import hashlib

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, ValidationError

try:
    # pydantic v2
    from pydantic import ConfigDict, field_validator, model_validator
    _IS_PYD_V2 = True
except Exception:  # pragma: no cover
    ConfigDict = None  # type: ignore[assignment]
    field_validator = None  # type: ignore[assignment]
    model_validator = None  # type: ignore[assignment]
    _IS_PYD_V2 = False

from config import PROFILES_NOTIFIER
from storage import load_json, save_json_atomic, atomic_update_json_list

log = logging.getLogger("notifier.profiles")

DEBUG_PRINT = True


def _dbg(msg: str) -> None:
    if DEBUG_PRINT:
        print(msg)


# ─────────────────────────────────────────────────────────────
# Pydantic Base (v1/v2) – STRICT, no extra fields
# ─────────────────────────────────────────────────────────────

class ApiModel(BaseModel):
    """
    Strict base model:
    - extra fields are forbidden
    - we always dump including None values (exclude_none=False in dump helper)
    """
    if _IS_PYD_V2:
        model_config = ConfigDict(extra="forbid")
    else:
        class Config:
            extra = "forbid"


def model_dump_full(m: Any) -> Dict[str, Any]:
    """
    Dump model to dict while preserving explicit nulls.
    """
    if hasattr(m, "model_dump"):  # pyd v2
        return m.model_dump(exclude_none=False)
    if hasattr(m, "dict"):  # pyd v1
        return m.dict(exclude_none=False)
    return dict(m)  # type: ignore[arg-type]


# ─────────────────────────────────────────────────────────────
# New Schema Models (authoritative)
# ─────────────────────────────────────────────────────────────

Op = Literal["eq", "ne", "gt", "gte", "lt", "lte"]
Logic = Literal["and", "or"]
SingleMode = Literal["symbol", "group", "everything"]
DeactivateOn = Literal["always_on", "auto_off", "pre_notification"]


class Indicator(ApiModel):
    """
    Always present fields:
      {name, output, symbol|null, interval|null, params}
    Null means inheritance from group.
    """
    name: str
    output: str
    symbol: Optional[str] = None
    interval: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)

    if _IS_PYD_V2:
        @field_validator("name", "output")
        @classmethod
        def _non_empty_str(cls, v: str) -> str:
            s = str(v or "").strip()
            if not s:
                raise ValueError("Indicator.name/output must be non-empty")
            return s

        @field_validator("params")
        @classmethod
        def _params_is_dict(cls, v: Any) -> Dict[str, Any]:
            if v is None:
                return {}
            if not isinstance(v, dict):
                raise ValueError("Indicator.params must be a dict")
            return v
    else:
        # pydantic v1 validators
        @classmethod
        def __get_validators__(cls):
            yield from super().__get_validators__()


class Threshold(ApiModel):
    """
    threshold is either null OR:
      - type: "streak", params: {min_count}
      - type: "count",  params: {window, min_count}
    """
    type: Literal["streak", "count"]
    params: Dict[str, Any] = Field(default_factory=dict)

    if _IS_PYD_V2:
        @model_validator(mode="after")
        def _validate_threshold(self) -> "Threshold":
            t = self.type
            p = self.params or {}
            if not isinstance(p, dict):
                raise ValueError("Threshold.params must be a dict")

            def _to_int(x: Any) -> Optional[int]:
                if x is None:
                    return None
                try:
                    i = int(x)
                    return i
                except Exception:
                    return None

            if t == "streak":
                mc = _to_int(p.get("min_count"))
                if mc is None or mc <= 0:
                    raise ValueError("Threshold(streak) requires params.min_count > 0")
                # strict: only allowed key(s)
                allowed = {"min_count"}
                extra = set(p.keys()) - allowed
                if extra:
                    raise ValueError(f"Threshold(streak) has unknown params keys: {sorted(extra)}")

            if t == "count":
                w = _to_int(p.get("window"))
                mc = _to_int(p.get("min_count"))
                if w is None or w <= 0 or mc is None or mc <= 0:
                    raise ValueError("Threshold(count) requires params.window > 0 and params.min_count > 0")
                allowed = {"window", "min_count"}
                extra = set(p.keys()) - allowed
                if extra:
                    raise ValueError(f"Threshold(count) has unknown params keys: {sorted(extra)}")

            return self


class Condition(ApiModel):
    rid: str
    logic: Logic = "and"
    left: Indicator
    op: Op
    right: Indicator
    threshold: Optional[Threshold] = None

    if _IS_PYD_V2:
        @field_validator("rid")
        @classmethod
        def _rid_non_empty(cls, v: str) -> str:
            s = str(v or "").strip()
            if not s:
                raise ValueError("Condition.rid must be non-empty")
            return s


class Group(ApiModel):
    gid: str
    name: str = ""
    description: Optional[str] = None
    active: bool = True


    # Symbol source: reference + explicit list, both may be null
    symbol_group: Optional[str] = None
    symbols: Optional[List[str]] = None

    exchange: Optional[str] = None  # group-level only
    interval: str  # REQUIRED

    telegram_id: Optional[str] = None
    single_mode: SingleMode = "symbol"
    deactivate_on: DeactivateOn = "auto_off"

    conditions: List[Condition] = Field(default_factory=list)

    if _IS_PYD_V2:
        @field_validator("gid")
        @classmethod
        def _gid_non_empty(cls, v: str) -> str:
            s = str(v or "").strip()
            if not s:
                raise ValueError("Group.gid must be non-empty")
            return s

        @field_validator("interval")
        @classmethod
        def _interval_non_empty(cls, v: str) -> str:
            s = str(v or "").strip()
            if not s:
                raise ValueError("Group.interval is required and must be non-empty")
            return s

        @model_validator(mode="after")
        def _validate_group(self) -> "Group":
            # must have a symbol source
            if self.symbol_group is None and self.symbols is None:
                raise ValueError("Invalid group: both symbol_group and symbols are null")
            # must have at least 1 condition
            if not self.conditions:
                raise ValueError("Invalid group: conditions must not be empty")
            return self


class Profile(ApiModel):
    id: str
    name: str
    enabled: bool = True
    groups: List[Group] = Field(default_factory=list)

    if _IS_PYD_V2:
        @field_validator("id")
        @classmethod
        def _id_non_empty(cls, v: str) -> str:
            s = str(v or "").strip()
            if not s:
                raise ValueError("Profile.id must be non-empty")
            return s

        @field_validator("name")
        @classmethod
        def _name_non_empty(cls, v: str) -> str:
            s = str(v or "").strip()
            if not s:
                raise ValueError("Profile.name must be non-empty")
            return s

        @model_validator(mode="after")
        def _validate_profile(self) -> "Profile":
            if not self.groups:
                raise ValueError("Profile.groups must not be empty")
            return self


# ─────────────────────────────────────────────────────────────
# Load / Save (raw, exact) – NO migration, NO normalization
# ─────────────────────────────────────────────────────────────

def load_profiles_raw() -> list[dict]:
    items = load_json(PROFILES_NOTIFIER, [])
    if not isinstance(items, list):
        log.warning("load_profiles_raw: expected list, got %s → fallback []", type(items).__name__)
        items = []
    _dbg(f"[PROFILES] load_raw count={len(items)} path={PROFILES_NOTIFIER}")
    return items


def save_profiles_raw(items: list[dict]) -> None:
    save_json_atomic(PROFILES_NOTIFIER, items)
    log.info("save_profiles_raw: saved count=%d", len(items))
    _dbg(f"[PROFILES] save_raw count={len(items)} path={PROFILES_NOTIFIER}")


def _parse_profiles_strict(items: list[dict]) -> list[Profile]:
    """
    Strictly parse stored profiles.
    If storage contains legacy garbage, this will raise.
    That's intended: the system should fail loudly, not mutate schemas.
    """
    out: list[Profile] = []
    for i, raw in enumerate(items or []):
        if not isinstance(raw, dict):
            raise ValueError(f"Stored profile at index {i} is not an object")
        try:
            p = Profile(**raw)
        except ValidationError as e:
            _dbg(f"[PROFILES] parse_strict FAILED index={i} err={e}")
            raise
        out.append(p)
    return out


def list_profiles() -> list[dict]:
    raw = load_profiles_raw()
    parsed = _parse_profiles_strict(raw)
    out = [model_dump_full(p) for p in parsed]
    _dbg(f"[PROFILES] list_profiles count={len(out)}")
    return out


def get_profile_by_id(profile_id: str) -> Optional[dict]:
    pid = str(profile_id or "").strip()
    if not pid:
        return None
    raw = load_profiles_raw()
    for i, p in enumerate(raw):
        if isinstance(p, dict) and str(p.get("id") or "").strip() == pid:
            # strict-validate before returning
            obj = Profile(**p)
            out = model_dump_full(obj)
            _dbg(f"[PROFILES] get_profile_by_id HIT id={pid} idx={i}")
            return out
    _dbg(f"[PROFILES] get_profile_by_id MISS id={pid}")
    return None


def delete_profile_by_id(profile_id: str) -> dict:
    pid = str(profile_id or "").strip()
    if not pid:
        raise ValueError("delete_profile_by_id: profile_id darf nicht leer sein")

    _dbg(f"[PROFILES] delete_profile_by_id pid='{pid}'")

    def _transform(current: list):
        items = [p for p in (current or []) if isinstance(p, dict)]
        before = len(items)
        kept = [p for p in items if str(p.get("id") or "").strip() != pid]
        after = len(kept)
        deleted = (after != before)
        result = {
            "status": "deleted" if deleted else "not_found",
            "id": pid,
            "deleted": deleted,
            "before": before,
            "after": after,
        }
        return kept, result

    _, outcome = atomic_update_json_list(Path(PROFILES_NOTIFIER), _transform)
    _dbg(f"[PROFILES] delete_profile_by_id outcome={outcome}")
    return outcome


def create_profile(profile: dict) -> dict:
    """
    Create a new profile. If incoming profile has no id, we create one.
    Must be NEW schema.
    """
    incoming = deepcopy(profile or {})
    if not str(incoming.get("id") or "").strip():
        incoming["id"] = str(uuid.uuid4())

    # Strict parse
    obj = Profile(**incoming)
    payload = model_dump_full(obj)

    pid = payload["id"]
    _dbg(f"[PROFILES] create_profile id={pid} name={payload.get('name')!r}")

    def _transform(current: list):
        items = [p for p in (current or []) if isinstance(p, dict)]
        if any(str(p.get("id") or "").strip() == pid for p in items):
            raise ValueError(f"Profile id already exists: {pid}")
        items.append(payload)
        return items, {"status": "created", "id": pid, "created": True, "updated": False}

    _, outcome = atomic_update_json_list(Path(PROFILES_NOTIFIER), _transform)
    _dbg(f"[PROFILES] create_profile outcome={outcome}")
    return outcome


def update_profile_by_id(profile_id: str, profile: dict) -> dict:
    """
    Replace profile by ID (no merge, no normalization).
    Path ID and body ID must match.
    Must be NEW schema.
    """
    pid = str(profile_id or "").strip()
    if not pid:
        raise ValueError("update_profile_by_id: profile_id darf nicht leer sein")

    incoming = deepcopy(profile or {})
    body_id = str(incoming.get("id") or "").strip()
    if not body_id:
        raise ValueError("update_profile_by_id: body.id darf nicht leer sein")
    if body_id != pid:
        raise ValueError(f"update_profile_by_id: path id '{pid}' != body id '{body_id}'")

    obj = Profile(**incoming)
    payload = model_dump_full(obj)

    _dbg(f"[PROFILES] update_profile_by_id id={pid} name={payload.get('name')!r} groups={len(payload.get('groups') or [])}")

    def _transform(current: list):
        items = [p for p in (current or []) if isinstance(p, dict)]
        target_idx = None
        for idx, p in enumerate(items):
            if str(p.get("id") or "").strip() == pid:
                target_idx = idx
                break

        if target_idx is None:
            items.append(payload)
            result = {"status": "created", "id": pid, "created": True, "updated": False}
        else:
            items[target_idx] = payload
            result = {"status": "updated", "id": pid, "created": False, "updated": True}

        return items, result

    _, outcome = atomic_update_json_list(Path(PROFILES_NOTIFIER), _transform)
    _dbg(f"[PROFILES] update_profile_by_id outcome={outcome}")
    return outcome


def add_or_update_profile_by_name(profile: dict) -> dict:
    """
    Backwards-kompatible Signatur bleibt, ABER: es akzeptiert nur NEW schema.
    Upsert by name: if name exists, replace profile content (keeping the existing id).
    No merging, no normalization.
    """
    incoming = deepcopy(profile or {})
    name = str(incoming.get("name") or "").strip()
    if not name:
        raise ValueError("Profile braucht ein 'name'-Feld.")

    _dbg(f"[PROFILES] add_or_update_profile_by_name incoming_name='{name}'")

    def _transform(current: list):
        items = [p for p in (current or []) if isinstance(p, dict)]
        target_idx = None
        existing_id = None

        for idx, p in enumerate(items):
            pname = str(p.get("name") or "").strip()
            if pname.lower() == name.lower():
                target_idx = idx
                existing_id = str(p.get("id") or "").strip() or None
                break

        inc = deepcopy(incoming)
        if existing_id:
            inc["id"] = existing_id
        else:
            if not str(inc.get("id") or "").strip():
                inc["id"] = str(uuid.uuid4())

        obj = Profile(**inc)
        payload = model_dump_full(obj)

        if target_idx is None:
            items.append(payload)
            result = {"status": "created", "id": payload["id"], "created": True, "updated": False}
        else:
            items[target_idx] = payload
            result = {"status": "updated", "id": payload["id"], "created": False, "updated": True}

        return items, result

    _, outcome = atomic_update_json_list(Path(PROFILES_NOTIFIER), _transform)
    _dbg(f"[PROFILES] add_or_update_profile_by_name outcome={outcome}")
    return outcome

def profiles_fingerprint(items: list[dict]) -> str:
    """
    Stable fingerprint of the profiles list.
    - keeps ordering stable by sorting by profile.id
    - keeps explicit nulls
    - no normalization/migration, just deterministic hashing
    """
    safe_items: list[dict] = []
    for p in (items or []):
        if isinstance(p, dict):
            safe_items.append(p)

    # stable order
    safe_items.sort(key=lambda x: str(x.get("id") or ""))

    payload = json.dumps(
        safe_items,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
