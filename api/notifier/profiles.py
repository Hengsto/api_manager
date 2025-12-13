# api/notifier/profiles.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import random
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple
from pathlib import Path
from copy import deepcopy

from pydantic import BaseModel, Field

try:
    # pydantic v2
    from pydantic import ConfigDict
    _IS_PYD_V2 = True
except Exception:  # pragma: no cover
    ConfigDict = None  # type: ignore[assignment]
    _IS_PYD_V2 = False

from config import PROFILES_NOTIFIER
from storage import load_json, save_json_atomic, atomic_update_json_list

log = logging.getLogger("notifier.profiles")

# ─────────────────────────────────────────────────────────────
# Pydantic-Base (v1/v2 kompatibel)
# ─────────────────────────────────────────────────────────────

class ApiModel(BaseModel):
    if _IS_PYD_V2:
        model_config = ConfigDict(extra="allow")
    else:
        class Config:
            extra = "allow"


def model_to_dict(model: Any) -> Dict[str, Any]:
    """
    Gibt ein dict aus einem Pydantic-Modell zurück (v1: .dict, v2: .model_dump).
    """
    if model is None:
        return {}
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    try:
        return dict(model)  # type: ignore[arg-type]
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


def _norm_symbol(s: Any) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKC", s).strip()
    return s.upper()


def _contains_profile_token(x: Any) -> bool:
    return isinstance(x, str) and x.strip().lower().startswith("profile:")


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
    if isinstance(values, (list, tuple)):
        for raw in values:
            if not isinstance(raw, str):
                continue
            if _looks_like_profile_id(raw):
                profs.append(_extract_profile_id(raw))
            else:
                syms.append(raw)
    return syms, profs


_ALLOWED_DEACT = {"always", "true", "any_true"}
_ALLOWED_THRESHOLDS = {"check", "min_tick"}
_ALLOWED_SINGLE_MODES = {"symbol", "group", "everything"}
_ALLOWED_OPS = {"eq", "ne", "gt", "gte", "lt", "lte"}
_ALLOWED_LOGIC = {"and", "or"}


def _normalize_deactivate_value(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else None
    s = _trim_str(v).lower()
    if not s:
        return None
    if s == "always":
        return "always"
    if s in {"true", "full", "match"}:
        return "true"
    if s in {"any_true", "any", "partial"}:
        return "any_true"
    return None


def _normalize_slope_params_dict(p: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(p, dict):
        return {}
    out = dict(p)
    bp = {
        k[3:]: v
        for k, v in p.items()
        if isinstance(k, str) and k.startswith("bp.") and v not in (None, "")
    }
    if bp:
        nested = dict(p.get("base_params") or {})
        nested.update(bp)
        out["base_params"] = nested
    return out


# ─────────────────────────────────────────────────────────────
# Datenmodelle
# ─────────────────────────────────────────────────────────────

class ConditionOut(ApiModel):
    rid: str
    left: str
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte"]
    right: str = ""
    right_symbol: str = ""
    right_interval: str = ""
    left_output: str = ""
    right_output: str = ""
    logic: Literal["and", "or"] = "and"
    left_params: Dict[str, Any] = Field(default_factory=dict)
    right_params: Dict[str, Any] = Field(default_factory=dict)
    left_symbol: str = ""
    left_interval: str = ""
    # Profile-Listen (IDs)
    left_profiles: List[str] = Field(default_factory=list)
    right_profiles: List[str] = Field(default_factory=list)
    # Threshold-Konzept (check|min_tick|empty)
    threshold: str = ""
    threshold_params: Dict[str, Any] = Field(default_factory=dict)


class ConditionIn(ApiModel):
    rid: Optional[str] = None
    left: Optional[str] = ""
    op: Optional[Literal["eq", "ne", "gt", "gte", "lt", "lte"]] = "gt"
    right: Any = ""
    right_symbol: Optional[str] = ""
    right_interval: Optional[str] = ""
    left_output: Optional[str] = ""
    right_output: Optional[str] = ""
    logic: Optional[Literal["and", "or"]] = "and"
    left_params: Dict[str, Any] = Field(default_factory=dict)
    right_params: Dict[str, Any] = Field(default_factory=dict)
    left_symbol: Optional[str] = ""
    left_interval: Optional[str] = ""
    # Profile-Listen (IDs)
    left_profiles: List[str] = Field(default_factory=list)
    right_profiles: List[str] = Field(default_factory=list)
    # Threshold-Konzept (UI: Check / Min.Tick)
    threshold: Optional[str] = ""
    threshold_params: Dict[str, Any] = Field(default_factory=dict)


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
    deactivate_on: Optional[Literal["always", "true", "any_true"]] = None
    min_true_ticks: Optional[int] = None
    single_mode: Optional[Literal["symbol", "group", "everything"]] = "symbol"


class GroupIn(ApiModel):
    gid: Optional[str] = None
    conditions: List[ConditionIn] = Field(default_factory=list)
    active: bool = True

    # Echte Symbole (Tickers etc.)
    symbols: List[str] = Field(default_factory=list)

    # Registry-Profile (IDs), die als Quelle dienen
    profiles: List[str] = Field(default_factory=list)

    # Optional nur für UI-Anzeige (nicht für Logik)
    profile_labels: List[str] = Field(default_factory=list)

    interval: str = ""
    exchange: str = ""
    name: str = ""
    description: str = ""
    deactivate_on: Optional[Literal["always", "true", "any_true"]] = None
    auto_deactivate: Optional[bool] = None
    min_true_ticks: Optional[int] = None
    single_mode: Optional[Literal["symbol", "group", "everything"]] = "symbol"


class ProfileBaseOut(ApiModel):
    name: str
    enabled: bool = True
    condition_groups: List[GroupOut]


class ProfileRead(ProfileBaseOut):
    id: str


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
# Validierung / Sanitize
# ─────────────────────────────────────────────────────────────

def _validate_group_strict(g: dict) -> None:
    # Nach unserem Mapping sollten hier keine Profile mehr liegen.
    bad = [
        s
        for s in (g.get("symbols") or [])
        if _contains_profile_token(s) or _looks_like_profile_id(s)
    ]
    if bad:
        log.warning(
            "Group symbols still contain profile-like tokens (will be ignored): %s",
            bad[:3],
        )


def _validate_condition_strict(c: dict) -> None:
    ls = _trim_str(c.get("left_symbol"))
    rs = _trim_str(c.get("right_symbol"))
    if _contains_profile_token(ls) or _looks_like_profile_id(ls):
        log.warning(
            "Condition.left_symbol contains profile-like token; will be moved by sanitizer: %s",
            ls,
        )
    if _contains_profile_token(rs) or _looks_like_profile_id(rs):
        log.warning(
            "Condition.right_symbol contains profile-like token; will be moved by sanitizer: %s",
            rs,
        )


def _sanitize_condition(c: dict) -> dict:
    """
    Normalisiert eine Condition:
    - Füllt Defaults
    - trimmt Strings
    - mappt Profile-Tokens in left_profiles/right_profiles
    - normalisiert Threshold / threshold_params
    - normalisiert slope-Parameter
    """
    # lokale Regex, um Import-Kreise zu vermeiden
    _uuid_re = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    _hex_id_re = re.compile(r"^[0-9a-fA-F]{6,16}$")

    def _looks_like_pid(x: object) -> bool:
        if not isinstance(x, str):
            return False
        s = x.strip()
        if not s:
            return False
        if s.lower().startswith("profile:"):
            return True
        if _uuid_re.match(s):
            return True
        if _hex_id_re.match(s) and ":" not in s and "/" not in s:
            return True
        return False

    def _extract_pid(token: str) -> str:
        s = str(token).strip()
        if s.lower().startswith("profile:"):
            return s.split(":", 1)[1].strip()
        return s

    def _canonical_threshold(raw_value: Any, container: dict) -> str:
        """
        Normalisiert Threshold:
        - akzeptiert Schreibweisen wie 'check', 'Check', 'min tick', 'Min-Tick', 'MIN_TICK'
        - mappt diverse Legacy-Felder (threshold_window/window/min_ticks/min_tick)
        """
        s = _trim_str(raw_value)
        if s:
            s_norm = s.lower().replace(" ", "_").replace("-", "_")
        else:
            s_norm = ""

        if not s_norm:
            has_min = any(
                (k in container) and str(container.get(k)).strip() != ""
                for k in ("threshold_min_tick", "min_tick")
            )
            has_window = any(
                (k in container) and str(container.get(k)).strip() != ""
                for k in ("threshold_window", "window", "min_ticks")
            )
            if has_min:
                s_norm = "min_tick"
            elif has_window:
                s_norm = "check"

        if not s_norm:
            return ""
        if s_norm not in _ALLOWED_THRESHOLDS:
            return ""
        return s_norm

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
    c.setdefault("left_profiles", [])
    c.setdefault("right_profiles", [])
    c.setdefault("threshold", "")
    c.setdefault("threshold_params", {})

    try:
        print(
            f"[DEBUG] _sanitize_condition:init rid={c.get('rid')} "
            f"left='{c.get('left')}' right='{c.get('right')}' "
            f"lsym='{c.get('left_symbol')}' rsym='{c.get('right_symbol')}' "
            f"lop='{c.get('left_output')}' rop='{c.get('right_output')}' "
            f"threshold_raw={c.get('threshold')} "
            f"threshold_params_raw={c.get('threshold_params')}"
        )
    except Exception:
        pass

    for k in (
        "left",
        "right_symbol",
        "right_interval",
        "left_output",
        "right_output",
        "logic",
        "op",
        "left_symbol",
        "left_interval",
        "threshold",
    ):
        c[k] = _trim_str(c.get(k))

    if c["op"] not in _ALLOWED_OPS:
        c["op"] = "gt"
    if c["logic"] not in _ALLOWED_LOGIC:
        c["logic"] = "and"

    # Legacy-Feld rauswerfen
    for k in ("right_absolute",):
        c.pop(k, None)

    thr = _canonical_threshold(c.get("threshold", ""), c)
    c["threshold"] = thr

    tp_raw = c.get("threshold_params", {})

    if isinstance(tp_raw, (int, float, str)):
        v_str = str(tp_raw).strip()
        if v_str == "":
            tp = {}
        else:
            tp = {"value": tp_raw}
    elif isinstance(tp_raw, dict):
        tp_clean: Dict[str, Any] = {
            k: v for k, v in tp_raw.items() if v not in (None, "")
        }
        if "value" in tp_clean:
            tp = {"value": tp_clean["value"]}
        else:
            legacy_val = None
            for legacy_key in (
                "value",
                "window",
                "min_ticks",
                "min_tick",
                "threshold_window",
            ):
                if legacy_key in tp_clean and str(tp_clean[legacy_key]).strip() != "":
                    legacy_val = tp_clean[legacy_key]
                    break
            tp = {"value": legacy_val} if legacy_val is not None else {}
    else:
        tp = {}

    if not tp:
        for k in ("threshold_window", "window", "min_ticks", "min_tick"):
            if k in c and str(c.get(k)).strip() != "":
                tp = {"value": c.get(k)}
                break

    c["threshold_params"] = tp

    try:
        print(
            f"[DEBUG] _sanitize_condition:threshold rid={c.get('rid')} "
            f"thr='{c.get('threshold')}' "
            f"thr_params={c.get('threshold_params')}"
        )
    except Exception:
        pass

    mapped = False
    if _looks_like_pid(c.get("left_symbol", "")):
        pid = _extract_pid(c.get("left_symbol", ""))
        if pid:
            c.setdefault("left_profiles", [])
            if pid not in c["left_profiles"]:
                c["left_profiles"].append(pid)
            c["left_symbol"] = ""
            mapped = True

    if _looks_like_pid(c.get("right_symbol", "")):
        pid = _extract_pid(c.get("right_symbol", ""))
        if pid:
            c.setdefault("right_profiles", [])
            if pid not in c["right_profiles"]:
                c["right_profiles"].append(pid)
            c["right_symbol"] = ""
            mapped = True

    if mapped:
        try:
            print(
                f"[DEBUG] _sanitize_condition:mapped rid={c.get('rid')} "
                f"left_profiles={c.get('left_profiles')} "
                f"right_profiles={c.get('right_profiles')}"
            )
        except Exception:
            pass

    rv = c.get("right")
    if isinstance(rv, (int, float)):
        c["right"] = str(rv)
    elif rv is None:
        c["right"] = ""
    else:
        c["right"] = _trim_str(rv)

    if not isinstance(c["left_params"], dict):
        c["left_params"] = {}
    if not isinstance(c["right_params"], dict):
        c["right_params"] = {}

    if _trim_str(c.get("left")).lower() == "slope":
        c["left_params"] = _normalize_slope_params_dict(c["left_params"])
    if _trim_str(c.get("right")).lower() == "slope":
        c["right_params"] = _normalize_slope_params_dict(c["right_params"])

    if not isinstance(c["left_profiles"], list):
        c["left_profiles"] = []
    if not isinstance(c["right_profiles"], list):
        c["right_profiles"] = []

    rid = _trim_str(c.get("rid")) or _rand_id()
    c["rid"] = rid

    try:
        print(
            f"[DEBUG] _sanitize_condition:done rid={c.get('rid')} "
            f"lsym='{c.get('left_symbol')}' rsym='{c.get('right_symbol')}' "
            f"lprof={c.get('left_profiles')} rprof={c.get('right_profiles')} "
            f"thr='{c.get('threshold')}' thr_params={c.get('threshold_params')}"
        )
    except Exception:
        pass

    return c


def _sanitize_group(g: dict) -> dict:
    g = dict(g or {})

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

    for k in ("gid", "interval", "exchange", "name", "description", "single_mode"):
        if k in g:
            g[k] = _trim_str(g.get(k))

    try:
        print(
            f"[DEBUG] _sanitize_group:init gid={g.get('gid')} name='{g.get('name')}' "
            f"interval='{g.get('interval')}' exchange='{g.get('exchange')}' "
            f"symbols_in={len(g.get('symbols') or [])} "
            f"profiles_in={len(g.get('profiles') or [])} "
            f"conds_in={len(g.get('conditions') or [])}"
        )
    except Exception:
        pass

    deact = _normalize_deactivate_value(g.get("deactivate_on"))
    if deact is None and g.get("auto_deactivate") is not None:
        deact = "true" if bool(g.get("auto_deactivate")) else "always"
    g["deactivate_on"] = deact

    sm = _trim_str(g.get("single_mode")).lower()
    g["single_mode"] = sm if sm in _ALLOWED_SINGLE_MODES else "symbol"

    syms_in = list(g.get("symbols") or [])
    profs_in = list(g.get("profiles") or [])

    # Split beide Listen, weil Clients/UI manchmal Müll in beide Felder legen
    split_syms_1, split_profs_1 = _split_symbols_and_profiles(syms_in)
    split_syms_2, split_profs_2 = _split_symbols_and_profiles(profs_in)

    # Symbole dürfen NUR in symbols landen, Profile-IDs NUR in profiles
    split_syms = split_syms_1 + split_syms_2
    profs_all = split_profs_1 + split_profs_2

    clean_syms: List[str] = []
    seen_s = set()
    for s in split_syms:
        ns = _norm_symbol(s)
        if ns and ns not in seen_s:
            clean_syms.append(ns)
            seen_s.add(ns)

    clean_profs: List[str] = []
    seen_p = set()
    for p in profs_all:
        pid = _trim_str(p)
        if pid and pid not in seen_p:
            clean_profs.append(pid)
            seen_p.add(pid)

    g["symbols"] = clean_syms
    g["profiles"] = clean_profs

    try:
        print(
            f"[DEBUG] _sanitize_group:after-split gid={g.get('gid')} "
            f"symbols={g.get('symbols')} profiles={g.get('profiles')}"
        )
    except Exception:
        pass

    conds_out = []
    for raw in (g.get("conditions") or []):
        if isinstance(raw, dict):
            sc = _sanitize_condition(raw)
            conds_out.append(sc)

    seen_rids = set()
    for c in conds_out:
        rid = _trim_str(c.get("rid")) or _rand_id()
        if rid in seen_rids:
            rid = _rand_id()
            c["rid"] = rid
        seen_rids.add(rid)
        try:
            _validate_condition_strict(c)
        except Exception:
            pass

    g["conditions"] = conds_out

    try:
        _validate_group_strict(g)
    except Exception:
        pass

    g["gid"] = _trim_str(g.get("gid")) or _rand_id()
    return g


def _sanitize_profiles(data: list) -> list:
    out = []
    seen_profile_ids = set()

    try:
        print(f"[DEBUG] _sanitize_profiles:START items_in={len(data or [])}")
    except Exception:
        pass

    for idx, p in enumerate(data or []):
        if not isinstance(p, dict):
            try:
                print(f"[WARN] _sanitize_profiles:skip idx={idx} type={type(p).__name__}")
            except Exception:
                pass
            continue

        # wichtig: nicht das Input-Objekt mutieren (macht Debug/Save/Migration stabiler)
        p = dict(p)

        # Snapshot before mutate
        try:
            keys_preview = list(p.keys())[:25]
            print(
                f"[DEBUG] _sanitize_profiles:RAW idx={idx} keys={keys_preview} "
                f"has_id={'id' in p} raw_id={repr(p.get('id'))} raw_name={repr(p.get('name'))}"
            )
        except Exception:
            pass

        p.setdefault("name", "Unnamed")
        p.setdefault("enabled", True)
        p.setdefault("condition_groups", [])

        pid = str(p.get("id") or "").strip()
        if not pid:
            try:
                print(
                    f"[ERROR] _sanitize_profiles:NO_ID idx={idx} "
                    f"name={repr(p.get('name'))} keys={list(p.keys())}"
                )
            except Exception:
                pass
            raise ValueError(
                "[SANITIZE] Profile ohne ID entdeckt – das darf nicht passieren. "
                "IDs müssen beim Erstellen vergeben und danach stabil bleiben."
            )

        # Detect duplicate IDs in same batch (should not happen)
        if pid in seen_profile_ids:
            try:
                print(f"[ERROR] _sanitize_profiles:DUPLICATE_ID idx={idx} id={pid}")
            except Exception:
                pass
        seen_profile_ids.add(pid)

        p["id"] = pid

        try:
            print(
                f"[DEBUG] _sanitize_profiles:init idx={idx} id={p['id']} "
                f"name='{p.get('name')}' enabled={p.get('enabled')} "
                f"groups_in={len(p.get('condition_groups') or [])}"
            )
        except Exception:
            pass

        groups = []
        conds_total_in = 0
        for gi, g in enumerate(p.get("condition_groups") or []):
            if not isinstance(g, dict):
                try:
                    print(f"[WARN] _sanitize_profiles:skip_group idx={idx} gi={gi} type={type(g).__name__}")
                except Exception:
                    pass
                continue

            try:
                conds_total_in += len(g.get("conditions") or [])
            except Exception:
                pass

            sg = _sanitize_group(g)
            groups.append(sg)

        # Ensure unique gids within this profile
        seen_gids = set()
        for g in groups:
            gid = str(g.get("gid") or "").strip()
            if gid in seen_gids:
                new_gid = _rand_id()
                try:
                    print(f"[WARN] _sanitize_profiles:duplicate_gid id={pid} gid={gid} -> {new_gid}")
                except Exception:
                    pass
                g["gid"] = new_gid
            seen_gids.add(str(g.get("gid") or "").strip())

        p["condition_groups"] = groups

        try:
            syms_total = sum(len(gr.get("symbols") or []) for gr in groups)
            profs_total = sum(len(gr.get("profiles") or []) for gr in groups)
            conds_total_out = sum(len(gr.get("conditions") or []) for gr in groups)
            gids = [str(gr.get("gid") or "") for gr in groups][:6]

            print(
                f"[DEBUG] _sanitize_profiles:done idx={idx} id={p['id']} "
                f"groups_out={len(groups)} gids_preview={gids} "
                f"conds_in≈{conds_total_in} conds_out={conds_total_out} "
                f"sum_symbols={syms_total} sum_profiles={profs_total}"
            )
        except Exception:
            pass

        out.append(p)

    try:
        print(f"[DEBUG] _sanitize_profiles:END items_out={len(out)}")
    except Exception:
        pass

    return out



# ─────────────────────────────────────────────────────────────
# Legacy-Migration
# ─────────────────────────────────────────────────────────────

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
                "gid": _trim_str(g.get("gid")) or _trim_str(cfg.get("gid")) or None,
                "name": _trim_str(cfg.get("name")),
                "active": bool(cfg.get("active", True)),
                "symbols": list(cfg.get("symbols") or []),
                "profiles": list(cfg.get("profiles") or []),
                "interval": _trim_str(cfg.get("interval")),
                "exchange": _trim_str(cfg.get("exchange")),
                "telegram_bot_id": cfg.get("telegram_bot_id"),
                "telegram_bot_token": cfg.get("telegram_bot_token"),
                "telegram_chat_id": cfg.get("telegram_chat_id"),
                "description": _trim_str(cfg.get("description")),
                "deactivate_on": _normalize_deactivate_value(cfg.get("deactivate_on")),
                "min_true_ticks": cfg.get("min_true_ticks"),
                "single_mode": _trim_str(cfg.get("single_mode") or "symbol"),
                "conditions": list(cfg.get("conditions") or []),
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


def coerce_legacy_profiles(data: list) -> tuple[list, bool]:
    """
    Public: migriert ggf. legacy 'groups' → 'condition_groups'.
    """
    changed_any = False
    out = []
    for p in data or []:
        if not isinstance(p, dict):
            continue
        pp, ch = _migrate_legacy_groups_one_profile(p)
        out.append(pp)
        changed_any = changed_any or ch
    return out, changed_any


def _profile_to_legacy_alias(p: dict) -> dict:
    """
    Spiegelt condition_groups zusätzlich als legacy 'groups: [{config: ...}]' aus.
    Für API-Kompatibilität (compat=legacy).
    """
    p = dict(p)
    cgs = p.get("condition_groups") or []
    legacy_groups = []
    for g in cgs:
        deactivate_on = g.get("deactivate_on")
        auto_deactivate = None
        if deactivate_on in ("true", "any_true"):
            auto_deactivate = True
        cfg = dict(g)
        cfg["auto_deactivate"] = auto_deactivate
        legacy_groups.append({"config": cfg})
    p["groups"] = legacy_groups
    return p


def profiles_with_legacy_aliases(items: list[dict]) -> list[dict]:
    """
    Public: gibt Profile inkl. 'groups'-Alias zurück (für alte Clients).
    """
    return [_profile_to_legacy_alias(x) for x in items]


# ─────────────────────────────────────────────────────────────
# Laden / Speichern / Fingerprint
# ─────────────────────────────────────────────────────────────

def load_profiles_raw() -> list[dict]:
    """
    Lädt die Roh-Profile (ohne Sanitize/Migration).
    """
    items = load_json(PROFILES_NOTIFIER, [])
    if not isinstance(items, list):
        log.warning(
            "load_profiles_raw: expected list, got %s → fallback []",
            type(items).__name__,
        )
        items = []
    try:
        print(f"[PROFILES] load_raw count={len(items)} path={PROFILES_NOTIFIER}")
    except Exception:
        pass
    return items


def save_profiles_raw(items: list[dict]) -> None:
    """
    Speichert eine Liste von Profil-Dicts roh (ohne extra Sanitize).
    """
    save_json_atomic(PROFILES_NOTIFIER, items)
    log.info("save_profiles_raw: saved count=%d", len(items))
    try:
        print(f"[PROFILES] save_raw count={len(items)} path={PROFILES_NOTIFIER}")
    except Exception:
        pass


def load_profiles_normalized() -> list[dict]:
    """
    Lädt die Profile, migriert Legacy-Felder und sanitisert das Schema.
    """
    raw = load_profiles_raw()
    migrated, changed = coerce_legacy_profiles(raw)
    if changed:
        save_profiles_raw(migrated)
        log.info(
            "Profiles: legacy → condition_groups migriert & gespeichert (count=%d)",
            len(migrated),
        )
        try:
            print(f"[PROFILES] migrated legacy→flat count={len(migrated)}")
        except Exception:
            pass
    sanitized = _sanitize_profiles(migrated)
    try:
        print(f"[PROFILES] load_normalized count={len(sanitized)}")
    except Exception:
        pass
    return sanitized


def profiles_fingerprint(profiles: list[dict]) -> str:
    """
    Erzeugt einen stabilen Fingerprint aus den Profilen (für Status-Merge).
    """
    try:
        normalized = _sanitize_profiles(json.loads(json.dumps(profiles)))
        payload = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
        import hashlib

        h = hashlib.sha256()
        h.update(payload.encode("utf-8"))
        fp = h.hexdigest()
        try:
            print(f"[PROFILES] fingerprint fp={fp[:16]}... len={len(normalized)}")
        except Exception:
            pass
        return fp
    except Exception as e:
        log.error("profiles_fingerprint failed: %s", e)
        try:
            print(f"[PROFILES] fingerprint ERROR: {e}")
        except Exception:
            pass
        return ""


# ─────────────────────────────────────────────────────────────
# ID-Merge / GID-Resolver
# ─────────────────────────────────────────────────────────────

def merge_ids(old_p: dict, new_p: dict) -> dict:
    """
    Stabilisiert IDs:
    - Gruppen werden primär per gid gematcht, sekundär per Name, erst dann per Index.
    - Conditions werden primär per rid gematcht; wenn rid fehlt, per Signatur.
    - Keine doppelten IDs; fehlende IDs werden neu erzeugt.
    """
    old_groups = old_p.get("condition_groups") or []
    new_groups = new_p.get("condition_groups") or []

    old_by_gid = {str(g.get("gid")): g for g in old_groups if str(g.get("gid") or "")}
    old_by_name: Dict[str, dict] = {}
    for g in old_groups:
        nk = _name_key(g.get("name"))
        if nk and nk not in old_by_name:
            old_by_name[nk] = g

    def _sign(c: dict) -> str:
        return json.dumps(
            {
                "left": _trim_str(c.get("left")),
                "op": _trim_str(c.get("op")),
                "right": _trim_str(c.get("right")),
                "right_symbol": _trim_str(c.get("right_symbol")),
                "right_interval": _trim_str(c.get("right_interval")),
                "left_output": _trim_str(c.get("left_output")),
                "right_output": _trim_str(c.get("right_output")),
            },
            sort_keys=True,
            ensure_ascii=False,
        )

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
            match = old_groups[i]

        if match and _trim_str(match.get("gid")):
            ng["gid"] = _trim_str(match.get("gid"))
        else:
            ng["gid"] = _trim_str(ng.get("gid")) or _rand_id()
        if match:
            used_old_groups.add(id(match))

        old_conds = (match.get("conditions") if match else []) or []
        old_by_rid = {
            _trim_str(c.get("rid")): c for c in old_conds if _trim_str(c.get("rid"))
        }
        old_by_sig = {_sign(c): c for c in old_conds}

        new_conds = ng.get("conditions") or []
        seen_rids = set()
        for nc in new_conds:
            rid = _trim_str(nc.get("rid"))
            if rid and rid in old_by_rid and rid not in seen_rids:
                # existierende RID behalten
                pass
            else:
                sig = _sign(nc)
                oc = old_by_sig.get(sig)
                if oc and _trim_str(oc.get("rid")) and _trim_str(oc.get("rid")) not in seen_rids:
                    nc["rid"] = _trim_str(oc.get("rid"))
                else:
                    nc["rid"] = _trim_str(nc.get("rid")) or _rand_id()
            if nc["rid"] in seen_rids:
                nc["rid"] = _rand_id()
            seen_rids.add(nc["rid"])

    seen_gids = set()
    for ng in new_groups:
        if ng["gid"] in seen_gids:
            ng["gid"] = _rand_id()
        seen_gids.add(ng["gid"])

    new_p["condition_groups"] = new_groups
    try:
        print(
            f"[PROFILES] merge_ids done id={old_p.get('id') or new_p.get('id')} "
            f"groups={len(new_groups)}"
        )
    except Exception:
        pass
    return new_p


def resolve_gid_from_profile(profile_obj: dict, gid_or_index: Any) -> Optional[str]:
    """
    Accepts:
      - exact gid (string)
      - integer index (0-based) given as int or numeric string
      - group name (case-insensitive)
    Returns real gid or None.
    """
    groups: List[dict] = list(profile_obj.get("condition_groups") or [])

    # exakte GID
    for g in groups:
        gid = str(g.get("gid") or "").strip()
        if gid and str(gid_or_index).strip() == gid:
            return gid

    # Index
    try:
        idx = int(str(gid_or_index).strip())
        if 0 <= idx < len(groups):
            real = str(groups[idx].get("gid") or "").strip()
            return real or None
    except Exception:
        pass

    # Name
    key = _name_key(gid_or_index)
    if key:
        for g in groups:
            if _name_key(g.get("name")) == key:
                real = str(g.get("gid") or "").strip()
                return real or None
    return None


# ─────────────────────────────────────────────────────────────
# High-Level Profile-API
# ─────────────────────────────────────────────────────────────

def list_profiles() -> list[dict]:
    """
    Bequemer Wrapper: alle Profile normalisiert laden.
    """
    profiles = load_profiles_normalized()
    return profiles


def get_profile_by_id(profile_id: str) -> Optional[dict]:
    """
    Liefert ein einzelnes Profil (normalisiert) nach ID.
    """
    pid = str(profile_id or "").strip()
    if not pid:
        return None
    profiles = load_profiles_normalized()
    for p in profiles:
        if str(p.get("id") or "").strip() == pid:
            try:
                print(f"[PROFILES] get_profile_by_id hit id={pid}")
            except Exception:
                pass
            return p
    try:
        print(f"[PROFILES] get_profile_by_id MISS id={pid}")
    except Exception:
        pass
    return None

def delete_profile_by_id(profile_id: str) -> dict:
    """
    Löscht ein Profil per ID.
    Rückgabe:
      {
        "status": "deleted",
        "id": "<profile-id>",
        "deleted": bool,
      }
    """
    pid = str(profile_id or "").strip()
    if not pid:
        raise ValueError("delete_profile_by_id: profile_id darf nicht leer sein")

    print(f"[PROFILES] delete_profile_by_id pid='{pid}'")

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
    print(f"[PROFILES] delete_profile_by_id outcome={outcome}")
    return outcome


def update_profile_by_id(profile_id: str, profile: dict) -> dict:
    """
    Upsert über eine feste Profil-ID.

    - Wenn ID existiert → Eintrag wird ersetzt.
    - Wenn ID nicht existiert → neues Profil wird angelegt mit genau dieser ID.

    Rückgabe:
      {
        "status": "created" | "updated",
        "id": "<profile-id>",
        "created": bool,
        "updated": bool,
      }
    """
    pid = str(profile_id or "").strip()
    if not pid:
        raise ValueError("update_profile_by_id: profile_id darf nicht leer sein")

    incoming = deepcopy(profile or {})
    if not str(incoming.get("id") or "").strip():
        raise ValueError("[WRITE] update_profile_by_id mit leerer ID aufgerufen")

    print(f"[PROFILES] update_profile_by_id incoming_id='{pid}'")

    def _transform(current: list):
        items = [p for p in (current or []) if isinstance(p, dict)]
        target_idx = None

        for idx, p in enumerate(items):
            if str(p.get("id") or "").strip() == pid:
                target_idx = idx
                break

        if target_idx is None:
            # neu anlegen
            items.append(incoming)
            result = {
                "status": "created",
                "id": pid,
                "created": True,
                "updated": False,
            }
        else:
            items[target_idx] = incoming
            result = {
                "status": "updated",
                "id": pid,
                "created": False,
                "updated": True,
            }

        return items, result

    _, outcome = atomic_update_json_list(Path(PROFILES_NOTIFIER), _transform)
    print(f"[PROFILES] update_profile_by_id outcome={outcome}")
    return outcome


# --- Backwards-kompatible Helper-Funktion für main.py & Group-Manager ------

def add_or_update_profile_by_name(profile: dict) -> dict:
    """
    Backwards-kompatibel:
    - Upsert nach Profil-Name (case-insensitive).
    - Falls Name schon existiert → Eintrag ersetzen, ID beibehalten.
    - Falls Name neu ist        → neues Profil mit neuer UUID anlegen.

    Rückgabe:
      {
        "status": "created" | "updated",
        "id": "<profile-id>",
        "created": bool,
        "updated": bool,
      }
    """
    incoming = deepcopy(profile or {})
    name = str(incoming.get("name") or "").strip()
    if not name:
        raise ValueError("Profile braucht ein 'name'-Feld.")

    print(f"[PROFILES] add_or_update_profile_by_name incoming_name='{name}'")

    def _transform(current: list):
        items = [p for p in (current or []) if isinstance(p, dict)]
        target_idx = None
        existing_id = None

        # existierendes Profil über Namen suchen (case-insensitive)
        for idx, p in enumerate(items):
            pname = str(p.get("name") or "").strip()
            if pname.lower() == name.lower():
                target_idx = idx
                existing_id = str(p.get("id") or "").strip() or None
                break

        # ID setzen
        if existing_id:
            incoming["id"] = existing_id
        else:
            incoming["id"] = str(uuid.uuid4())

        if not str(incoming.get("id") or "").strip():
            raise ValueError("[WRITE] add_or_update_profile_by_name erzeugt Profil ohne ID")


        # Upsert
        if target_idx is None:
            items.append(incoming)
            result = {
                "status": "created",
                "id": incoming["id"],
                "created": True,
                "updated": False,
            }
        else:
            items[target_idx] = incoming
            result = {
                "status": "updated",
                "id": incoming["id"],
                "created": False,
                "updated": True,
            }

        return items, result

    _, outcome = atomic_update_json_list(Path(PROFILES_NOTIFIER), _transform)
    print(f"[PROFILES] add_or_update_profile_by_name outcome={outcome}")
    return outcome


# ─────────────────────────────────────────────────────────────
# Backwards-Compatible Aliasse (falls alter Code Namen erwartet)
# ─────────────────────────────────────────────────────────────

# (Hier gerade keine zusätzlichen Aliasse nötig – falls du später alte Funktionsnamen
# zurückbringen musst, kannst du sie hier sauber mappen, z.B.:
# _update_profile_by_id = update_profile_by_id
# etc.)
