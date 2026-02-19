# api/notifier/validate.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Union

from pydantic import ValidationError

from api.notifier.profiles import Profile  # nutzt dein strict NEW schema

logger = logging.getLogger(__name__)

DEBUG_PRINT = True


def _dbg(msg: str) -> None:
    if DEBUG_PRINT:
        try:
            print(msg)
        except Exception:
            pass


def _format_validation_error(e: ValidationError) -> List[Dict[str, Any]]:
    """
    Convert Pydantic ValidationError into a UI-friendly list with clear paths.
    """
    out: List[Dict[str, Any]] = []

    # pydantic v2: e.errors() returns dicts with 'loc', 'msg', 'type'
    # pydantic v1: same shape enough for our usage
    for err in (e.errors() or []):
        loc = err.get("loc", [])
        msg = err.get("msg", "Validation error")
        typ = err.get("type", "")

        # turn ('groups', 0, 'conditions', 0, 'left', 'output') into "groups[0].conditions[0].left.output"
        path_parts: List[str] = []
        for p in loc:
            if isinstance(p, int):
                # attach to previous element like groups[0]
                if path_parts:
                    path_parts[-1] = f"{path_parts[-1]}[{p}]"
                else:
                    path_parts.append(f"[{p}]")
            else:
                path_parts.append(str(p))

        path = ".".join(path_parts) if path_parts else ""
        out.append({"path": path, "message": msg, "type": typ})

    return out


def validate_profile_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a single profile payload (NEW schema only).

    NOTE:
    - UI may send id=None / missing for new profiles.
    - Profile schema currently requires id.
    - We inject a temporary id ONLY for validation.
    """
    data = dict(payload or {})

    injected_id: str = ""
    try:
        _dbg(f"[VALIDATE] incoming id type={type(data.get('id')).__name__} value={data.get('id')!r}")
        logger.debug("[VALIDATE] incoming id type=%s value=%r", type(data.get("id")).__name__, data.get("id"))
    except Exception:
        pass

    # If id missing/empty -> inject a temporary id for validation only
    if data.get("id") in (None, "", "null", "None"):
        injected_id = "tmp-" + uuid.uuid4().hex
        data["id"] = injected_id
        _dbg(f"[VALIDATE] injected temporary id={injected_id!r} for validation only")
        logger.debug("[VALIDATE] injected temporary id=%r for validation only", injected_id)
    else:
        # ensure id is str if present
        try:
            data["id"] = str(data["id"])
        except Exception as e:
            _dbg(f"[VALIDATE] ❌ id not convertible to str: {e!r}")
            logger.warning("[VALIDATE] id not convertible to str: %r", e)
            return {
                "ok": False,
                "errors": [{"path": "id", "message": "ID must be convertible to string", "type": "type_error"}],
            }

    try:
        Profile(**data)
        # Return injected_id (if any) as debug-only info; UI can ignore it.
        res = {"ok": True, "errors": []}
        if injected_id:
            res["injected_id"] = injected_id
        return res

    except ValidationError as e:
        res = {"ok": False, "errors": _format_validation_error(e)}
        if injected_id:
            res["injected_id"] = injected_id
        return res

    except Exception as e:
        _dbg(f"[VALIDATE] ❌ crashed: {e!r}")
        logger.error("[VALIDATE] Validation crashed: %r", e)
        res = {
            "ok": False,
            "errors": [{"path": "", "message": f"Validation crashed: {e}", "type": "internal_error"}],
        }
        if injected_id:
            res["injected_id"] = injected_id
        return res


def validate_profiles_payload(payload: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate either:
      - one profile object
      - a list of profile objects
    """
    if isinstance(payload, dict):
        res = validate_profile_payload(payload)
        meta = {
            "index": 0,
            "id": payload.get("id"),
            "name": payload.get("name"),
        }
        return {
            "ok": res.get("ok", False),
            "count": 1,
            "results": [{**meta, **res}],
        }

    if not isinstance(payload, list):
        return {
            "ok": False,
            "count": 0,
            "results": [],
            "errors": [
                {
                    "path": "",
                    "message": "Payload must be a profile object or a list of profiles",
                    "type": "type_error",
                }
            ],
        }

    results: List[Dict[str, Any]] = []
    ok_all = True

    for i, item in enumerate(payload):
        if not isinstance(item, dict):
            ok_all = False
            results.append(
                {
                    "index": i,
                    "id": None,
                    "name": None,
                    "ok": False,
                    "errors": [{"path": f"[{i}]", "message": "Profile entry must be an object", "type": "type_error"}],
                }
            )
            continue

        r = validate_profile_payload(item)
        meta = {"index": i, "id": item.get("id"), "name": item.get("name")}
        results.append({**meta, **r})

        if not r.get("ok"):
            ok_all = False

    return {"ok": ok_all, "count": len(payload), "results": results}
