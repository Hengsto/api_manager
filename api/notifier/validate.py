# api/notifier/validate.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Union

from pydantic import ValidationError

from api.notifier.profiles import Profile  # nutzt dein strict NEW schema


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
    """
    try:
        Profile(**(payload or {}))
        return {"ok": True, "errors": []}
    except ValidationError as e:
        return {"ok": False, "errors": _format_validation_error(e)}


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
            "ok": res["ok"],
            "count": 1,
            "results": [{**meta, **res}],
        }


    if not isinstance(payload, list):
        return {
            "ok": False,
            "count": 0,
            "results": [],
            "errors": [{"path": "", "message": "Payload must be a profile object or a list of profiles", "type": "type_error"}],
        }

    results: List[Dict[str, Any]] = []
    ok_all = True

    for i, item in enumerate(payload):
        if not isinstance(item, dict):
            ok_all = False
            results.append({
                "ok": False,
                "errors": [{"path": f"[{i}]", "message": "Profile entry must be an object", "type": "type_error"}],
            })
            continue

        r = validate_profile_payload(item)
        meta = {"index": i, "id": item.get("id"), "name": item.get("name")}
        results.append({**meta, **r})

        if not r["ok"]:
            ok_all = False

    return {"ok": ok_all, "count": len(payload), "results": results}
