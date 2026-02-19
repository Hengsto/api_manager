# notifier_evaluator/eval/validate.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from notifier_evaluator.models.schema import Condition, Group, Profile


def _dbg(msg: str) -> None:
    print(f"[evaluator][DBG] {msg}")


@dataclass
class ValidationError:
    severity: str
    level: str
    profile_id: str
    gid: Optional[str]
    rid: Optional[str]
    field: str
    message: str


@dataclass
class ValidationResult:
    ok: bool
    errors: List[ValidationError] = field(default_factory=list)
    profiles: int = 0
    groups: int = 0
    rows: int = 0
    errors_n: int = 0


def _add(res: ValidationResult, *, level: str, profile_id: str, gid: Optional[str], rid: Optional[str], field: str, message: str) -> None:
    res.ok = False
    res.errors_n += 1
    res.errors.append(
        ValidationError(
            severity="error",
            level=level,
            profile_id=profile_id,
            gid=gid,
            rid=rid,
            field=field,
            message=message,
        )
    )


def validate_profiles(profiles: List[Profile]) -> ValidationResult:
    res = ValidationResult(ok=True, profiles=len(profiles or []))
    for p in profiles or []:
        if not p.enabled:
            _dbg(f"validate skip disabled profile id={p.id}")
            continue

        if not p.groups:
            _add(res, level="profile", profile_id=p.id, gid=None, rid=None, field="groups", message="profile.enabled=true but groups is empty")
            continue

        for g in p.groups:
            _validate_group(p, g, res)

    _dbg(f"validate done ok={res.ok} profiles={res.profiles} groups={res.groups} rows={res.rows} errors={res.errors_n}")
    return res


def _validate_group(profile: Profile, group: Group, res: ValidationResult) -> None:
    res.groups += 1
    if not group.active:
        _dbg(f"validate skip inactive group profile={profile.id} gid={group.gid}")
        return

    if not group.conditions:
        _add(
            res,
            level="group",
            profile_id=profile.id,
            gid=group.gid,
            rid=None,
            field="conditions",
            message="group.active=true but conditions is empty",
        )
        return

    for cond in group.conditions:
        _validate_condition(profile, group, cond, res)


def _validate_condition(profile: Profile, group: Group, cond: Condition, res: ValidationResult) -> None:
    res.rows += 1
    if cond.logic not in ("and", "or"):
        _add(res, level="row", profile_id=profile.id, gid=group.gid, rid=cond.rid, field="logic", message="logic must be 'and' or 'or'")
