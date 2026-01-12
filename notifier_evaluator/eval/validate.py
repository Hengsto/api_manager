# notifier_evaluator/eval/validate.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from notifier_evaluator.models.schema import Profile, Group, Condition, ThresholdConfig, AlarmConfig


# ──────────────────────────────────────────────────────────────────────────────
# Validation
# - Detect invalid profiles/groups/rows BEFORE engine runs
# - Returns errors + optionally filtered structures
#
# Notes:
# - "ok" should represent: no FATAL errors
# - warnings are collected but do NOT flip ok=False
# - disabled profiles/groups/rows are skipped (they shouldn't block loading)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class ValidationError:
    severity: str  # "error" | "warn"
    level: str  # "profile" | "group" | "row"
    profile_id: str
    gid: Optional[str]
    rid: Optional[str]
    field: str
    message: str


@dataclass
class ValidationResult:
    ok: bool
    errors: List[ValidationError] = field(default_factory=list)

    # convenience counters
    profiles: int = 0
    groups: int = 0
    rows: int = 0

    # extra counters
    errors_n: int = 0
    warns_n: int = 0


def validate_profiles(profiles: List[Profile]) -> ValidationResult:
    res = ValidationResult(ok=True, profiles=len(profiles or []))

    for p in profiles or []:
        pid = (p.profile_id or "").strip() or "<missing>"

        if not p.enabled:
            print(f"[validate] skip disabled profile={pid}")
            continue

        if not (p.profile_id or "").strip():
            _add(res, severity="error", level="profile", pid="<missing>", gid=None, rid=None, field="profile_id", msg="missing profile_id")
        if not (p.name or "").strip():
            _add(res, severity="error", level="profile", pid=pid, gid=None, rid=None, field="name", msg="missing name")

        for g in p.groups or []:
            res.groups += 1
            _validate_group(p, g, res)

    # Debug prints
    print(
        "[validate] profiles=%d groups=%d rows=%d ok=%s errors=%d warns=%d"
        % (res.profiles, res.groups, res.rows, res.ok, res.errors_n, res.warns_n)
    )
    for e in res.errors[:50]:
        print("[validate] %s %s" % (e.severity.upper(), e))
    if len(res.errors) > 50:
        print("[validate] ... (%d more issues)" % (len(res.errors) - 50))

    return res


def _validate_group(p: Profile, g: Group, res: ValidationResult) -> None:
    pid = (p.profile_id or "").strip() or "<missing>"
    gid = (g.gid or "").strip() or "<missing>"

    if not g.enabled:
        print(f"[validate] skip disabled group profile={pid} gid={gid}")
        return

    if not (g.gid or "").strip():
        _add(res, severity="error", level="group", pid=pid, gid="<missing>", rid=None, field="gid", msg="missing gid")

    # group config sanity
    if g.threshold:
        _validate_threshold(pid, gid, g.threshold, res)
    if g.alarm:
        _validate_alarm(pid, gid, g.alarm, res)

    # rows
    if not g.rows:
        # not fatal, but likely unintended
        _add(res, severity="warn", level="group", pid=pid, gid=gid, rid=None, field="rows", msg="group has no rows (will evaluate UNKNOWN)")

    for r in g.rows or []:
        res.rows += 1
        _validate_row(pid, gid, r, res)


def _validate_row(pid: str, gid: str, r: Condition, res: ValidationResult) -> None:
    rid = (r.rid or "").strip() or "<missing>"

    if not r.enabled:
        print(f"[validate] skip disabled row profile={pid} gid={gid} rid={rid}")
        return

    if not (r.rid or "").strip():
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid="<missing>", field="rid", msg="missing rid")

    # left/right basic
    if not (r.left.name or "").strip():
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="left.name", msg="missing left indicator name")
    if not (r.right.name or "").strip():
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="right.name", msg="missing right indicator name")

    # operator
    op = (r.op or "").strip().lower()
    if op not in ("gt", "gte", "lt", "lte", "eq", "ne"):
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="op", msg=f"invalid op '{r.op}'")

    # counts
    try:
        if int(r.left.count or 1) < 1:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="left.count", msg="left.count must be >= 1")
    except Exception:
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="left.count", msg="left.count not int")

    try:
        if int(r.right.count or 1) < 1:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="right.count", msg="right.count must be >= 1")
    except Exception:
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="right.count", msg="right.count not int")

    # logic_to_prev
    ltp = (r.logic_to_prev or "and").strip().lower()
    if ltp not in ("and", "or"):
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="logic_to_prev", msg=f"invalid '{r.logic_to_prev}'")

    # NOTE: symbol/interval/exchange overrides are NOT validated here
    # because resolver provides fallbacks and engine may provide base symbols.
    # If you want strict: you can enforce allowed formats here.


def _validate_threshold(pid: str, gid: str, t: ThresholdConfig, res: ValidationResult) -> None:
    mode = (t.mode or "none").strip().lower()
    if mode not in ("none", "streak", "count"):
        _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.mode", msg=f"invalid '{t.mode}'")
        return

    if mode == "streak":
        if t.streak_n is None:
            _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.streak_n", msg="missing streak_n for mode=streak")
        else:
            try:
                if int(t.streak_n) < 1:
                    _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.streak_n", msg="streak_n must be >= 1")
            except Exception:
                _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.streak_n", msg="streak_n not int")

    if mode == "count":
        if t.window_ticks is None:
            _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.window_ticks", msg="missing window_ticks for mode=count")
        if t.count_required is None:
            _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.count_required", msg="missing count_required for mode=count")

        if t.window_ticks is not None and t.count_required is not None:
            try:
                w = int(t.window_ticks)
                c = int(t.count_required)
                if w < 1:
                    _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.window_ticks", msg="window_ticks must be >= 1")
                if c < 1:
                    _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold.count_required", msg="count_required must be >= 1")
                if c > w:
                    # not fatal but usually wrong
                    _add(res, severity="warn", level="group", pid=pid, gid=gid, rid=None, field="threshold", msg=f"count_required ({c}) > window_ticks ({w}) => impossible")
            except Exception:
                _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="threshold", msg="window_ticks/count_required not int")


def _validate_alarm(pid: str, gid: str, a: AlarmConfig, res: ValidationResult) -> None:
    mode = (a.mode or "always_on").strip().lower()
    if mode not in ("always_on", "auto_off", "pre_notification"):
        _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="alarm.mode", msg=f"invalid '{a.mode}'")

    try:
        cd = int(a.cooldown_sec or 0)
        if cd < 0:
            _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="alarm.cooldown_sec", msg="cooldown_sec must be >= 0")
    except Exception:
        _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="alarm.cooldown_sec", msg="cooldown_sec not int")

    # edge_only is bool; pydantic usually coerces, but keep it safe
    if a.edge_only not in (True, False):
        _add(res, severity="warn", level="group", pid=pid, gid=gid, rid=None, field="alarm.edge_only", msg="edge_only is not a bool (coercion failed?)")


def _add(
    res: ValidationResult,
    *,
    severity: str,
    level: str,
    pid: str,
    gid: Optional[str],
    rid: Optional[str],
    field: str,
    msg: str,
) -> None:
    sev = (severity or "error").strip().lower()
    if sev not in ("error", "warn"):
        sev = "error"

    res.errors.append(
        ValidationError(
            severity=sev,
            level=level,
            profile_id=pid,
            gid=gid,
            rid=rid,
            field=field,
            message=msg,
        )
    )

    if sev == "error":
        res.ok = False
        res.errors_n += 1
    else:
        res.warns_n += 1
