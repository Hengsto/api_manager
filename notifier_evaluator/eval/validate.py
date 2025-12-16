# notifier_evaluator/eval/validate.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from notifier_evaluator.models.schema import Profile, Group, Condition, ThresholdConfig, AlarmConfig


# ──────────────────────────────────────────────────────────────────────────────
# Validation
# - Detect invalid profiles/groups/rows BEFORE engine runs
# - Returns errors + optionally filtered structures
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class ValidationError:
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


def validate_profiles(profiles: List[Profile]) -> ValidationResult:
    res = ValidationResult(ok=True, profiles=len(profiles or []))

    for p in profiles or []:
        if not (p.profile_id or "").strip():
            res.ok = False
            res.errors.append(_err("profile", "<missing>", None, None, "profile_id", "missing profile_id"))
        if not (p.name or "").strip():
            res.ok = False
            res.errors.append(_err("profile", p.profile_id or "<missing>", None, None, "name", "missing name"))

        for g in p.groups or []:
            res.groups += 1
            _validate_group(p, g, res)

    # Debug prints
    print("[validate] profiles=%d groups=%d rows=%d ok=%s errors=%d"
          % (res.profiles, res.groups, res.rows, res.ok, len(res.errors)))
    for e in res.errors[:50]:
        print("[validate] ERROR %s" % e)
    if len(res.errors) > 50:
        print("[validate] ... (%d more errors)" % (len(res.errors) - 50))

    return res


def _validate_group(p: Profile, g: Group, res: ValidationResult) -> None:
    pid = p.profile_id or "<missing>"

    if not (g.gid or "").strip():
        res.ok = False
        res.errors.append(_err("group", pid, "<missing>", None, "gid", "missing gid"))

    # group config sanity
    if g.threshold:
        _validate_threshold(pid, g.gid, g.threshold, res)
    if g.alarm:
        _validate_alarm(pid, g.gid, g.alarm, res)

    # rows
    if not g.rows:
        # not fatal, but likely unintended
        res.errors.append(_err("group", pid, g.gid, None, "rows", "group has no rows (will evaluate UNKNOWN)"))

    for r in g.rows or []:
        res.rows += 1
        _validate_row(pid, g.gid, r, res)


def _validate_row(pid: str, gid: str, r: Condition, res: ValidationResult) -> None:
    rid = r.rid or "<missing>"

    if not (r.rid or "").strip():
        res.ok = False
        res.errors.append(_err("row", pid, gid, "<missing>", "rid", "missing rid"))

    # left/right basic
    if not (r.left.name or "").strip():
        res.ok = False
        res.errors.append(_err("row", pid, gid, rid, "left.name", "missing left indicator name"))
    if not (r.right.name or "").strip():
        res.ok = False
        res.errors.append(_err("row", pid, gid, rid, "right.name", "missing right indicator name"))

    # operator
    op = (r.op or "").strip().lower()
    if op not in ("gt", "gte", "lt", "lte", "eq", "ne"):
        res.ok = False
        res.errors.append(_err("row", pid, gid, rid, "op", f"invalid op '{r.op}'"))

    # counts
    try:
        if int(r.left.count or 1) < 1:
            res.ok = False
            res.errors.append(_err("row", pid, gid, rid, "left.count", "left.count must be >= 1"))
    except Exception:
        res.ok = False
        res.errors.append(_err("row", pid, gid, rid, "left.count", "left.count not int"))

    try:
        if int(r.right.count or 1) < 1:
            res.ok = False
            res.errors.append(_err("row", pid, gid, rid, "right.count", "right.count must be >= 1"))
    except Exception:
        res.ok = False
        res.errors.append(_err("row", pid, gid, rid, "right.count", "right.count not int"))

    # logic_to_prev
    ltp = (r.logic_to_prev or "and").strip().lower()
    if ltp not in ("and", "or"):
        res.ok = False
        res.errors.append(_err("row", pid, gid, rid, "logic_to_prev", f"invalid '{r.logic_to_prev}'"))

    # NOTE: symbol/interval/exchange overrides are NOT validated here
    # because resolver provides fallbacks and engine may provide base symbols.
    # If you want strict: you can enforce allowed formats here.


def _validate_threshold(pid: str, gid: str, t: ThresholdConfig, res: ValidationResult) -> None:
    mode = (t.mode or "none").strip().lower()
    if mode not in ("none", "streak", "count"):
        res.ok = False
        res.errors.append(_err("group", pid, gid, None, "threshold.mode", f"invalid '{t.mode}'"))
        return

    if mode == "streak":
        if t.streak_n is None:
            res.ok = False
            res.errors.append(_err("group", pid, gid, None, "threshold.streak_n", "missing streak_n for mode=streak"))
        else:
            try:
                if int(t.streak_n) < 1:
                    res.ok = False
                    res.errors.append(_err("group", pid, gid, None, "threshold.streak_n", "streak_n must be >= 1"))
            except Exception:
                res.ok = False
                res.errors.append(_err("group", pid, gid, None, "threshold.streak_n", "streak_n not int"))

    if mode == "count":
        if t.window_ticks is None:
            res.ok = False
            res.errors.append(_err("group", pid, gid, None, "threshold.window_ticks", "missing window_ticks for mode=count"))
        if t.count_required is None:
            res.ok = False
            res.errors.append(_err("group", pid, gid, None, "threshold.count_required", "missing count_required for mode=count"))

        if t.window_ticks is not None and t.count_required is not None:
            try:
                w = int(t.window_ticks)
                c = int(t.count_required)
                if w < 1:
                    res.ok = False
                    res.errors.append(_err("group", pid, gid, None, "threshold.window_ticks", "window_ticks must be >= 1"))
                if c < 1:
                    res.ok = False
                    res.errors.append(_err("group", pid, gid, None, "threshold.count_required", "count_required must be >= 1"))
                if c > w:
                    # not fatal but usually wrong
                    res.errors.append(_err("group", pid, gid, None, "threshold", f"count_required ({c}) > window_ticks ({w}) => impossible"))
            except Exception:
                res.ok = False
                res.errors.append(_err("group", pid, gid, None, "threshold", "window_ticks/count_required not int"))


def _validate_alarm(pid: str, gid: str, a: AlarmConfig, res: ValidationResult) -> None:
    mode = (a.mode or "always_on").strip().lower()
    if mode not in ("always_on", "auto_off", "pre_notification"):
        res.ok = False
        res.errors.append(_err("group", pid, gid, None, "alarm.mode", f"invalid '{a.mode}'"))

    try:
        cd = int(a.cooldown_sec or 0)
        if cd < 0:
            res.ok = False
            res.errors.append(_err("group", pid, gid, None, "alarm.cooldown_sec", "cooldown_sec must be >= 0"))
    except Exception:
        res.ok = False
        res.errors.append(_err("group", pid, gid, None, "alarm.cooldown_sec", "cooldown_sec not int"))

    # edge_only is bool; pydantic usually coerces, but keep it safe
    if a.edge_only not in (True, False):
        res.errors.append(_err("group", pid, gid, None, "alarm.edge_only", "edge_only is not a bool (coercion failed?)"))


def _err(level: str, pid: str, gid: Optional[str], rid: Optional[str], field: str, msg: str) -> ValidationError:
    return ValidationError(level=level, profile_id=pid, gid=gid, rid=rid, field=field, message=msg)
