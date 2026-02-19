# notifier_evaluator/eval/validate.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional

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


DEBUG_PRINT = True


def _dbg(msg: str) -> None:
    if not DEBUG_PRINT:
        return
    try:
        print(msg)
    except Exception:
        pass


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


# ──────────────────────────────────────────────────────────────────────────────
# Tolerant helpers (schema drift between legacy vs NEW UI schema)
# ──────────────────────────────────────────────────────────────────────────────


def _get_bool(obj: Any, *names: str, default: bool = True) -> bool:
    """
    Best-effort boolean getter:
      - tries attribute names in order
      - accepts bool/int
      - accepts common strings
    """
    for n in names:
        try:
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is None:
                    continue
                if isinstance(v, bool):
                    return v
                if isinstance(v, int):
                    return bool(v)
                if isinstance(v, str):
                    s = v.strip().lower()
                    if s in ("1", "true", "yes", "y", "on", "enabled", "active"):
                        return True
                    if s in ("0", "false", "no", "n", "off", "disabled", "inactive"):
                        return False
                    # unknown string -> keep default
                    return default
                # unknown type -> default
                return default
        except Exception:
            continue
    return default


def _get_str(obj: Any, *names: str, default: str = "") -> str:
    for n in names:
        try:
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    return s
        except Exception:
            continue
    return default


def _get_rows(g: Any) -> List[Any]:
    """
    NEW schema uses `conditions`, legacy used `rows`.
    Return list (possibly empty).
    """
    rows = None
    try:
        rows = getattr(g, "rows", None)
    except Exception:
        rows = None
    if rows is None:
        try:
            rows = getattr(g, "conditions", None)
        except Exception:
            rows = None
    if isinstance(rows, list):
        return rows
    return []


def _get_logic_to_prev(r: Any) -> str:
    """
    NEW schema uses `logic` ("and"/"or") to chain to previous.
    Legacy schema uses `logic_to_prev`.
    """
    s = _get_str(r, "logic_to_prev", "logic", default="and").lower().strip()
    return s or "and"


def _get_count(side: Any) -> int:
    """
    Side in NEW schema often has no 'count'. Treat missing as 1.
    """
    try:
        if hasattr(side, "count"):
            v = getattr(side, "count")
            if v is None:
                return 1
            return int(v)
    except Exception:
        return -1
    return 1


# ──────────────────────────────────────────────────────────────────────────────
# Main validation
# ──────────────────────────────────────────────────────────────────────────────


def validate_profiles(profiles: List[Profile]) -> ValidationResult:
    res = ValidationResult(ok=True, profiles=len(profiles or []))

    for p in profiles or []:
        pid = _get_str(p, "profile_id", "id", "pid", default="<missing>") or "<missing>"

        # tolerate schema drift: profile can use enabled|active|is_enabled
        p_enabled = _get_bool(p, "enabled", "active", "is_enabled", default=True)
        if not p_enabled:
            _dbg(f"[validate] skip disabled profile={pid}")
            continue

        if not _get_str(p, "profile_id", "id", "pid", default=""):
            _add(res, severity="error", level="profile", pid="<missing>", gid=None, rid=None, field="profile_id", msg="missing profile_id")
        if not _get_str(p, "name", default=""):
            _add(res, severity="error", level="profile", pid=pid, gid=None, rid=None, field="name", msg="missing name")

        groups = []
        try:
            groups = list(getattr(p, "groups") or [])
        except Exception:
            groups = []

        for g in groups:
            res.groups += 1
            _validate_group(p, g, res)

    # Debug prints
    _dbg(
        "[validate] profiles=%d groups=%d rows=%d ok=%s errors=%d warns=%d"
        % (res.profiles, res.groups, res.rows, res.ok, res.errors_n, res.warns_n)
    )
    for e in res.errors[:50]:
        _dbg("[validate] %s %s" % (e.severity.upper(), e))
    if len(res.errors) > 50:
        _dbg("[validate] ... (%d more issues)" % (len(res.errors) - 50))

    return res


def _validate_group(p: Profile, g: Group, res: ValidationResult) -> None:
    pid = _get_str(p, "profile_id", "id", "pid", default="<missing>") or "<missing>"
    gid = _get_str(g, "gid", default="<missing>") or "<missing>"

    # tolerate schema drift: group can use enabled|active
    g_enabled = _get_bool(g, "enabled", "active", default=True)
    if not g_enabled:
        return

    if not _get_str(g, "gid", default=""):
        _add(res, severity="error", level="group", pid=pid, gid="<missing>", rid=None, field="gid", msg="missing gid")

    # group config sanity
    # NEW schema: threshold is per-condition (row), not per-group.
    # alarm may exist on some schemas; also NEW schema may use deactivate_on (string),
    # which is NOT the same thing as AlarmConfig. We only validate AlarmConfig if present.
    g_alarm = getattr(g, "alarm", None) if hasattr(g, "alarm") else None
    if g_alarm:
        _validate_alarm(pid, gid, g_alarm, res)

    rows = _get_rows(g)

    # rows
    if not rows:
        # not fatal, but likely unintended
        _add(
            res,
            severity="warn",
            level="group",
            pid=pid,
            gid=gid,
            rid=None,
            field="rows",
            msg="group has no rows/conditions (will evaluate UNKNOWN)",
        )

    for r in rows:
        res.rows += 1
        _validate_row(pid, gid, r, res)


def _validate_row(pid: str, gid: str, r: Condition, res: ValidationResult) -> None:
    rid = _get_str(r, "rid", default="<missing>") or "<missing>"

    # tolerate schema drift: row can use enabled|active|is_enabled; NEW schema often has none -> default True
    r_enabled = _get_bool(r, "enabled", "active", "is_enabled", default=True)
    if not r_enabled:
        _dbg(f"[validate] skip disabled row profile={pid} gid={gid} rid={rid}")
        return

    if not _get_str(r, "rid", default=""):
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid="<missing>", field="rid", msg="missing rid")

    # left/right basic
    left = getattr(r, "left", None)
    right = getattr(r, "right", None)

    if left is None:
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="left", msg="missing left indicator object")
    if right is None:
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="right", msg="missing right indicator object")

    if left is not None:
        ln = _get_str(left, "name", default="")
        if not ln:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="left.name", msg="missing left indicator name")

    if right is not None:
        rn = _get_str(right, "name", default="")
        if not rn:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="right.name", msg="missing right indicator name")

    # operator
    op = _get_str(r, "op", default="").strip().lower()
    if op not in ("gt", "gte", "lt", "lte", "eq", "ne"):
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="op", msg=f"invalid op '{getattr(r, 'op', None)}'")

    # counts (NEW schema may not provide counts -> treated as 1)
    if left is not None:
        lc = _get_count(left)
        if lc == -1:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="left.count", msg="left.count not int")
        elif lc < 1:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="left.count", msg="left.count must be >= 1")

    if right is not None:
        rc = _get_count(right)
        if rc == -1:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="right.count", msg="right.count not int")
        elif rc < 1:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="right.count", msg="right.count must be >= 1")

    # logic_to_prev (NEW schema uses `logic`)
    ltp = _get_logic_to_prev(r)
    if ltp not in ("and", "or"):
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="logic_to_prev", msg=f"invalid '{getattr(r, 'logic_to_prev', None) or getattr(r, 'logic', None)}'")

    # threshold per row (NEW schema)
    t = getattr(r, "threshold", None) if hasattr(r, "threshold") else None
    if t:
        _validate_threshold(pid, gid, rid, t, res)

    # NOTE: symbol/interval/exchange overrides are NOT validated here
    # because resolver provides fallbacks and engine may provide base symbols.
    # If you want strict: you can enforce allowed formats here.


def _validate_threshold(pid: str, gid: str, rid: str, t: ThresholdConfig, res: ValidationResult) -> None:
    """
    Supports both:
      - legacy ThresholdConfig fields (mode/streak_n/window_ticks/count_required)
      - NEW schema row.threshold: null or {type: streak|min_count | count|window+min_count}
    """
    # NEW schema: {type: "...", window: int, min_count: int}
    t_type = _get_str(t, "type", default="").lower().strip()
    if t_type:
        if t_type not in ("streak", "count"):
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.type", msg=f"invalid '{t_type}'")
            return

        if t_type == "streak":
            mn = None
            try:
                mn = int(getattr(t, "min_count"))
            except Exception:
                mn = None
            if mn is None:
                _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.min_count", msg="missing min_count for type=streak")
            else:
                if mn < 1:
                    _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.min_count", msg="min_count must be >= 1")
        if t_type == "count":
            w = None
            c = None
            try:
                w = int(getattr(t, "window"))
            except Exception:
                w = None
            try:
                c = int(getattr(t, "min_count"))
            except Exception:
                c = None

            if w is None:
                _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.window", msg="missing window for type=count")
            if c is None:
                _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.min_count", msg="missing min_count for type=count")

            if w is not None and c is not None:
                if w < 1:
                    _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.window", msg="window must be >= 1")
                if c < 1:
                    _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.min_count", msg="min_count must be >= 1")
                if c > w:
                    _add(res, severity="warn", level="row", pid=pid, gid=gid, rid=rid, field="threshold", msg=f"min_count ({c}) > window ({w}) => impossible")
        return

    # Legacy schema: ThresholdConfig(mode=none|streak|count, ...)
    mode = _get_str(t, "mode", default="none").strip().lower()
    if mode not in ("none", "streak", "count"):
        _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.mode", msg=f"invalid '{getattr(t, 'mode', None)}'")
        return

    if mode == "streak":
        if getattr(t, "streak_n", None) is None:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.streak_n", msg="missing streak_n for mode=streak")
        else:
            try:
                if int(getattr(t, "streak_n")) < 1:
                    _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.streak_n", msg="streak_n must be >= 1")
            except Exception:
                _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.streak_n", msg="streak_n not int")

    if mode == "count":
        if getattr(t, "window_ticks", None) is None:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.window_ticks", msg="missing window_ticks for mode=count")
        if getattr(t, "count_required", None) is None:
            _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.count_required", msg="missing count_required for mode=count")

        if getattr(t, "window_ticks", None) is not None and getattr(t, "count_required", None) is not None:
            try:
                w = int(getattr(t, "window_ticks"))
                c = int(getattr(t, "count_required"))
                if w < 1:
                    _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.window_ticks", msg="window_ticks must be >= 1")
                if c < 1:
                    _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold.count_required", msg="count_required must be >= 1")
                if c > w:
                    _add(res, severity="warn", level="row", pid=pid, gid=gid, rid=rid, field="threshold", msg=f"count_required ({c}) > window_ticks ({w}) => impossible")
            except Exception:
                _add(res, severity="error", level="row", pid=pid, gid=gid, rid=rid, field="threshold", msg="window_ticks/count_required not int")


def _validate_alarm(pid: str, gid: str, a: AlarmConfig, res: ValidationResult) -> None:
    mode = _get_str(a, "mode", default="always_on").strip().lower()
    if mode not in ("always_on", "auto_off", "pre_notification"):
        _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="alarm.mode", msg=f"invalid '{getattr(a, 'mode', None)}'")

    try:
        cd = int(getattr(a, "cooldown_sec", 0) or 0)
        if cd < 0:
            _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="alarm.cooldown_sec", msg="cooldown_sec must be >= 0")
    except Exception:
        _add(res, severity="error", level="group", pid=pid, gid=gid, rid=None, field="alarm.cooldown_sec", msg="cooldown_sec not int")

    # edge_only is bool; pydantic usually coerces, but keep it safe
    edge = getattr(a, "edge_only", None)
    if edge not in (True, False, None):
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
