# notifier_evaluator/debug/dump.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Iterable, List, Optional

from notifier_evaluator.fetch.types import RequestKey
from notifier_evaluator.models.runtime import (
    ConditionResult,
    FetchResult,
    HistoryEvent,
    ResolvedContext,
    ResolvedPair,
    StatusKey,
    StatusState,
)


# ──────────────────────────────────────────────────────────────────────────────
# Dump helpers
# - NUR Debug / Diagnose
# - Keine Logik, keine Side Effects
# ──────────────────────────────────────────────────────────────────────────────


def dump_json(
    obj: Any,
    *,
    title: str = "",
    max_len: int = 10_000,
    indent: int = 2,
) -> str:
    """
    Serialisiert beliebige Objekte tolerant nach JSON.
    Schneidet ab, wenn zu lang.
    """
    try:
        payload = _to_jsonable(obj)
        txt = json.dumps(payload, indent=indent, ensure_ascii=False)
    except Exception as e:
        txt = f"<dump_failed err={e}>"

    if max_len and len(txt) > max_len:
        txt = txt[:max_len] + "\n... <truncated>"

    if title:
        return f"=== {title} ===\n{txt}"
    return txt


# ──────────────────────────────────────────────────────────────────────────────
# Specific dump helpers (Engine-relevant)
# ──────────────────────────────────────────────────────────────────────────────


def dump_request_plan(
    *,
    profile_id: str,
    gid: str,
    base_symbol: str,
    unique_keys: Iterable[RequestKey],
    row_map: Dict[Any, RequestKey],
) -> str:
    """
    Dump geplante Requests für eine (profile,gid,symbol)-Unit.
    """
    data = {
        "profile_id": profile_id,
        "gid": gid,
        "symbol": base_symbol,
        "unique_requests": [k.short() for k in unique_keys],
        "row_map": {str(k): v.short() for k, v in row_map.items()},
    }
    return dump_json(data, title="REQUEST_PLAN")


def dump_resolved_contexts(
    resolved_pairs: Dict[str, ResolvedPair],
) -> str:
    """
    Dump resolved LEFT/RIGHT contexts pro row.
    """
    out = {}
    for rid, pair in resolved_pairs.items():
        out[rid] = {
            "left": _ctx(pair.left),
            "right": _ctx(pair.right),
        }
    return dump_json(out, title="RESOLVED_CONTEXTS")


def dump_fetch_results(
    fetch_results: Dict[RequestKey, FetchResult],
) -> str:
    """
    Dump Fetch-Ergebnisse (gekürzt).
    """
    out = {}
    for k, fr in fetch_results.items():
        out[k.short()] = {
            "ok": fr.ok,
            "latest_value": fr.latest_value,
            "latest_ts": fr.latest_ts,
            "error": fr.error,
        }
    return dump_json(out, title="FETCH_RESULTS")


def dump_condition_results(
    results: List[ConditionResult],
) -> str:
    """
    Dump ConditionResults einer Kette.
    """
    out = []
    for r in results:
        out.append(
            {
                "rid": r.rid,
                "state": r.state.value,
                "op": r.op,
                "left": r.left_value,
                "right": r.right_value,
                "reason": r.reason,
                "debug": r.debug,
            }
        )
    return dump_json(out, title="CONDITION_RESULTS")


def dump_status(
    key: StatusKey,
    state: StatusState,
) -> str:
    """
    Dump StatusState für einen Key.
    """
    return dump_json(
        {
            "status_key": _safe_asdict(key),
            "state": _safe_asdict(state),
        },
        title="STATUS_STATE",
    )


def dump_history(
    events: List[HistoryEvent],
    limit: int = 20,
) -> str:
    """
    Dump letzte History-Events.
    """
    evs = events[-limit:]
    out = []
    for e in evs:
        out.append(_safe_asdict(e))
    return dump_json(out, title=f"HISTORY_LAST_{len(out)}")


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _ctx(c: ResolvedContext) -> Dict[str, Any]:
    return {
        "symbol": c.symbol,
        "interval": c.interval,
        "exchange": c.exchange,
        "clock_interval": c.clock_interval,
        "source": c.source,
    }


def _safe_asdict(obj: Any) -> Any:
    if is_dataclass(obj):
        try:
            return asdict(obj)
        except Exception:
            pass
    return _to_jsonable(obj)


def _to_jsonable(obj: Any) -> Any:
    """
    Macht Objekte JSON-kompatibel (best-effort).
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(x) for x in obj]
    if is_dataclass(obj):
        return {k: _to_jsonable(v) for k, v in asdict(obj).items()}
    # Fallback
    return str(obj)
