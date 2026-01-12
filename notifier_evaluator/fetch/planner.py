# notifier_evaluator/fetch/planner.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from notifier_evaluator.fetch.types import RequestKey
from notifier_evaluator.models.schema import Condition
from notifier_evaluator.models.runtime import ResolvedPair, RowSide


# ──────────────────────────────────────────────────────────────────────────────
# Planner
#
# Inputs:
#   - resolved contexts pro row: ResolvedPair(left_ctx, right_ctx)
#   - Condition: enthält left/right IndicatorRef(name, params, output, count)
#
# Output:
#   - unique_keys: list[RequestKey]  (deduped)
#   - row_map: dict[(profile_id,gid,rid,symbol,side)] -> RequestKey
#
# Symbol in row_map:
#   das ist das "base_symbol" (Engine eval loop symbol), nicht zwingend left/right symbol.
#   Why? Status/Chain laufen pro (profile,gid,base_symbol).
#   left/right symbol kann abweichen; das ist im RequestKey sowieso drin.
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class PlanResult:
    unique_keys: List[RequestKey] = field(default_factory=list)
    row_map: Dict[Tuple[str, str, str, str, str], RequestKey] = field(default_factory=dict)
    debug: Dict[str, int] = field(default_factory=dict)


def _safe_count(x: Optional[int]) -> int:
    try:
        n = int(x or 1)
        return n if n >= 1 else 1
    except Exception:
        return 1


def plan_requests_for_symbol(
    *,
    profile_id: str,
    gid: str,
    base_symbol: str,
    rows: List[Condition],
    resolved_pairs: Dict[str, ResolvedPair],
    mode: str = "latest",
    as_of: Optional[str] = None,
) -> PlanResult:
    """
    Plan requests for ONE (profile_id, gid, base_symbol) evaluation unit.

    resolved_pairs: dict[rid] -> ResolvedPair
    """
    mode2 = (mode or "latest").strip() or "latest"
    as_of2 = (as_of.strip() if isinstance(as_of, str) else as_of)

    unique_set: Set[RequestKey] = set()
    unique_keys: List[RequestKey] = []
    row_map: Dict[Tuple[str, str, str, str, str], RequestKey] = {}

    dbg_rows = 0
    dbg_keys = 0
    dbg_dedup = 0
    dbg_skipped = 0

    for cond in rows or []:
        if not cond.enabled:
            continue

        rid = cond.rid
        pair = resolved_pairs.get(rid)
        if pair is None:
            dbg_skipped += 1
            print("[planner] WARN missing resolved pair rid=%s profile=%s gid=%s base_symbol=%s" % (rid, profile_id, gid, base_symbol))
            continue

        left_name = (cond.left.name or "").strip()
        right_name = (cond.right.name or "").strip()
        if not left_name or not right_name:
            dbg_skipped += 1
            print(
                "[planner] WARN missing indicator name rid=%s profile=%s gid=%s base_symbol=%s left=%s right=%s (skip row)"
                % (rid, profile_id, gid, base_symbol, bool(left_name), bool(right_name))
            )
            continue

        left_count = _safe_count(cond.left.count)
        right_count = _safe_count(cond.right.count)

        # LEFT
        k_left = RequestKey.from_parts(
            indicator=left_name,
            ctx=pair.left,
            params=cond.left.params or {},
            output=cond.left.output,
            count=left_count,
            mode=mode2,
            as_of=as_of2,
        )

        map_key_left = (profile_id, gid, rid, base_symbol, RowSide.LEFT.value)
        row_map[map_key_left] = k_left
        dbg_keys += 1

        if k_left not in unique_set:
            unique_set.add(k_left)
            unique_keys.append(k_left)
        else:
            dbg_dedup += 1

        # RIGHT
        k_right = RequestKey.from_parts(
            indicator=right_name,
            ctx=pair.right,
            params=cond.right.params or {},
            output=cond.right.output,
            count=right_count,
            mode=mode2,
            as_of=as_of2,
        )

        map_key_right = (profile_id, gid, rid, base_symbol, RowSide.RIGHT.value)
        row_map[map_key_right] = k_right
        dbg_keys += 1

        if k_right not in unique_set:
            unique_set.add(k_right)
            unique_keys.append(k_right)
        else:
            dbg_dedup += 1

        dbg_rows += 1

        print(
            "[planner] profile=%s gid=%s base_symbol=%s rid=%s -> L=%s | R=%s"
            % (profile_id, gid, base_symbol, rid, k_left.short(), k_right.short())
        )

    print(
        "[planner] DONE profile=%s gid=%s base_symbol=%s rows=%d skipped=%d planned_keys=%d unique=%d dedup=%d mode=%s as_of=%s"
        % (profile_id, gid, base_symbol, dbg_rows, dbg_skipped, dbg_keys, len(unique_keys), dbg_dedup, mode2, as_of2)
    )

    return PlanResult(
        unique_keys=unique_keys,
        row_map=row_map,
        debug={
            "rows": dbg_rows,
            "skipped": dbg_skipped,
            "keys_total": dbg_keys,
            "unique": len(unique_keys),
            "dedup": dbg_dedup,
        },
    )
