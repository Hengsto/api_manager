# notifier_evaluator/fetch/planner.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

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
    unique_set = set()
    unique_keys: List[RequestKey] = []
    row_map: Dict[Tuple[str, str, str, str, str], RequestKey] = {}

    dbg_rows = 0
    dbg_keys = 0
    dbg_dedup = 0

    for cond in rows or []:
        if not cond.enabled:
            continue
        rid = cond.rid
        pair = resolved_pairs.get(rid)
        if pair is None:
            print("[planner] WARN missing resolved pair rid=%s profile=%s gid=%s base_symbol=%s" % (rid, profile_id, gid, base_symbol))
            continue

        # LEFT
        k_left = RequestKey.from_parts(
            indicator=cond.left.name,
            ctx=pair.left,
            params=cond.left.params or {},
            output=cond.left.output,
            count=cond.left.count,
            mode=mode,
            as_of=as_of,
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
            indicator=cond.right.name,
            ctx=pair.right,
            params=cond.right.params or {},
            output=cond.right.output,
            count=cond.right.count,
            mode=mode,
            as_of=as_of,
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
            "[planner] profile=%s gid=%s base_symbol=%s rid=%s -> "
            "L=%s | R=%s"
            % (profile_id, gid, base_symbol, rid, k_left.short(), k_right.short())
        )

    print(
        "[planner] DONE profile=%s gid=%s base_symbol=%s rows=%d planned_keys=%d unique=%d dedup=%d"
        % (profile_id, gid, base_symbol, dbg_rows, dbg_keys, len(unique_keys), dbg_dedup)
    )

    return PlanResult(
        unique_keys=unique_keys,
        row_map=row_map,
        debug={
            "rows": dbg_rows,
            "keys_total": dbg_keys,
            "unique": len(unique_keys),
            "dedup": dbg_dedup,
        },
    )
