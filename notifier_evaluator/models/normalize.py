# notifier_evaluator/models/normalize.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class NormalizationError(Exception):
    """Raised when profile normalization fails in strict mode."""
    pass


def _is_blank(x: Any) -> bool:
    """True if value is None or an empty/whitespace-only string."""
    return x is None or (isinstance(x, str) and x.strip() == "")


def _s(x: Any) -> str:
    """Safe string cast + strip."""
    if x is None:
        return ""
    try:
        return str(x).strip()
    except Exception as e:
        logger.warning(f"[normalize] Failed to convert value to string: {e}")
        return ""


def _norm_lower(x: Any) -> str:
    """Normalize string: strip + lower, safe."""
    return _s(x).lower()


def _dbg(debug: bool, msg: str) -> None:
    if debug:
        try:
            print(msg)
        except Exception:
            pass


def _prepare_allowed(allowed: Optional[Set[str]]) -> Optional[Set[str]]:
    """
    Normalize allowed set to lowercase strings.
    Keeps None as None.
    """
    if allowed is None:
        return None
    out: Set[str] = set()
    for v in allowed:
        vv = _norm_lower(v)
        if vv:
            out.add(vv)
    return out


def _validate_or_fallback(
    value: Any,
    *,
    allowed: Optional[Set[str]],
    allowed_sorted: Optional[List[str]],
    fallback: str,
    what: str,
    context: str,
    strict: bool,
    debug: bool,
) -> str:
    """
    Normalize + validate a value against an optional allow-list.
    If blank -> fallback.
    If not allowed -> fallback (or raise if strict).
    """
    if _is_blank(value):
        _dbg(debug, f"[normalize] {context}{what} blank -> fallback {fallback!r}")
        return fallback

    raw = _s(value)
    normalized = raw.lower()

    if allowed is not None and normalized not in allowed:
        allowed_list = allowed_sorted or sorted(allowed)
        msg = (
            f"[normalize] {context}{what} invalid value={value!r} "
            f"normalized={normalized!r} allowed={allowed_list}"
        )
        if strict:
            raise NormalizationError(msg)
        logger.warning(msg)
        _dbg(debug, msg + f" -> fallback {fallback!r}")
        return fallback

    # print only if normalization actually changed the string
    if debug and normalized != raw:
        _dbg(debug, f"[normalize] {context}{what} normalized {raw!r} -> {normalized!r}")

    return normalized


def normalize_profile_dict(
    item: Dict[str, Any],
    *,
    default_exchange: str,
    default_interval: str,
    allowed_exchanges: Optional[Set[str]] = None,
    allowed_intervals: Optional[Set[str]] = None,
    strict: bool = False,
    debug: bool = True,
    debug_summary: bool = True,
) -> Dict[str, Any]:
    """
    Normalize Notifier-UI profile JSON into Evaluator Profile.from_dict()-compatible dict.

    Key goals:
      - map condition_groups -> groups
      - fill blank exchange/interval fields with defaults (inherit)
      - fill blank left_*/right_* context fields from group exchange/interval
      - optionally validate exchange/interval against allow-lists (strict or fallback)
      - DO NOT write back to disk; returns a new dict.

    IMPORTANT:
      - tolerant by default (strict=False)
      - does NOT import schema.py (avoids coupling)
      - validation is optional via allowed_* sets
    """
    out: Dict[str, Any] = dict(item)

    # ---- normalize allow-lists once ----
    allowed_exchanges_n = _prepare_allowed(allowed_exchanges)
    allowed_intervals_n = _prepare_allowed(allowed_intervals)
    allowed_exchanges_sorted = sorted(allowed_exchanges_n) if allowed_exchanges_n is not None else None
    allowed_intervals_sorted = sorted(allowed_intervals_n) if allowed_intervals_n is not None else None

    # ---- groups key mapping ----
    if "groups" not in out and "condition_groups" in out:
        out["groups"] = out.get("condition_groups")
        _dbg(debug, "[normalize] mapped condition_groups -> groups")

    groups = out.get("groups") or []
    if not isinstance(groups, list):
        _dbg(debug, f"[normalize] WARN groups not list type={type(groups)}")
        return out

    # ---- hard fallbacks (if caller passes blanks) ----
    base_exchange = _norm_lower(default_exchange) or "binance"
    base_interval = _norm_lower(default_interval) or "1h"

    _dbg(debug, f"[normalize] defaults exchange={base_exchange!r} interval={base_interval!r} strict={strict}")

    new_groups: List[Any] = []
    patched_groups = 0
    patched_rows = 0
    total_rows = 0

    # compact summary: (context, key, before, after)
    changes: List[Tuple[str, str, Any, Any]] = []

    for gi, g in enumerate(groups):
        if not isinstance(g, dict):
            new_groups.append(g)
            continue

        gg: Dict[str, Any] = dict(g)
        gid = gg.get("gid") or gg.get("id") or f"#{gi}"
        gctx = f"group {gid} "

        # ----- group exchange/interval: validate + inherit -----
        g_ex_before = gg.get("exchange")
        g_it_before = gg.get("interval")

        gg["exchange"] = _validate_or_fallback(
            gg.get("exchange"),
            allowed=allowed_exchanges_n,
            allowed_sorted=allowed_exchanges_sorted,
            fallback=base_exchange,
            what="exchange",
            context=gctx,
            strict=strict,
            debug=debug,
        )
        gg["interval"] = _validate_or_fallback(
            gg.get("interval"),
            allowed=allowed_intervals_n,
            allowed_sorted=allowed_intervals_sorted,
            fallback=base_interval,
            what="interval",
            context=gctx,
            strict=strict,
            debug=debug,
        )

        if (g_ex_before != gg.get("exchange")):
            changes.append((gctx.rstrip() + ":", "exchange", g_ex_before, gg.get("exchange")))
        if (g_it_before != gg.get("interval")):
            changes.append((gctx.rstrip() + ":", "interval", g_it_before, gg.get("interval")))

        if (g_ex_before != gg.get("exchange")) or (g_it_before != gg.get("interval")):
            patched_groups += 1
            _dbg(
                debug,
                f"[normalize] {gctx}patched exchange={g_ex_before!r}->{gg.get('exchange')!r} "
                f"interval={g_it_before!r}->{gg.get('interval')!r}",
            )

        # ---- rows live here in your JSON (legacy uses 'conditions') ----
        rows = gg.get("conditions")
        if not isinstance(rows, list):
            _dbg(debug, f"[normalize] WARN {gctx}has no list 'conditions' key")
            new_groups.append(gg)
            continue

        new_rows: List[Any] = []
        for ri, r in enumerate(rows):
            if not isinstance(r, dict):
                new_rows.append(r)
                continue

            rr: Dict[str, Any] = dict(r)
            total_rows += 1
            rid = rr.get("rid") or rr.get("id") or f"#{ri}"
            rctx = f"group {gid} row {rid} "

            before = (
                rr.get("left_exchange"),
                rr.get("left_interval"),
                rr.get("right_exchange"),
                rr.get("right_interval"),
                rr.get("exchange"),
                rr.get("interval"),
            )

            # left/right specific overrides inherit from group
            rr["left_exchange"] = _validate_or_fallback(
                rr.get("left_exchange"),
                allowed=allowed_exchanges_n,
                allowed_sorted=allowed_exchanges_sorted,
                fallback=gg["exchange"],
                what="left_exchange",
                context=rctx,
                strict=strict,
                debug=debug,
            )
            rr["right_exchange"] = _validate_or_fallback(
                rr.get("right_exchange"),
                allowed=allowed_exchanges_n,
                allowed_sorted=allowed_exchanges_sorted,
                fallback=gg["exchange"],
                what="right_exchange",
                context=rctx,
                strict=strict,
                debug=debug,
            )
            rr["left_interval"] = _validate_or_fallback(
                rr.get("left_interval"),
                allowed=allowed_intervals_n,
                allowed_sorted=allowed_intervals_sorted,
                fallback=gg["interval"],
                what="left_interval",
                context=rctx,
                strict=strict,
                debug=debug,
            )
            rr["right_interval"] = _validate_or_fallback(
                rr.get("right_interval"),
                allowed=allowed_intervals_n,
                allowed_sorted=allowed_intervals_sorted,
                fallback=gg["interval"],
                what="right_interval",
                context=rctx,
                strict=strict,
                debug=debug,
            )

            # generic fallbacks (harmless, but handy for downstream code)
            rr["exchange"] = _validate_or_fallback(
                rr.get("exchange"),
                allowed=allowed_exchanges_n,
                allowed_sorted=allowed_exchanges_sorted,
                fallback=gg["exchange"],
                what="exchange",
                context=rctx,
                strict=strict,
                debug=debug,
            )
            rr["interval"] = _validate_or_fallback(
                rr.get("interval"),
                allowed=allowed_intervals_n,
                allowed_sorted=allowed_intervals_sorted,
                fallback=gg["interval"],
                what="interval",
                context=rctx,
                strict=strict,
                debug=debug,
            )

            after = (
                rr.get("left_exchange"),
                rr.get("left_interval"),
                rr.get("right_exchange"),
                rr.get("right_interval"),
                rr.get("exchange"),
                rr.get("interval"),
            )

            if before != after:
                patched_rows += 1
                _dbg(
                    debug,
                    f"[normalize] {rctx}patched "
                    f"left_ex={before[0]!r}->{after[0]!r} left_it={before[1]!r}->{after[1]!r} "
                    f"right_ex={before[2]!r}->{after[2]!r} right_it={before[3]!r}->{after[3]!r} "
                    f"ex={before[4]!r}->{after[4]!r} it={before[5]!r}->{after[5]!r}",
                )

                # store change details (only for keys that actually changed)
                keys = ("left_exchange", "left_interval", "right_exchange", "right_interval", "exchange", "interval")
                for idx, k in enumerate(keys):
                    if before[idx] != after[idx]:
                        changes.append((rctx.rstrip() + ":", k, before[idx], after[idx]))

            new_rows.append(rr)

        gg["conditions"] = new_rows
        new_groups.append(gg)

    out["groups"] = new_groups

    _dbg(
        debug,
        f"[normalize] done groups={len(new_groups)} patched_groups={patched_groups} "
        f"rows={total_rows} patched_rows={patched_rows}",
    )

    if debug and debug_summary and changes:
        _dbg(debug, f"[normalize] summary changes={len(changes)}")
        # keep summary readable: show first N and then count remaining
        max_lines = 50
        for i, (ctx, key, before, after) in enumerate(changes[:max_lines]):
            _dbg(debug, f"[normalize]  - {ctx} {key}: {before!r} -> {after!r}")
        if len(changes) > max_lines:
            _dbg(debug, f"[normalize]  ... +{len(changes) - max_lines} more")

    return out
