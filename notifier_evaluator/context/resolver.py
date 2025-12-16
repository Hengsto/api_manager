# notifier_evaluator/context/resolver.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from notifier_evaluator.models.schema import Condition, Group, Profile, EngineDefaults
from notifier_evaluator.models.runtime import ResolvedContext, ResolvedPair


# ──────────────────────────────────────────────────────────────────────────────
# Kontextauflösung: row > group > profile > global(defaults)
#
# Ergebnis:
#   ResolvedPair(left_ctx, right_ctx)
#
# WICHTIG:
# - left/right haben eigene symbol/interval/exchange overrides
# - clock_interval ist die EVAL-CLOCK (Tick/Threshold) und MUSS eindeutig sein
#   -> standard: group.interval
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class ResolverDebug:
    profile_id: str
    gid: str
    rid: str
    base_symbol: str
    # raw values used (for debugging)
    left_symbol_src: str = ""
    right_symbol_src: str = ""
    left_interval_src: str = ""
    right_interval_src: str = ""
    exchange_src: str = ""
    clock_interval_src: str = ""


class ResolverError(RuntimeError):
    pass


def _strip(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = str(s).strip()
    return s2 or None


def _pick_first(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        vv = _strip(v)
        if vv:
            return vv
    return None


def _require(name: str, value: Optional[str], dbg: ResolverDebug) -> str:
    if value is None or str(value).strip() == "":
        raise ResolverError(
            f"[resolver] missing required '{name}' "
            f"(profile_id={dbg.profile_id} gid={dbg.gid} rid={dbg.rid} base_symbol={dbg.base_symbol})"
        )
    return value


def resolve_contexts(
    *,
    profile: Profile,
    group: Group,
    cond: Condition,
    defaults: EngineDefaults,
    base_symbol: str,
) -> Tuple[ResolvedPair, ResolverDebug]:
    """
    Resolves contexts for LEFT and RIGHT separately.

    Priority (hard):
      row overrides > group defaults > profile defaults > global defaults

    base_symbol:
      symbol currently being evaluated after group expansion.
      If row does not override symbol, LEFT/RIGHT default to base_symbol.

    Returns:
      (ResolvedPair, ResolverDebug)

    Raises:
      ResolverError if required symbol/interval/exchange/clock_interval cannot be resolved.
    """
    dbg = ResolverDebug(
        profile_id=profile.profile_id,
        gid=group.gid,
        rid=cond.rid,
        base_symbol=base_symbol,
    )

    # -------------------------
    # SYMBOL (per side)
    # -------------------------
    left_symbol = _pick_first(cond.left.symbol, base_symbol)
    right_symbol = _pick_first(cond.right.symbol, base_symbol)

    dbg.left_symbol_src = "row" if _strip(cond.left.symbol) else "base_symbol"
    dbg.right_symbol_src = "row" if _strip(cond.right.symbol) else "base_symbol"

    # -------------------------
    # INTERVAL (per side)
    # -------------------------
    # data interval for fetch (can differ between left and right)
    left_interval = _pick_first(
        cond.left.interval,
        group.interval,
        profile.default_interval,
        defaults.interval,
    )
    right_interval = _pick_first(
        cond.right.interval,
        group.interval,
        profile.default_interval,
        defaults.interval,
    )

    dbg.left_interval_src = (
        "row" if _strip(cond.left.interval) else
        "group" if _strip(group.interval) else
        "profile" if _strip(profile.default_interval) else
        "global"
    )
    dbg.right_interval_src = (
        "row" if _strip(cond.right.interval) else
        "group" if _strip(group.interval) else
        "profile" if _strip(profile.default_interval) else
        "global"
    )

    # -------------------------
    # EXCHANGE (shared default, but row may override per side)
    # -------------------------
    # NOTE: if you want exchange to be *strictly per side*, we support it here.
    left_exchange = _pick_first(
        cond.left.exchange,
        group.exchange,
        profile.default_exchange,
        defaults.exchange,
    )
    right_exchange = _pick_first(
        cond.right.exchange,
        group.exchange,
        profile.default_exchange,
        defaults.exchange,
    )

    # For debug: prefer most relevant source label (left/right could differ)
    if _strip(cond.left.exchange) or _strip(cond.right.exchange):
        dbg.exchange_src = "row"
    elif _strip(group.exchange):
        dbg.exchange_src = "group"
    elif _strip(profile.default_exchange):
        dbg.exchange_src = "profile"
    else:
        dbg.exchange_src = "global"

    # -------------------------
    # CLOCK INTERVAL (single, for tick/threshold)
    # -------------------------
    # Critical rule:
    # - clock interval is NOT left/right interval
    # - default: group.interval
    # - if group.interval missing, fall back profile.default_interval or defaults.clock_interval or defaults.interval
    clock_interval = _pick_first(
        group.interval,
        profile.default_interval,
        defaults.clock_interval,
        defaults.interval,
    )

    dbg.clock_interval_src = (
        "group" if _strip(group.interval) else
        "profile" if _strip(profile.default_interval) else
        "global_clock" if _strip(defaults.clock_interval) else
        "global_interval"
    )

    # -------------------------
    # REQUIRED checks (hard fail)
    # -------------------------
    left_symbol = _require("left_symbol", left_symbol, dbg)
    right_symbol = _require("right_symbol", right_symbol, dbg)

    left_interval = _require("left_interval", left_interval, dbg)
    right_interval = _require("right_interval", right_interval, dbg)

    left_exchange = _require("left_exchange", left_exchange, dbg)
    right_exchange = _require("right_exchange", right_exchange, dbg)

    clock_interval = _require("clock_interval", clock_interval, dbg)

    # -------------------------
    # Build contexts
    # -------------------------
    left_ctx = ResolvedContext(
        symbol=left_symbol,
        interval=left_interval,
        exchange=left_exchange,
        clock_interval=clock_interval,
    )
    right_ctx = ResolvedContext(
        symbol=right_symbol,
        interval=right_interval,
        exchange=right_exchange,
        clock_interval=clock_interval,
    )

    # Debug prints (extra noisy on purpose; you can gate later behind logger)
    print(
        "[resolver] profile=%s gid=%s rid=%s base_symbol=%s | "
        "LEFT(sym=%s[%s] int=%s[%s] ex=%s) | "
        "RIGHT(sym=%s[%s] int=%s[%s] ex=%s) | "
        "CLOCK=%s[%s]"
        % (
            profile.profile_id, group.gid, cond.rid, base_symbol,
            left_ctx.symbol, dbg.left_symbol_src,
            left_ctx.interval, dbg.left_interval_src,
            left_ctx.exchange,
            right_ctx.symbol, dbg.right_symbol_src,
            right_ctx.interval, dbg.right_interval_src,
            right_ctx.exchange,
            clock_interval, dbg.clock_interval_src,
        )
    )

    return ResolvedPair(left=left_ctx, right=right_ctx), dbg
