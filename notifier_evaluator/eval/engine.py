# notifier_evaluator/eval/engine.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from notifier_evaluator.alarms.policy import apply_alarm_policy
from notifier_evaluator.context.group_expander import TTLGroupExpander
from notifier_evaluator.context.resolver import resolve_contexts
from notifier_evaluator.context.tick import detect_new_tick
from notifier_evaluator.eval.chain_eval import eval_chain
from notifier_evaluator.eval.condition_eval import eval_condition_row
from notifier_evaluator.eval.threshold import apply_threshold
from notifier_evaluator.fetch.cache import FetchCache
from notifier_evaluator.fetch.client import IndicatorClient
from notifier_evaluator.fetch.planner import plan_requests_for_symbol
from notifier_evaluator.fetch.types import RequestKey
from notifier_evaluator.models.schema import AlarmConfig, EngineDefaults, Group, Profile, ThresholdConfig
from notifier_evaluator.models.runtime import (
    FetchResult,
    HistoryEvent,
    ResolvedPair,
    StatusKey,
    StatusState,
    TriState,
)
from notifier_evaluator.state.store import StateStore, StoreCommit


# ──────────────────────────────────────────────────────────────────────────────
# Engine Orchestrator
#
# Flow:
# 1) expand symbols
# 2) for each (profile,group,symbol):
#    a) resolve contexts per row (left/right)
#    b) plan unique requests + row_map
# 3) fetch all unique keys (dedupe via cache)
# 4) eval rows -> ConditionResults
# 5) chain eval -> partial_true + final_state
# 6) tick detect (clock interval) -> new_tick
# 7) threshold apply (tick gated) -> threshold_passed
# 8) alarm policy -> events + active transitions
# 9) commit status updates + history events (atomic)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class EngineConfig:
    defaults: EngineDefaults
    fetch_ttl_sec: int = 5
    group_expand_ttl_sec: int = 10

    # planner: as_of mode (optional)
    request_mode: str = "latest"
    request_as_of: Optional[str] = None


@dataclass
class RunSummary:
    profiles: int = 0
    groups: int = 0
    symbols: int = 0
    rows: int = 0
    unique_requests: int = 0
    fetch_ok: int = 0
    fetch_fail: int = 0
    pushes: int = 0
    events: int = 0
    status_updates: int = 0


@dataclass
class UnitPlan:
    profile_id: str
    gid: str
    base_symbol: str
    group: Group
    profile: Profile
    resolved_pairs: Dict[str, ResolvedPair]
    unique_keys: List[RequestKey]
    row_map: Dict[Tuple[str, str, str, str, str], RequestKey]


def _safe_strip(x: Optional[str]) -> str:
    return (str(x).strip() if x is not None else "").strip()


def _normalize_logic(x: Optional[str]) -> str:
    s = _safe_strip(x).lower()
    if s in ("and", "or"):
        return s
    if s:
        print(f"[engine] WARN invalid logic_to_prev='{s}' -> default AND")
    return "and"


# ──────────────────────────────────────────────────────────────────────────────
# Schema drift adapters (NEW Notifier JSON vs Legacy Evaluator expectations)
# - profile: enabled vs active
# - group: enabled vs active
# - group rows: rows vs conditions
# - group threshold/alarm: legacy expects group.threshold/group.alarm but NEW stores:
#     - threshold per condition (row) (so group threshold defaults to "none")
#     - group.deactivate_on describes alarm mode ("auto_off", "always_on", ...)
# ──────────────────────────────────────────────────────────────────────────────


def _profile_is_enabled(p: Profile) -> bool:
    v = getattr(p, "enabled", None)
    if v is None:
        v = getattr(p, "active", True)
    try:
        return bool(v)
    except Exception:
        return True


def _group_is_enabled(g: Group) -> bool:
    v = getattr(g, "enabled", None)
    if v is None:
        v = getattr(g, "active", True)
    try:
        return bool(v)
    except Exception:
        return True


def _group_rows(g: Group) -> List:
    rows = getattr(g, "rows", None)
    if rows is None:
        rows = getattr(g, "conditions", None)
    return list(rows or [])


def _group_threshold_cfg(g: Group) -> ThresholdConfig:
    """
    Legacy engine expects group.threshold.
    NEW schema has threshold per condition -> group-level threshold defaults to NONE.
    """
    t = getattr(g, "threshold", None)
    if t is None:
        # default = no threshold gating
        try:
            return ThresholdConfig(mode="none")  # type: ignore[call-arg]
        except Exception:
            # in case model uses different field names, fallback to empty constructor
            return ThresholdConfig()  # type: ignore[call-arg]
    return t


def _group_alarm_cfg(g: Group) -> AlarmConfig:
    """
    Legacy engine expects group.alarm.
    NEW schema stores alarm intent as group.deactivate_on (e.g. "auto_off") + other fields.
    """
    a = getattr(g, "alarm", None)
    if a is not None:
        return a

    mode = getattr(g, "deactivate_on", None) or "always_on"
    # normalize common variants
    mode_s = str(mode).strip().lower()
    if mode_s in ("autooff", "auto-off"):
        mode_s = "auto_off"
    if mode_s in ("alwayson", "always-on"):
        mode_s = "always_on"
    if mode_s in ("pre", "pre_notification", "pre-notification"):
        mode_s = "pre_notification"

    try:
        # sensible defaults; you can wire telegram_id later in apply_alarm_policy
        return AlarmConfig(mode=mode_s, cooldown_sec=0, edge_only=True)  # type: ignore[call-arg]
    except Exception:
        return AlarmConfig()  # type: ignore[call-arg]


def _build_logic_to_prev_from_conditions(conds_in_eval_order: List, n_results: int) -> List[str]:
    """
    Build logic_to_prev list aligned with results.
    - length == n_results
    - index 0 is ignored by chain_eval, but we keep it as "<start>"
    - for i>=1, logic_to_prev[i] is conds_in_eval_order[i].logic_to_prev normalized
    """
    logic: List[str] = []
    if n_results <= 0:
        return logic

    # index 0 ignored by chain_eval (documented), but keep a marker
    logic.append("<start>")

    # for each subsequent row, use that row's logic_to_prev
    for i in range(1, n_results):
        try:
            cond = conds_in_eval_order[i]
            logic.append(_normalize_logic(getattr(cond, "logic_to_prev", None)))
        except Exception as e:
            print(f"[engine] WARN build_logic_to_prev failed at i={i} err={e} -> default AND")
            logic.append("and")

    return logic


def _pick_tick_ts_for_unit(
    *,
    profile_id: str,
    gid: str,
    base_symbol: str,
    group_rows: List,
    row_map: Dict[Tuple[str, str, str, str, str], RequestKey],
    fetch_results: Dict[RequestKey, FetchResult],
    clock_interval: str,
) -> Tuple[Optional[str], Dict[str, object]]:
    """
    Best-effort tick timestamp selection.

    Strategy:
    1) Prefer latest_ts from any request whose *data interval* == clock_interval (LEFT or RIGHT).
       (This is the closest approximation we have without dedicated candle clock fetching.)
    2) Fallback: last seen LEFT latest_ts in row order (legacy behavior).
    """
    dbg: Dict[str, object] = {
        "clock_interval": clock_interval,
        "picked": None,
        "reason": None,
        "candidates": 0,
    }

    clock_interval = _safe_strip(clock_interval)

    # collect candidates where request interval matches clock_interval
    candidates: List[Tuple[str, str]] = []  # (latest_ts, key_short)
    for cond in group_rows or []:
        if not getattr(cond, "enabled", True):
            continue
        rid = getattr(cond, "rid", None)
        if not rid:
            continue

        for side in ("left", "right"):
            mk = (profile_id, gid, rid, base_symbol, side)
            k = row_map.get(mk)
            if not k:
                continue

            fr = fetch_results.get(k)
            if not fr or not fr.latest_ts:
                continue

            # We try to match the clock interval on the RequestKey context interval
            try:
                k_interval = _safe_strip(getattr(getattr(k, "ctx", None), "interval", None))
            except Exception:
                k_interval = ""

            if clock_interval and k_interval and (k_interval == clock_interval):
                candidates.append((str(fr.latest_ts), k.short()))

    dbg["candidates"] = len(candidates)

    if candidates:
        # pick max by string (works if ISO timestamps are consistent). If not consistent, we still at least pick one.
        # We also print the top few for debugging.
        candidates_sorted = sorted(candidates, key=lambda x: x[0])
        picked_ts, picked_key = candidates_sorted[-1]
        dbg["picked"] = picked_ts
        dbg["reason"] = "interval_match"
        dbg["picked_key"] = picked_key
        if len(candidates_sorted) > 1:
            dbg["top2"] = candidates_sorted[-2:]
        return picked_ts, dbg

    # fallback: legacy "last left wins"
    tick_ts = None
    last_key_short = None
    for cond in group_rows or []:
        if not getattr(cond, "enabled", True):
            continue
        rid = getattr(cond, "rid", None)
        if not rid:
            continue
        mk_left = (profile_id, gid, rid, base_symbol, "left")
        k_left = row_map.get(mk_left)
        if not k_left:
            continue
        fr = fetch_results.get(k_left)
        if fr and fr.latest_ts:
            tick_ts = fr.latest_ts
            last_key_short = k_left.short()

    dbg["picked"] = tick_ts
    dbg["reason"] = "fallback_last_left"
    dbg["picked_key"] = last_key_short
    return (str(tick_ts).strip() if tick_ts is not None else None), dbg


class EvaluatorEngine:
    def __init__(
        self,
        *,
        cfg: EngineConfig,
        store: StateStore,
        group_expander: TTLGroupExpander,
        client: IndicatorClient,
        fetch_cache: Optional[FetchCache] = None,
    ):
        self.cfg = cfg
        self.store = store
        self.group_expander = group_expander
        self.client = client
        self.cache = fetch_cache or FetchCache(ttl_sec=cfg.fetch_ttl_sec)

    def run(self, profiles: List[Profile]) -> RunSummary:
        run_id = f"run_{int(time.time() * 1000)}"
        print("[engine] START %s profiles=%d" % (run_id, len(profiles or [])))

        self.cache.reset_run_cache()
        summary = RunSummary(profiles=len(profiles or []))

        # Collect all planned unique RequestKeys across the whole run (global dedupe)
        global_unique: Dict[RequestKey, None] = {}

        # Per evaluation unit plan
        unit_plans: Dict[Tuple[str, str, str], UnitPlan] = {}

        # --- PLAN PHASE ---
        for profile in profiles or []:
            if not _profile_is_enabled(profile):
                print("[engine] skip disabled profile=%s" % getattr(profile, "profile_id", "<no-profile-id>"))
                continue

            for group in profile.groups or []:
                summary.groups += 1

                if not _group_is_enabled(group):
                    print(
                        "[engine] skip disabled group=%s profile=%s"
                        % (getattr(group, "gid", "<no-gid>"), getattr(profile, "profile_id", "<no-profile-id>"))
                    )
                    continue

                # DEBUG: schema drift visibility
                print(
                    "[engine][DBG] group gid=%s has_enabled=%s has_active=%s has_rows=%s has_conditions=%s has_alarm=%s has_deactivate_on=%s has_threshold=%s"
                    % (
                        getattr(group, "gid", "<no-gid>"),
                        hasattr(group, "enabled"),
                        hasattr(group, "active"),
                        hasattr(group, "rows"),
                        hasattr(group, "conditions"),
                        hasattr(group, "alarm"),
                        hasattr(group, "deactivate_on"),
                        hasattr(group, "threshold"),
                    )
                )

                # expand symbols
                exp = self.group_expander.expand_group(group)
                symbols = exp.symbols
                summary.symbols += len(symbols)

                print(
                    "[engine] profile=%s gid=%s expanded_symbols=%d version=%s"
                    % (profile.profile_id, group.gid, len(symbols), getattr(exp, "version_key", "<no-version>"))
                )

                for base_symbol in symbols:
                    base_symbol = _safe_strip(base_symbol)
                    if not base_symbol:
                        continue

                    # Resolve per row contexts
                    resolved_pairs: Dict[str, ResolvedPair] = {}
                    resolved_n = 0
                    for cond in _group_rows(group):
                        if not getattr(cond, "enabled", True):
                            continue
                        pair, dbg = resolve_contexts(
                            profile=profile,
                            group=group,
                            cond=cond,
                            defaults=self.cfg.defaults,
                            base_symbol=base_symbol,
                        )
                        resolved_pairs[getattr(cond, "rid", "<no-rid>")] = pair
                        resolved_n += 1

                    print(
                        "[engine] PLAN resolve profile=%s gid=%s sym=%s resolved_pairs=%d"
                        % (profile.profile_id, group.gid, base_symbol, resolved_n)
                    )

                    # Plan requests for this evaluation unit
                    _rows = _group_rows(group)
                    plan = plan_requests_for_symbol(
                        profile_id=profile.profile_id,
                        gid=group.gid,
                        base_symbol=base_symbol,
                        rows=_rows,
                        resolved_pairs=resolved_pairs,
                        mode=self.cfg.request_mode,
                        as_of=self.cfg.request_as_of,
                    )

                    summary.rows += plan.debug.get("rows", 0)

                    # global unique
                    before = len(global_unique)
                    for k in plan.unique_keys:
                        global_unique[k] = None
                    after = len(global_unique)

                    print(
                        "[engine] PLAN unit profile=%s gid=%s sym=%s rows=%d unit_unique=%d global_unique %d->%d"
                        % (
                            profile.profile_id,
                            group.gid,
                            base_symbol,
                            plan.debug.get("rows", 0),
                            plan.debug.get("unique", 0),
                            before,
                            after,
                        )
                    )

                    unit_plans[(profile.profile_id, group.gid, base_symbol)] = UnitPlan(
                        profile_id=profile.profile_id,
                        gid=group.gid,
                        base_symbol=base_symbol,
                        group=group,
                        profile=profile,
                        resolved_pairs=resolved_pairs,
                        unique_keys=plan.unique_keys,
                        row_map=plan.row_map,
                    )

        # Correct global unique_requests count (fix: previously was summed per-unit)
        summary.unique_requests = len(global_unique)

        # --- FETCH PHASE (global unique) ---
        fetch_results: Dict[RequestKey, FetchResult] = {}

        def _fetch_fn(k: RequestKey) -> FetchResult:
            return self.client.fetch_indicator(k)

        print("[engine] FETCH global_unique=%d ttl=%ds" % (len(global_unique), int(self.cfg.fetch_ttl_sec)))
        for k in global_unique.keys():
            fr = self.cache.get_or_fetch(k, _fetch_fn)
            fetch_results[k] = fr
            if fr.ok:
                summary.fetch_ok += 1
            else:
                summary.fetch_fail += 1

        print("[engine] FETCH DONE ok=%d fail=%d cache=%s" % (summary.fetch_ok, summary.fetch_fail, self.cache.summary()))

        # --- EVAL PHASE + COMMIT COLLECTION ---
        status_updates: Dict[StatusKey, StatusState] = {}
        history_events: List[HistoryEvent] = []

        now_unix = time.time()
        now_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now_unix))  # UTC-ish; replace if you want tz aware

        for (profile_id, gid, base_symbol), up in unit_plans.items():
            group = up.group
            profile = up.profile
            resolved_pairs = up.resolved_pairs
            row_map = up.row_map
            rows_for_group = _group_rows(group)

            # status key uses clock_interval (take from any pair; fallback to group.interval/defaults)
            clock_interval = ""
            if resolved_pairs:
                any_pair = next(iter(resolved_pairs.values()))
                clock_interval = any_pair.left.clock_interval
            else:
                clock_interval = (
                    getattr(group, "interval", None)
                    or getattr(profile, "default_interval", None)
                    or self.cfg.defaults.clock_interval
                    or self.cfg.defaults.interval
                    or ""
                )

            exchange = getattr(group, "exchange", None) or getattr(profile, "default_exchange", None) or self.cfg.defaults.exchange or ""

            skey = StatusKey(
                profile_id=profile_id,
                gid=gid,
                symbol=base_symbol,
                exchange=exchange,
                clock_interval=clock_interval,
            )

            print(
                "[engine] EVAL unit profile=%s gid=%s sym=%s ex=%s clock=%s rows_total=%d"
                % (profile_id, gid, base_symbol, exchange, clock_interval, len(rows_for_group))
            )

            # load status
            st = self.store.load_status(skey)

            # if inactive -> still update last states? depends; for now: skip everything but keep it quiet
            if not st.active:
                print("[engine] SKIP inactive profile=%s gid=%s sym=%s" % (profile_id, gid, base_symbol))
                continue

            # evaluate all rows in order
            cond_results: List = []
            conds_in_eval_order: List = []
            last_row_left = None
            last_row_right = None
            last_row_op = None

            for cond in rows_for_group:
                if not getattr(cond, "enabled", True):
                    continue
                rid = getattr(cond, "rid", None)
                if not rid:
                    print("[engine] WARN condition without rid profile=%s gid=%s sym=%s" % (profile_id, gid, base_symbol))
                    continue

                pair = resolved_pairs.get(rid)
                if pair is None:
                    print(
                        "[engine] WARN missing resolved_pair profile=%s gid=%s sym=%s rid=%s"
                        % (profile_id, gid, base_symbol, rid)
                    )
                    continue

                cr = eval_condition_row(
                    profile_id=profile_id,
                    gid=gid,
                    base_symbol=base_symbol,
                    cond=cond,
                    pair=pair,
                    row_map=row_map,
                    fetch_results=fetch_results,
                )
                cond_results.append(cr)
                conds_in_eval_order.append(cond)

                # keep last row values for push formatting/debug
                last_row_left = cr.left_value
                last_row_right = cr.right_value
                last_row_op = cr.op

            # FIX: logic_to_prev aligned with results (no off-by-one)
            logic_to_prev = _build_logic_to_prev_from_conditions(conds_in_eval_order, len(cond_results))

            if logic_to_prev and (len(logic_to_prev) != len(cond_results)):
                print(
                    "[engine] WARN logic_to_prev length mismatch profile=%s gid=%s sym=%s logic=%d results=%d"
                    % (profile_id, gid, base_symbol, len(logic_to_prev), len(cond_results))
                )

            chain = eval_chain(cond_results, logic_to_prev=logic_to_prev)

            # choose tick ts: best-effort selection guided by clock_interval
            tick_ts, tick_dbg = _pick_tick_ts_for_unit(
                profile_id=profile_id,
                gid=gid,
                base_symbol=base_symbol,
                group_rows=rows_for_group,
                row_map=row_map,
                fetch_results=fetch_results,
                clock_interval=clock_interval,
            )

            print(
                "[engine] TICK_PICK profile=%s gid=%s sym=%s picked=%s reason=%s candidates=%s key=%s"
                % (
                    profile_id,
                    gid,
                    base_symbol,
                    tick_dbg.get("picked"),
                    tick_dbg.get("reason"),
                    tick_dbg.get("candidates"),
                    tick_dbg.get("picked_key"),
                )
            )

            tick_res = detect_new_tick(skey=skey, state=st, current_tick_ts=tick_ts)

            # NEW schema: group threshold is not a thing -> default ThresholdConfig(mode="none")
            g_thr = _group_threshold_cfg(group)
            thr = apply_threshold(
                final_state=chain.final_state,
                new_tick=tick_res.new_tick,
                cfg=g_thr,
                state=st,
                now_ts=now_ts,
            )

            # NEW schema: group alarm config can come from deactivate_on
            g_alarm = _group_alarm_cfg(group)

            # Decide push based on threshold passed
            pol = apply_alarm_policy(
                skey=skey,
                state=st,
                cfg=g_alarm,
                now_ts=now_ts,
                now_unix=now_unix,
                partial_true=chain.partial_true,
                final_state=chain.final_state,
                threshold_passed=thr.passed,
                last_row_left=last_row_left,
                last_row_right=last_row_right,
                last_row_op=last_row_op,
            )

            print(
                "[engine] DECISION profile=%s gid=%s sym=%s partial=%s final=%s tick_new=%s thr_passed=%s push=%s reason=%s"
                % (
                    profile_id,
                    gid,
                    base_symbol,
                    chain.partial_true,
                    chain.final_state.value,
                    tick_res.new_tick,
                    thr.passed,
                    pol.push,
                    pol.push_reason,
                )
            )

            # History: always store a compact eval snapshot (optional but super helpful)
            history_events.append(
                HistoryEvent(
                    ts=now_ts,
                    profile_id=profile_id,
                    gid=gid,
                    symbol=base_symbol,
                    exchange=exchange,
                    event="eval",
                    partial_true=chain.partial_true,
                    final_state=chain.final_state.value,
                    left_value=last_row_left,
                    right_value=last_row_right,
                    op=last_row_op,
                    threshold_snapshot={
                        "mode": getattr(g_thr, "mode", None),
                        "passed": thr.passed,
                        "streak_current": st.streak_current,
                        "count_window": list(st.count_window),
                        "new_tick": tick_res.new_tick,
                        "tick_ts": tick_res.tick_ts,
                        "tick_pick": tick_dbg,
                    },
                    debug={
                        "tick_reason": tick_res.reason,
                        "threshold_reason": thr.reason,
                        "policy_reason": pol.push_reason,
                        "policy": pol.debug,
                        "chain": chain.debug,
                        "logic_to_prev": logic_to_prev,
                        "alarm_mode": getattr(g_alarm, "mode", None),
                    },
                )
            )

            # include policy events (push, deactivated, partial change)
            history_events.extend(pol.events)

            if pol.push:
                summary.pushes += 1

            # collect status update
            status_updates[skey] = st

        # --- COMMIT ---
        summary.events = len(history_events)
        summary.status_updates = len(status_updates)

        print("[engine] COMMIT status_updates=%d history_events=%d" % (len(status_updates), len(history_events)))
        self.store.commit(StoreCommit(status_updates=status_updates, history_events=history_events))

        print(
            "[engine] END %s profiles=%d groups=%d symbols=%d rows=%d global_unique_requests=%d pushes=%d"
            % (
                run_id,
                summary.profiles,
                summary.groups,
                summary.symbols,
                summary.rows,
                summary.unique_requests,
                summary.pushes,
            )
        )
        return summary
