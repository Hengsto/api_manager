# notifier_evaluator/eval/engine.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

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
from notifier_evaluator.models.schema import EngineDefaults, Group, Profile
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
        run_id = f"run_{int(time.time()*1000)}"
        print("[engine] START %s profiles=%d" % (run_id, len(profiles or [])))

        self.cache.reset_run_cache()
        summary = RunSummary(profiles=len(profiles or []))

        # Collect all planned unique RequestKeys across the whole run (global dedupe)
        global_unique: Dict[RequestKey, None] = {}
        # Map for each evaluation unit: (profile_id,gid,base_symbol) -> plan result
        unit_plans: Dict[Tuple[str, str, str], Tuple[List[RequestKey], Dict[Tuple[str, str, str, str, str], RequestKey], Group, Profile, Dict[str, ResolvedPair]]] = {}

        # --- PLAN PHASE ---
        for profile in profiles or []:
            if not profile.enabled:
                print("[engine] skip disabled profile=%s" % profile.profile_id)
                continue

            for group in profile.groups or []:
                summary.groups += 1
                if not group.enabled:
                    print("[engine] skip disabled group=%s profile=%s" % (group.gid, profile.profile_id))
                    continue

                # expand symbols
                exp = self.group_expander.expand_group(group)
                symbols = exp.symbols
                summary.symbols += len(symbols)

                print("[engine] profile=%s gid=%s expanded_symbols=%d" % (profile.profile_id, group.gid, len(symbols)))

                for base_symbol in symbols:
                    if not base_symbol:
                        continue

                    # Resolve per row contexts
                    resolved_pairs: Dict[str, ResolvedPair] = {}
                    for cond in group.rows or []:
                        if not cond.enabled:
                            continue
                        pair, dbg = resolve_contexts(
                            profile=profile,
                            group=group,
                            cond=cond,
                            defaults=self.cfg.defaults,
                            base_symbol=base_symbol,
                        )
                        resolved_pairs[cond.rid] = pair

                    # Plan requests for this evaluation unit
                    plan = plan_requests_for_symbol(
                        profile_id=profile.profile_id,
                        gid=group.gid,
                        base_symbol=base_symbol,
                        rows=group.rows or [],
                        resolved_pairs=resolved_pairs,
                        mode=self.cfg.request_mode,
                        as_of=self.cfg.request_as_of,
                    )
                    summary.rows += plan.debug.get("rows", 0)
                    summary.unique_requests += plan.debug.get("unique", 0)

                    for k in plan.unique_keys:
                        global_unique[k] = None

                    unit_plans[(profile.profile_id, group.gid, base_symbol)] = (
                        plan.unique_keys,
                        plan.row_map,
                        group,
                        profile,
                        resolved_pairs,
                    )

        # --- FETCH PHASE (global unique) ---
        fetch_results: Dict[RequestKey, FetchResult] = {}

        def _fetch_fn(k: RequestKey) -> FetchResult:
            return self.client.fetch_indicator(k)

        print("[engine] FETCH global_unique=%d" % len(global_unique))
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

        for (profile_id, gid, base_symbol), (unit_keys, row_map, group, profile, resolved_pairs) in unit_plans.items():
            # status key uses clock_interval (take from any pair; fallback to group.interval/defaults)
            clock_interval = None
            if resolved_pairs:
                any_pair = next(iter(resolved_pairs.values()))
                clock_interval = any_pair.left.clock_interval
            else:
                clock_interval = group.interval or profile.default_interval or self.cfg.defaults.clock_interval or self.cfg.defaults.interval or ""

            exchange = group.exchange or profile.default_exchange or self.cfg.defaults.exchange or ""

            skey = StatusKey(
                profile_id=profile_id,
                gid=gid,
                symbol=base_symbol,
                exchange=exchange,
                clock_interval=clock_interval,
            )

            # load status
            st = self.store.load_status(skey)

            # if inactive -> still update last states? depends; for now: skip everything but keep it quiet
            if not st.active:
                print("[engine] SKIP inactive profile=%s gid=%s sym=%s" % (profile_id, gid, base_symbol))
                continue

            # evaluate all rows in order
            cond_results = []
            logic_to_prev = ["and"]  # dummy for index 0
            last_row_left = None
            last_row_right = None
            last_row_op = None

            for cond in group.rows or []:
                if not cond.enabled:
                    continue
                pair = resolved_pairs.get(cond.rid)
                if pair is None:
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
                logic_to_prev.append((cond.logic_to_prev or "and").strip().lower())

                # keep last row values for push formatting/debug
                last_row_left = cr.left_value
                last_row_right = cr.right_value
                last_row_op = cr.op

            chain = eval_chain(cond_results, logic_to_prev=logic_to_prev)

            # choose tick ts: simplest: use the max/latest of LEFT timestamp from any row
            # Better later: fetch a dedicated "clock" candle series. For now: best-effort.
            tick_ts = None
            for cond in group.rows or []:
                if not cond.enabled:
                    continue
                rid = cond.rid
                mk_left = (profile_id, gid, rid, base_symbol, "left")
                k_left = row_map.get(mk_left)
                if not k_left:
                    continue
                fr = fetch_results.get(k_left)
                if fr and fr.latest_ts:
                    tick_ts = fr.latest_ts  # last wins; ok for now
            tick_res = detect_new_tick(skey=skey, state=st, current_tick_ts=tick_ts)

            thr = apply_threshold(
                final_state=chain.final_state,
                new_tick=tick_res.new_tick,
                cfg=group.threshold,
                state=st,
                now_ts=now_ts,
            )

            # Decide push based on threshold passed
            pol = apply_alarm_policy(
                skey=skey,
                state=st,
                cfg=group.alarm,
                now_ts=now_ts,
                now_unix=now_unix,
                partial_true=chain.partial_true,
                final_state=chain.final_state,
                threshold_passed=thr.passed,
                last_row_left=last_row_left,
                last_row_right=last_row_right,
                last_row_op=last_row_op,
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
                        "mode": group.threshold.mode,
                        "passed": thr.passed,
                        "streak_current": st.streak_current,
                        "count_window": list(st.count_window),
                        "new_tick": tick_res.new_tick,
                        "tick_ts": tick_res.tick_ts,
                    },
                    debug={
                        "tick_reason": tick_res.reason,
                        "threshold_reason": thr.reason,
                        "policy_reason": pol.push_reason,
                        "policy": pol.debug,
                        "chain": chain.debug,
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
            "[engine] END %s profiles=%d groups=%d symbols=%d rows=%d unique_requests=%d pushes=%d"
            % (run_id, summary.profiles, summary.groups, summary.symbols, summary.rows, summary.unique_requests, summary.pushes)
        )
        return summary
