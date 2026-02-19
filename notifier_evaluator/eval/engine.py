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
from notifier_evaluator.models.schema import AlarmConfig, EngineDefaults, Group, Profile, ThresholdConfig
from notifier_evaluator.models.runtime import FetchResult, HistoryEvent, ResolvedPair, StatusKey, StatusState
from notifier_evaluator.state.store import StateStore, StoreCommit


@dataclass
class EngineConfig:
    defaults: EngineDefaults
    fetch_ttl_sec: int = 5
    group_expand_ttl_sec: int = 10
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
    resolved_pairs: Dict[str, ResolvedPair]
    unique_keys: List[RequestKey]
    row_map: Dict[Tuple[str, str, str, str, str], RequestKey]


def _safe_strip(x: Optional[str]) -> str:
    return (str(x).strip() if x is not None else "").strip()


def _alarm_from_group(group: Group) -> AlarmConfig:
    return AlarmConfig(mode=group.deactivate_on, cooldown_sec=0, edge_only=True)


def _logic_to_prev(conditions: List) -> List[str]:
    ops = ["<start>"]
    for cond in conditions[1:]:
        ops.append(cond.logic)
    return ops


def _pick_threshold_condition(conditions_in_eval_order: List) -> Tuple[Optional[ThresholdConfig], Optional[str]]:
    picked: Optional[ThresholdConfig] = None
    picked_rid: Optional[str] = None
    for cond in conditions_in_eval_order:
        if cond.threshold is not None:
            picked = cond.threshold
            picked_rid = cond.rid
    return picked, picked_rid


class EvaluatorEngine:
    def __init__(self, *, cfg: EngineConfig, store: StateStore, group_expander: TTLGroupExpander, client: IndicatorClient, fetch_cache: Optional[FetchCache] = None):
        self.cfg = cfg
        self.store = store
        self.group_expander = group_expander
        self.client = client
        self.cache = fetch_cache or FetchCache(ttl_sec=cfg.fetch_ttl_sec)

    def run(self, profiles: List[Profile]) -> RunSummary:
        print(f"[evaluator][DBG] engine start profiles={len(profiles or [])}")
        self.cache.reset_run_cache()
        summary = RunSummary(profiles=len(profiles or []))
        global_unique: Dict[RequestKey, None] = {}
        unit_plans: Dict[Tuple[str, str, str], UnitPlan] = {}

        for profile in profiles or []:
            if not profile.enabled:
                print(f"[evaluator][DBG] skip disabled profile id={profile.id}")
                continue
            for group in profile.groups or []:
                summary.groups += 1
                if not group.active:
                    print(f"[evaluator][DBG] skip inactive group profile={profile.id} gid={group.gid}")
                    continue

                exp = self.group_expander.expand_group(group)
                summary.symbols += len(exp.symbols)

                for base_symbol in exp.symbols:
                    resolved_pairs: Dict[str, ResolvedPair] = {}
                    for cond in group.conditions:
                        pair, _ = resolve_contexts(profile=profile, group=group, cond=cond, defaults=self.cfg.defaults, base_symbol=base_symbol)
                        resolved_pairs[cond.rid] = pair

                    plan = plan_requests_for_symbol(
                        profile_id=profile.id,
                        gid=group.gid,
                        base_symbol=base_symbol,
                        rows=group.conditions,
                        resolved_pairs=resolved_pairs,
                        mode=self.cfg.request_mode,
                        as_of=self.cfg.request_as_of,
                    )
                    summary.rows += plan.debug.get("rows", 0)
                    for k in plan.unique_keys:
                        global_unique[k] = None

                    unit_plans[(profile.id, group.gid, base_symbol)] = UnitPlan(
                        profile_id=profile.id,
                        gid=group.gid,
                        base_symbol=base_symbol,
                        group=group,
                        resolved_pairs=resolved_pairs,
                        unique_keys=plan.unique_keys,
                        row_map=plan.row_map,
                    )

        summary.unique_requests = len(global_unique)
        fetch_results: Dict[RequestKey, FetchResult] = {}
        for k in global_unique:
            fr = self.cache.get_or_fetch(k, self.client.fetch_indicator)
            fetch_results[k] = fr
            if fr.ok:
                summary.fetch_ok += 1
            else:
                summary.fetch_fail += 1

        status_updates: Dict[StatusKey, StatusState] = {}
        history_events: List[HistoryEvent] = []
        now_unix = time.time()
        now_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now_unix))

        for (profile_id, gid, base_symbol), up in unit_plans.items():
            group = up.group
            st = self.store.load_status(
                StatusKey(
                    profile_id=profile_id,
                    gid=gid,
                    symbol=base_symbol,
                    exchange=group.exchange or self.cfg.defaults.default_exchange,
                    clock_interval=group.interval,
                )
            )
            if not st.active:
                print(f"[evaluator][DBG] skip inactive status profile={profile_id} gid={gid} symbol={base_symbol}")
                continue

            cond_results = []
            eval_conditions = []
            last_row_left = None
            last_row_right = None
            last_row_op = None

            for cond in group.conditions:
                pair = up.resolved_pairs.get(cond.rid)
                if pair is None:
                    raise ValueError(f"missing resolved pair for rid={cond.rid} profile={profile_id} gid={gid}")
                cr = eval_condition_row(
                    profile_id=profile_id,
                    gid=gid,
                    base_symbol=base_symbol,
                    cond=cond,
                    pair=pair,
                    row_map=up.row_map,
                    fetch_results=fetch_results,
                )
                cond_results.append(cr)
                eval_conditions.append(cond)
                last_row_left = cr.left_value
                last_row_right = cr.right_value
                last_row_op = cr.op

            chain = eval_chain(cond_results, logic_to_prev=_logic_to_prev(eval_conditions))

            tick_ts = None
            if cond_results:
                first = eval_conditions[0]
                k_left = up.row_map.get((profile_id, gid, first.rid, base_symbol, "left"))
                if k_left and fetch_results.get(k_left):
                    tick_ts = fetch_results[k_left].latest_ts
            tick_res = detect_new_tick(
                skey=StatusKey(profile_id=profile_id, gid=gid, symbol=base_symbol, exchange=group.exchange or self.cfg.defaults.default_exchange, clock_interval=group.interval),
                state=st,
                current_tick_ts=tick_ts,
            )

            threshold_cfg, threshold_rid = _pick_threshold_condition(eval_conditions)
            threshold_target_state = chain.final_state
            if threshold_rid is not None:
                for cr in cond_results:
                    if cr.rid == threshold_rid:
                        threshold_target_state = cr.state
                        break
            thr = apply_threshold(final_state=threshold_target_state, new_tick=tick_res.new_tick, cfg=threshold_cfg, state=st, now_ts=now_ts)
            print(f"[evaluator][DBG] threshold_strategy=last_condition_with_threshold rid={threshold_rid} target_state={threshold_target_state.value} passed={thr.passed}")

            pol = apply_alarm_policy(
                skey=StatusKey(profile_id=profile_id, gid=gid, symbol=base_symbol, exchange=group.exchange or self.cfg.defaults.default_exchange, clock_interval=group.interval),
                state=st,
                cfg=_alarm_from_group(group),
                now_ts=now_ts,
                now_unix=now_unix,
                partial_true=chain.partial_true,
                final_state=chain.final_state,
                threshold_passed=thr.passed,
                last_row_left=last_row_left,
                last_row_right=last_row_right,
                last_row_op=last_row_op,
            )

            history_events.extend(pol.events)
            if pol.push:
                summary.pushes += 1
            status_updates[StatusKey(profile_id=profile_id, gid=gid, symbol=base_symbol, exchange=group.exchange or self.cfg.defaults.default_exchange, clock_interval=group.interval)] = st

        summary.events = len(history_events)
        summary.status_updates = len(status_updates)
        self.store.commit(StoreCommit(status_updates=status_updates, history_events=history_events))
        return summary
