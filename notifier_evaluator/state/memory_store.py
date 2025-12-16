# notifier_evaluator/state/memory_store.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import asdict
from typing import Dict, Iterable, List, Optional

from notifier_evaluator.models.runtime import HistoryEvent, StatusKey, StatusState
from notifier_evaluator.state.store import StateStore, StoreCommit


# ──────────────────────────────────────────────────────────────────────────────
# In-memory Store (Tests/Dev)
# - commit() ist atomic (in-memory)
# ──────────────────────────────────────────────────────────────────────────────


class MemoryStore(StateStore):
    def __init__(self):
        self._status: Dict[StatusKey, StatusState] = {}
        self._history: List[HistoryEvent] = []

    # ---- STATUS ----

    def load_status(self, key: StatusKey) -> StatusState:
        st = self._status.get(key)
        if st is None:
            st = StatusState()
            self._status[key] = st
            print("[memory_store] init_status key=%s" % (key,))
        else:
            print("[memory_store] load_status key=%s active=%s" % (key, st.active))
        return st

    def load_status_batch(self, keys: Iterable[StatusKey]) -> Dict[StatusKey, StatusState]:
        out: Dict[StatusKey, StatusState] = {}
        n_init = 0
        n_hit = 0
        for k in keys:
            if k in self._status:
                out[k] = self._status[k]
                n_hit += 1
            else:
                out[k] = StatusState()
                self._status[k] = out[k]
                n_init += 1
        print("[memory_store] load_status_batch keys=%d hit=%d init=%d" % (len(list(keys)) if hasattr(keys, "__len__") else -1, n_hit, n_init))
        return out

    # ---- COMMIT ----

    def commit(self, commit: StoreCommit) -> None:
        su = commit.status_updates or {}
        he = commit.history_events or []

        print("[memory_store] COMMIT status_updates=%d history_events=%d" % (len(su), len(he)))

        # update statuses
        for k, st in su.items():
            self._status[k] = st
            print(
                "[memory_store] save_status key=%s active=%s streak=%s count_len=%s last_tick=%s last_push=%s"
                % (k, st.active, st.streak_current, len(st.count_window), st.last_tick_ts, st.last_push_ts)
            )

        # append history
        if he:
            self._history.extend(he)
            for e in he:
                print("[memory_store] history=%s" % asdict(e))

    # ---- HISTORY READ ----

    def load_history(self, profile_id: Optional[str] = None, limit: int = 200) -> List[HistoryEvent]:
        items = self._history
        if profile_id:
            items = [e for e in items if e.profile_id == profile_id]
        out = items[-max(1, int(limit)):]
        print("[memory_store] load_history profile=%s limit=%d -> %d" % (profile_id, limit, len(out)))
        return out

    # ---- UTIL ----

    def stats(self) -> Dict[str, int]:
        return {"status": len(self._status), "history": len(self._history)}
