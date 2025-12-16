# notifier_evaluator/state/json_store.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict
from typing import Dict, List, Optional

from notifier_evaluator.models.runtime import HistoryEvent, StatusKey, StatusState
from notifier_evaluator.state.store import StateStore, StoreCommit


# ──────────────────────────────────────────────────────────────────────────────
# JSON Store
# - atomic write (write tmp -> fsync -> replace)
# - in-process lock (threading)
#
# Files:
#   status.json:
#     { "<StatusKey str>": {StatusState dict}, ... }
#
#   history.json:
#     [ {HistoryEvent dict}, ... ]
#
# NOTE:
# - For multi-process you need OS file locks. This is in-process only.
# - Good enough for first version; upgrade later if evaluator runs in multiple processes.
# ──────────────────────────────────────────────────────────────────────────────


class JsonStore(StateStore):
    def __init__(self, *, status_path: str, history_path: str):
        self.status_path = status_path
        self.history_path = history_path
        self._lock = threading.Lock()

        os.makedirs(os.path.dirname(status_path) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(history_path) or ".", exist_ok=True)

        # ensure files exist
        if not os.path.exists(self.status_path):
            self._atomic_write_json(self.status_path, {})
        if not os.path.exists(self.history_path):
            self._atomic_write_json(self.history_path, [])

        print("[json_store] init status_path=%s history_path=%s" % (self.status_path, self.history_path))

    # ---- STATUS ----

    def load_status(self, key: StatusKey) -> StatusState:
        with self._lock:
            data = self._read_json(self.status_path, default={})
            sk = self._key_to_str(key)
            raw = data.get(sk)
            if not isinstance(raw, dict):
                st = StatusState()
                data[sk] = asdict(st)
                # write back (so status is materialized)
                self._atomic_write_json(self.status_path, data)
                print("[json_store] load_status init key=%s" % sk)
                return st

            st = self._dict_to_status(raw)
            print("[json_store] load_status key=%s active=%s" % (sk, st.active))
            return st

    def load_status_batch(self, keys):
        # Not required by Protocol at runtime for now; engine uses load_status()
        # Provided for completeness.
        out: Dict[StatusKey, StatusState] = {}
        for k in keys:
            out[k] = self.load_status(k)
        return out

    # ---- COMMIT ----

    def commit(self, commit: StoreCommit) -> None:
        su = commit.status_updates or {}
        he = commit.history_events or []

        with self._lock:
            status_data = self._read_json(self.status_path, default={})
            history_data = self._read_json(self.history_path, default=[])

            if not isinstance(status_data, dict):
                print("[json_store] WARN status_data not dict -> reset")
                status_data = {}
            if not isinstance(history_data, list):
                print("[json_store] WARN history_data not list -> reset")
                history_data = []

            # apply status updates
            for k, st in su.items():
                ks = self._key_to_str(k)
                status_data[ks] = asdict(st)

            # append history
            for e in he:
                history_data.append(asdict(e))

            # optional: cap history (keep last N)
            MAX_HIST = 5000
            if len(history_data) > MAX_HIST:
                history_data = history_data[-MAX_HIST:]

            self._atomic_write_json(self.status_path, status_data)
            self._atomic_write_json(self.history_path, history_data)

        print("[json_store] COMMIT status_updates=%d history_events=%d" % (len(su), len(he)))

    # ---- HISTORY READ ----

    def load_history(self, profile_id: Optional[str] = None, limit: int = 200) -> List[HistoryEvent]:
        with self._lock:
            data = self._read_json(self.history_path, default=[])
        if not isinstance(data, list):
            return []

        items = data
        if profile_id:
            items = [x for x in items if isinstance(x, dict) and x.get("profile_id") == profile_id]
        items = items[-max(1, int(limit)):]
        out: List[HistoryEvent] = []
        for x in items:
            if isinstance(x, dict):
                out.append(self._dict_to_event(x))
        print("[json_store] load_history profile=%s limit=%d -> %d" % (profile_id, limit, len(out)))
        return out

    # ---- UTIL ----

    def stats(self) -> Dict[str, int]:
        with self._lock:
            sd = self._read_json(self.status_path, default={})
            hd = self._read_json(self.history_path, default=[])
        return {"status": len(sd) if isinstance(sd, dict) else 0, "history": len(hd) if isinstance(hd, list) else 0}

    # ---- Internal helpers ----

    def _key_to_str(self, k: StatusKey) -> str:
        # stable serialization
        return f"{k.profile_id}::{k.gid}::{k.symbol}::{k.exchange}::{k.clock_interval}"

    def _read_json(self, path: str, default):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return default
        except Exception as e:
            print("[json_store] READ_FAIL path=%s err=%s -> default" % (path, e))
            return default

    def _atomic_write_json(self, path: str, obj) -> None:
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except Exception as e:
            print("[json_store] WRITE_FAIL path=%s err=%s" % (path, e))
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
            raise

    def _dict_to_status(self, d: dict) -> StatusState:
        # tolerant migration: ignore unknown fields, fill missing
        st = StatusState()
        st.active = bool(d.get("active", True))
        st.streak_current = int(d.get("streak_current", 0) or 0)
        st.count_window = list(d.get("count_window", []) or [])
        st.last_partial_true = d.get("last_partial_true", None)
        st.last_final_state = d.get("last_final_state", None)  # might be string; engine treats as optional
        st.last_true_ts = d.get("last_true_ts", None)
        st.last_push_ts = d.get("last_push_ts", None)
        st.last_tick_ts = d.get("last_tick_ts", None)
        st.last_reason = d.get("last_reason", "") or ""
        st.last_debug = d.get("last_debug", {}) or {}
        return st

    def _dict_to_event(self, d: dict) -> HistoryEvent:
        # tolerant event restore
        return HistoryEvent(
            ts=str(d.get("ts", "")),
            profile_id=str(d.get("profile_id", "")),
            gid=str(d.get("gid", "")),
            symbol=str(d.get("symbol", "")),
            exchange=str(d.get("exchange", "")),
            event=str(d.get("event", "")),
            partial_true=d.get("partial_true", None),
            final_state=d.get("final_state", None),
            rid=d.get("rid", None),
            left_value=d.get("left_value", None),
            right_value=d.get("right_value", None),
            op=d.get("op", None),
            threshold_snapshot=d.get("threshold_snapshot", {}) or {},
            debug=d.get("debug", {}) or {},
        )
