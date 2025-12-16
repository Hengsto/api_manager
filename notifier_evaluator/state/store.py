# notifier_evaluator/state/store.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from notifier_evaluator.models.runtime import HistoryEvent, StatusKey, StatusState


@dataclass
class StoreCommit:
    """
    Atomischer Commit:
      - status_updates: kompletter neuer Status pro Key
      - history_events: append-only Events
    """
    status_updates: Dict[StatusKey, StatusState]
    history_events: List[HistoryEvent]


class StateStore(ABC):
    """
    Store Interface, das die Engine wirklich braucht (JSON/SQLite austauschbar).

    Warum ABC statt Protocol?
    - Wenn du versehentlich ein halb-implementiertes Store-Objekt übergibst,
      knallt es SOFORT (statt “irgendwie” zu laufen).
    """

    @abstractmethod
    def load_status(self, key: StatusKey) -> StatusState:
        """
        Muss default-Status liefern, wenn Key nicht existiert.
        """
        raise NotImplementedError("StateStore.load_status not implemented")

    @abstractmethod
    def load_status_batch(self, keys: Iterable[StatusKey]) -> Dict[StatusKey, StatusState]:
        """
        Batch-Load (Performance). Muss fehlende Keys defaulten.
        """
        raise NotImplementedError("StateStore.load_status_batch not implemented")

    @abstractmethod
    def commit(self, commit: StoreCommit) -> None:
        """
        Muss Status + History atomar persistieren.
        """
        raise NotImplementedError("StateStore.commit not implemented")

    @abstractmethod
    def load_history(self, profile_id: Optional[str] = None, limit: int = 200) -> List[HistoryEvent]:
        raise NotImplementedError("StateStore.load_history not implemented")

    @abstractmethod
    def stats(self) -> Dict[str, int]:
        raise NotImplementedError("StateStore.stats not implemented")
