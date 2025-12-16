# notifier_evaluator/debug/trace.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Any, Optional


def new_run_id() -> str:
    return uuid.uuid4().hex[:10]


@dataclass
class Trace:
    run_id: str = field(default_factory=new_run_id)
    t0: float = field(default_factory=time.time)
    marks: Dict[str, float] = field(default_factory=dict)
    counters: Dict[str, int] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)

    def mark(self, name: str) -> None:
        self.marks[name] = time.time()

    def inc(self, name: str, n: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + n

    def dt(self, name_from: str, name_to: Optional[str] = None) -> Optional[float]:
        a = self.marks.get(name_from)
        b = self.marks.get(name_to) if name_to else time.time()
        if a is None or b is None:
            return None
        return b - a

    def since_start(self) -> float:
        return time.time() - self.t0
