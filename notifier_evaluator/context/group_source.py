# notifier_evaluator/context/group_source.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List


class StaticGroupSource:
    """
    Minimaler GroupSource für Smoke-Runs:
    - group_tags werden NICHT expandiert (liefert immer [])
    - echte Symbol-Listen werden NICHT hier verarbeitet (macht group_expander via group.symbols)

    Das ist absichtlich dumm – nur für Smoke-Run.
    """

    def expand_tags(self, tags: List[str]) -> List[str]:
        # tags absichtlich ignoriert
        for t in tags or []:
            if t:
                print(f"[group_source] WARN ignoring group tag {t}")
        return []
