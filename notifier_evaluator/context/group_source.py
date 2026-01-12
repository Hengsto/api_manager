# notifier_evaluator/context/group_source.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List


class StaticGroupSource:
    """
    Minimaler GroupSource:
    - @group tags werden NICHT expandiert
    - echte Symbole werden 1:1 durchgereicht

    Das ist absichtlich dumm – nur für Smoke-Run.
    """

    def expand(self, symbols: List[str]) -> List[str]:
        out: List[str] = []
        for s in symbols:
            if not s:
                continue
            # group-tags ignorieren wir erstmal
            if s.startswith("@"):
                print(f"[group_source] WARN ignoring group tag {s}")
                continue
            out.append(s)
        return out
