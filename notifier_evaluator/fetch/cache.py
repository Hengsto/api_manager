# notifier_evaluator/fetch/cache.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple

from notifier_evaluator.fetch.types import RequestKey
from notifier_evaluator.models.runtime import FetchResult


# ──────────────────────────────────────────────────────────────────────────────
# Fetch Cache
# - run_cache: dedupe innerhalb eines Engine-Runs (100% deterministisch)
# - ttl_cache: dedupe über Runs (zeitbasiert)
#
# API:
#   cache.get_or_fetch(key, fetch_fn) -> FetchResult
#
# fetch_fn:
#   Callable[[RequestKey], FetchResult]
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class CacheStats:
    run_hit: int = 0
    ttl_hit: int = 0
    miss: int = 0
    set: int = 0
    ttl_expired: int = 0


class TTLCache:
    """
    Simple TTL cache: key -> (expires_ts, FetchResult)

    NOTE:
    - intentionally minimal (keine LRU eviction)
    - ok für Start; wenn du später memory growth willst -> LRU/size cap.
    """

    def __init__(self, ttl_sec: int = 5):
        self.ttl_sec = max(0, int(ttl_sec))
        self._data: Dict[RequestKey, Tuple[float, FetchResult]] = {}

    def get(self, key: RequestKey) -> Optional[FetchResult]:
        if self.ttl_sec <= 0:
            return None
        item = self._data.get(key)
        if not item:
            return None
        exp_ts, val = item
        now = time.time()
        if now <= exp_ts:
            return val
        # expired
        try:
            del self._data[key]
        except Exception:
            pass
        return None

    def set(self, key: RequestKey, val: FetchResult) -> None:
        if self.ttl_sec <= 0:
            return
        exp_ts = time.time() + self.ttl_sec
        self._data[key] = (exp_ts, val)

    def purge(self) -> int:
        """
        Removes expired items. Returns number removed.
        """
        if self.ttl_sec <= 0:
            return 0
        now = time.time()
        dead = [k for k, (exp, _) in self._data.items() if now > exp]
        for k in dead:
            try:
                del self._data[k]
            except Exception:
                pass
        return len(dead)

    def size(self) -> int:
        return len(self._data)


class FetchCache:
    """
    Combines run_cache and ttl_cache.
    """

    def __init__(self, ttl_sec: int = 5):
        self.run_cache: Dict[RequestKey, FetchResult] = {}
        self.ttl_cache = TTLCache(ttl_sec=ttl_sec)
        self.stats = CacheStats()

    def reset_run_cache(self) -> None:
        """
        Call at start of each engine run.
        """
        print("[fetch.cache] reset_run_cache() prev_size=%d" % len(self.run_cache))
        self.run_cache = {}

    def get_or_fetch(self, key: RequestKey, fetch_fn: Callable[[RequestKey], FetchResult]) -> FetchResult:
        """
        Dedupe order:
          1) run_cache (strong)
          2) ttl_cache (weak)
          3) fetch
        """
        # 1) run_cache
        if key in self.run_cache:
            self.stats.run_hit += 1
            fr = self.run_cache[key]
            print("[fetch.cache] RUN_HIT key=%s ok=%s" % (key.short(), fr.ok))
            return fr

        # 2) ttl_cache
        fr2 = self.ttl_cache.get(key)
        if fr2 is not None:
            self.stats.ttl_hit += 1
            self.run_cache[key] = fr2
            print("[fetch.cache] TTL_HIT key=%s ok=%s ttl_size=%d" % (key.short(), fr2.ok, self.ttl_cache.size()))
            return fr2

        # 3) fetch
        self.stats.miss += 1
        print("[fetch.cache] MISS key=%s (fetching...)" % key.short())

        fr = fetch_fn(key)

        # set caches regardless of ok? -> yes, short TTL prevents storm on failing endpoints
        self.run_cache[key] = fr
        self.ttl_cache.set(key, fr)
        self.stats.set += 1

        print(
            "[fetch.cache] SET key=%s ok=%s err=%s ttl_size=%d run_size=%d"
            % (key.short(), fr.ok, fr.error, self.ttl_cache.size(), len(self.run_cache))
        )
        return fr

    def summary(self) -> str:
        return (
            f"run_hit={self.stats.run_hit} ttl_hit={self.stats.ttl_hit} "
            f"miss={self.stats.miss} set={self.stats.set} ttl_size={self.ttl_cache.size()} "
            f"run_size={len(self.run_cache)}"
        )
