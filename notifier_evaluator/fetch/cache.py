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
    purged: int = 0


class TTLCache:
    """
    Simple TTL cache: key -> (expires_ts, FetchResult)

    NOTE:
    - intentionally minimal (keine LRU eviction)
    - ok für Start; wenn du später memory growth willst -> LRU/size cap.
    """

    def __init__(self, ttl_sec_ok: int = 5, ttl_sec_fail: int = 1):
        self.ttl_sec_ok = max(0, int(ttl_sec_ok))
        self.ttl_sec_fail = max(0, int(ttl_sec_fail))
        self._data: Dict[RequestKey, Tuple[float, FetchResult]] = {}

    def _ttl_for(self, val: FetchResult) -> int:
        # Failure TTL shorter to avoid "sticky" outages while still preventing storms.
        if val is None:
            return self.ttl_sec_fail
        return self.ttl_sec_ok if bool(val.ok) else self.ttl_sec_fail

    def enabled(self) -> bool:
        return (self.ttl_sec_ok > 0) or (self.ttl_sec_fail > 0)

    def get(self, key: RequestKey) -> Tuple[Optional[FetchResult], bool]:
        """
        Returns (val, expired_flag).
        expired_flag True means: key existed but was expired and got evicted.
        """
        if not self.enabled():
            return None, False

        item = self._data.get(key)
        if not item:
            return None, False

        exp_ts, val = item
        now = time.time()
        if now <= exp_ts:
            return val, False

        # expired
        try:
            del self._data[key]
        except Exception:
            pass
        return None, True

    def set(self, key: RequestKey, val: FetchResult) -> None:
        if not self.enabled():
            return
        ttl = self._ttl_for(val)
        if ttl <= 0:
            return
        exp_ts = time.time() + ttl
        self._data[key] = (exp_ts, val)

    def purge(self) -> int:
        """
        Removes expired items. Returns number removed.
        """
        if not self.enabled():
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

    def __init__(self, ttl_sec: int = 5, ttl_sec_fail: int = 1, purge_every_sets: int = 200):
        self.run_cache: Dict[RequestKey, FetchResult] = {}
        self.ttl_cache = TTLCache(ttl_sec_ok=ttl_sec, ttl_sec_fail=ttl_sec_fail)
        self.stats = CacheStats()
        self._purge_every_sets = max(0, int(purge_every_sets))

    def reset_run_cache(self) -> None:
        """
        Call at start of each engine run.
        """
        print("[fetch.cache] reset_run_cache() prev_size=%d" % len(self.run_cache))
        self.run_cache.clear()

    def get_or_fetch(self, key: RequestKey, fetch_fn: Callable[[RequestKey], FetchResult]) -> FetchResult:
        """
        Dedupe order:
          1) run_cache (strong)
          2) ttl_cache (weak)
          3) fetch
        """
        # 1) run_cache
        fr = self.run_cache.get(key)
        if fr is not None:
            self.stats.run_hit += 1
            print("[fetch.cache] RUN_HIT key=%s ok=%s" % (key.short(), fr.ok))
            return fr

        # 2) ttl_cache
        fr2, expired = self.ttl_cache.get(key)
        if expired:
            self.stats.ttl_expired += 1
            print("[fetch.cache] TTL_EXPIRED key=%s ttl_size=%d" % (key.short(), self.ttl_cache.size()))

        if fr2 is not None:
            self.stats.ttl_hit += 1
            self.run_cache[key] = fr2
            print("[fetch.cache] TTL_HIT key=%s ok=%s ttl_size=%d" % (key.short(), fr2.ok, self.ttl_cache.size()))
            return fr2

        # 3) fetch
        self.stats.miss += 1
        print("[fetch.cache] MISS key=%s (fetching...)" % key.short())

        fr3 = fetch_fn(key)

        # set caches regardless of ok? -> yes, short fail TTL prevents storms on failing endpoints
        self.run_cache[key] = fr3
        self.ttl_cache.set(key, fr3)
        self.stats.set += 1

        # opportunistic purge (keeps cache from growing forever)
        if self._purge_every_sets > 0 and (self.stats.set % self._purge_every_sets == 0):
            removed = self.ttl_cache.purge()
            self.stats.purged += removed
            print("[fetch.cache] PURGE removed=%d ttl_size=%d" % (removed, self.ttl_cache.size()))

        print(
            "[fetch.cache] SET key=%s ok=%s err=%s ttl_size=%d run_size=%d"
            % (key.short(), fr3.ok, fr3.error, self.ttl_cache.size(), len(self.run_cache))
        )
        return fr3

    def summary(self) -> str:
        return (
            f"run_hit={self.stats.run_hit} ttl_hit={self.stats.ttl_hit} "
            f"miss={self.stats.miss} set={self.stats.set} ttl_expired={self.stats.ttl_expired} "
            f"purged={self.stats.purged} ttl_size={self.ttl_cache.size()} run_size={len(self.run_cache)}"
        )
