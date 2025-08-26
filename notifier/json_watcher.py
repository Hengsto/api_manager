# notifier/json_watcher.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import time, json, hashlib
from pathlib import Path
from typing import Any, Callable, Tuple, Optional

def _fingerprint(obj: Any) -> Tuple[str,int]:
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",",":"))
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]
    return h, len(s)

def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[JSON-WATCH] load error {path}: {e}")
        return None

def run(path: str | Path,
        on_change: Optional[Callable[[Any, Any], None]] = None,
        interval_sec: float = 1.0,
        settle_seconds: float = 1.5) -> None:
    p = Path(path)
    prev_obj = None
    prev_fp = None
    first = True
    print(f"[JSON-WATCH] monitoring {p} every {interval_sec}s")
    while True:
        try:
            if not p.exists():
                time.sleep(interval_sec); continue
            stat = p.stat()
            if (time.time() - stat.st_mtime) < settle_seconds:
                time.sleep(interval_sec); continue
            obj = _load_json(p)
            fp, size = _fingerprint(obj)
            if first:
                print(f"[JSON-WATCH] initial hash={fp} size={size}")
                prev_obj, prev_fp, first = obj, fp, False
            elif fp != prev_fp:
                print(f"[JSON-WATCH] change: {prev_fp} -> {fp}")
                if callable(on_change):
                    try:
                        on_change(prev_obj, obj)
                    except Exception as e:
                        print(f"[JSON-WATCH] on_change failed: {e}")
                prev_obj, prev_fp = obj, fp
        except Exception as e:
            print(f"[JSON-WATCH] loop error: {e}")
        time.sleep(interval_sec)
