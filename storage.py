# storage.py – zentrales File- & JSON-Handling + FileLock
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import time
import tempfile
import hashlib
import logging
from pathlib import Path
from typing import Any, Callable, Tuple, List

log = logging.getLogger("notifier.storage")


# ─────────────────────────────────────────────────────────────
# Pfad-Helfer
# ─────────────────────────────────────────────────────────────

def to_path(p: Any) -> Path:
    """
    Konvertiert beliebige Pfadangaben in einen absoluten Path.
    """
    if isinstance(p, Path):
        return p.expanduser().resolve()
    return Path(str(p)).expanduser().resolve()


def _ensure_parent_dir(path: Path) -> None:
    """
    Stellt sicher, dass das Elternverzeichnis existiert.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.error("Failed to create parent dir for %s: %s", path, e)


# ─────────────────────────────────────────────────────────────
# Lock-Verzeichnis + Hash
# ─────────────────────────────────────────────────────────────

_ENV_LOCK_DIR = os.environ.get("NOTIFIER_LOCK_DIR", "").strip()
if _ENV_LOCK_DIR:
    LOCK_DIR = to_path(_ENV_LOCK_DIR)
else:
    LOCK_DIR = Path(tempfile.gettempdir()) / "notifier_locks"

try:
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    log.debug("Lock dir ensured: %s", LOCK_DIR)
except Exception as e:
    log.error("Failed to create lock dir %s: %s", LOCK_DIR, e)


def _lock_path(path: Path) -> Path:
    """
    Erzeugt Pfad zur Lock-Datei für eine gegebene Ressource.
    """
    try:
        name = Path(path).name
    except Exception:
        name = str(path)
    return LOCK_DIR / (name + ".lock")


def sha256_bytes(b: bytes) -> str:
    """
    Gibt SHA256-Hash eines Byte-Strings zurück.
    """
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


# ─────────────────────────────────────────────────────────────
# FileLock
# ─────────────────────────────────────────────────────────────

class FileLock:
    """
    Einfacher File-basierten Lock mit Stale-Erkennung.
    Verwendet O_CREAT|O_EXCL um atomar eine Lock-Datei zu erzeugen.

    Beispiel:
        with FileLock(path):
            # sicherer Block
            ...
    """

    def __init__(
        self,
        path: Path,
        timeout: float = 10.0,
        poll: float = 0.1,
        stale_after: float = 300.0,
    ) -> None:
        self.lockfile = _lock_path(path)
        self.timeout = timeout
        self.poll = poll
        self.stale_after = stale_after
        self._acquired = False

    def _is_stale(self) -> bool:
        try:
            st = self.lockfile.stat()
            return (time.time() - st.st_mtime) > self.stale_after
        except FileNotFoundError:
            return False

    def acquire(self) -> None:
        start = time.time()
        while True:
            try:
                fd = os.open(str(self.lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                self._acquired = True
                log.debug("FileLock acquired: %s", self.lockfile)
                return
            except FileExistsError:
                if self._is_stale():
                    try:
                        os.unlink(self.lockfile)
                        log.warning("FileLock stale removed: %s", self.lockfile)
                    except FileNotFoundError:
                        pass
                    continue
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lockfile}")
                time.sleep(self.poll)

    def release(self) -> None:
        if self._acquired:
            try:
                os.unlink(self.lockfile)
                log.debug("FileLock released: %s", self.lockfile)
            except FileNotFoundError:
                pass
            finally:
                self._acquired = False

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


# ─────────────────────────────────────────────────────────────
# Text-IO (für Debug/Logfiles etc.)
# ─────────────────────────────────────────────────────────────

def read_text(path: Any, default: str = "") -> str:
    """
    Liest eine Textdatei, gibt default zurück bei Fehlern.
    """
    p = to_path(path)
    try:
        data = p.read_text(encoding="utf-8")
        log.debug("read_text: %s bytes=%d", p, len(data))
        return data
    except FileNotFoundError:
        log.info("read_text: missing → default (%s)", p)
        return default
    except Exception as e:
        log.error("read_text failed (%s): %s", p, e)
        return default


def write_text_atomic(path: Any, text: str) -> None:
    """
    Schreibt Text atomar auf die Platte (Temp-Datei + replace).
    """
    p = to_path(path)
    _ensure_parent_dir(p)
    tmp = p.with_suffix(p.suffix + ".tmp")
    payload = text.encode("utf-8")

    with FileLock(p):
        try:
            if p.exists():
                cur = p.read_bytes()
                if len(cur) == len(payload) and sha256_bytes(cur) == sha256_bytes(payload):
                    log.debug("write_text_atomic skipped (no change): %s", p)
                    return
        except Exception:
            pass

        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)

        try:
            if hasattr(os, "O_DIRECTORY"):
                dfd = os.open(str(p.parent), os.O_DIRECTORY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
        except Exception:
            pass

    log.info("write_text_atomic: %s bytes=%d", p, len(payload))


# ─────────────────────────────────────────────────────────────
# JSON-IO (generisch)
# ─────────────────────────────────────────────────────────────

def load_json(path: Any, fallback: Any) -> Any:
    """
    Lädt JSON (list/dict/etc.). Gibt fallback zurück bei Fehlern.

    Achtung: fallback wird geklont, damit der Aufrufer nicht versehentlich
    ein veränderbares Default-Objekt shared.
    """
    p = to_path(path)
    if not p.exists():
        log.info("load_json: missing → fallback (%s)", p)
        # kein deepcopy, um Import-Loop mit copy zu vermeiden; für einfache
        # Strukturen reicht json roundtrip als Clone:
        try:
            return json.loads(json.dumps(fallback))
        except Exception:
            return fallback
    try:
        txt = p.read_text(encoding="utf-8")
        data = json.loads(txt)
        log.info(
            "load_json: %s type=%s",
            p,
            type(data).__name__,
        )
        return data
    except Exception as e:
        log.error("load_json failed (%s): %s", p, e)
        try:
            return json.loads(json.dumps(fallback))
        except Exception:
            return fallback


def save_json_atomic(path: Any, data: Any) -> None:
    """
    Schreibt JSON atomar, vermeidet unnötige Writes via Hashvergleich.
    """
    p = to_path(path)
    _ensure_parent_dir(p)
    tmp = p.with_suffix(p.suffix + ".tmp")

    payload_str = json.dumps(data, indent=2, ensure_ascii=False)
    payload = payload_str.encode("utf-8")

    with FileLock(p):
        try:
            if p.exists():
                cur = p.read_bytes()
                if len(cur) == len(payload) and sha256_bytes(cur) == sha256_bytes(payload):
                    log.debug("save_json_atomic skipped (no change): %s", p)
                    return
        except Exception:
            pass

        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)

        try:
            if hasattr(os, "O_DIRECTORY"):
                dfd = os.open(str(p.parent), os.O_DIRECTORY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
        except Exception:
            pass

    log.info("save_json_atomic: %s bytes=%d", p, len(payload))


# ─────────────────────────────────────────────────────────────
# Backwards-Compatible Wrapper
# ─────────────────────────────────────────────────────────────

def save_json(path: Any, data: Any) -> None:
    """Alias für save_json_atomic für bestehenden Code."""

    save_json_atomic(path, data)


def load_json_any(path: Any, fallback: Any = None) -> Any:
    """Alias für load_json, akzeptiert beliebige Fallback-Typen."""

    return load_json(path, fallback)


def save_json_any(path: Any, data: Any) -> None:
    """Alias für save_json_atomic für beliebige JSON-Daten."""

    save_json_atomic(path, data)


def load_json_list(path: Any, fallback: List[Any] | None = None) -> List[Any]:
    """
    Lädt ein JSON-Array. Gibt immer eine Liste zurück.
    """
    if fallback is None:
        fallback = []
    data = load_json(path, fallback)
    if isinstance(data, list):
        return data
    log.warning("load_json_list: expected list, got %s → fallback", type(data).__name__)
    return fallback


def atomic_update_json_list(
    path: Any,
    transform_fn: Callable[[List[Any]], Tuple[List[Any], Any]],
) -> Tuple[List[Any], Any]:
    """
    Atomisches Read→Transform→Write unter EINEM FileLock.

    transform_fn: (current_list: list) -> (new_list: list, result: Any)
    Gibt (new_list, result) zurück und speichert nur bei Änderung.
    """
    p = to_path(path)
    current = load_json_list(p, fallback=[])

    with FileLock(p):
        # Aktuellen Stand nochmal lesen, um Race-Conditions zu minimieren
        current = load_json_list(p, fallback=[])
        new_list, result = transform_fn(list(current))

        cur_bytes = json.dumps(current, sort_keys=True, ensure_ascii=False).encode("utf-8")
        new_bytes = json.dumps(new_list, sort_keys=True, ensure_ascii=False).encode("utf-8")

        if sha256_bytes(cur_bytes) != sha256_bytes(new_bytes):
            tmp = p.with_suffix(p.suffix + ".tmp")
            _ensure_parent_dir(p)
            with open(tmp, "wb") as f:
                f.write(json.dumps(new_list, indent=2, ensure_ascii=False).encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, p)
            try:
                if hasattr(os, "O_DIRECTORY"):
                    dfd = os.open(str(p.parent), os.O_DIRECTORY)
                    try:
                        os.fsync(dfd)
                    finally:
                        os.close(dfd)
            except Exception:
                pass
            log.info("atomic_update_json_list: saved %s (len=%d)", p, len(new_list))
        else:
            log.debug("atomic_update_json_list: no change for %s", p)

    return new_list, result
