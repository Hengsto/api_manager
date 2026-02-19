# main_evaluator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict

import config as cfg
from pydantic import ValidationError

from notifier_evaluator.context.group_expander import StaticMappingSource, TTLGroupExpander
from notifier_evaluator.eval.engine import EngineConfig, EvaluatorEngine
from notifier_evaluator.eval.validate import validate_profiles
from notifier_evaluator.fetch.client import ClientConfig, IndicatorClient
from notifier_evaluator.models.schema import EngineDefaults, Profile
from notifier_evaluator.state.json_store import JsonStore


# ──────────────────────────────────────────────────────────────────────────────
# DEBUG: prove which file is executed
# ──────────────────────────────────────────────────────────────────────────────
print("[EVALUATOR] __file__ =", __file__)
print("[EVALUATOR] cwd =", os.getcwd())


def _prime_sys_path() -> None:
    cwd = str(Path.cwd().resolve())
    here = str(Path(__file__).resolve().parent)
    for p in (cwd, here):
        if p not in sys.path:
            sys.path.insert(0, p)
    print(f"[evaluator][DBG] sys.path primed cwd={cwd} here={here}")


def _default_profiles_path() -> Path:
    return Path(os.getenv("EVALUATOR_PROFILES_FILE", "") or str(getattr(cfg, "PROFILES_NOTIFIER")))


def _default_status_path() -> Path:
    return Path(os.getenv("EVALUATOR_STATUS_FILE", "") or (Path(cfg.EVALUATOR_DATA_DIR) / "evaluator_status.json"))


def _default_history_path() -> Path:
    return Path(os.getenv("EVALUATOR_HISTORY_FILE", "") or (Path(cfg.EVALUATOR_DATA_DIR) / "evaluator_history.json"))


def _load_profiles(path: Path) -> list[Profile]:
    print(f"[evaluator][DBG] profiles_file={path}")
    if not path.exists():
        raise FileNotFoundError(f"profiles file missing: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("profiles payload must be a list (NEW schema)")

    profiles: list[Profile] = []
    for i, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"profile index {i} must be an object")
        try:
            profiles.append(Profile.model_validate(item))
        except ValidationError as exc:
            raise ValueError(f"profile index {i} failed schema validation: {exc}") from exc

    print(f"[evaluator][DBG] loaded profiles={len(profiles)}")
    return profiles


def _load_group_mapping(path: Path | None) -> dict[str, list[str]]:
    if not path:
        return {}
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"group mapping must be object: {path}")
    mapping: dict[str, list[str]] = {}
    for key, val in payload.items():
        if isinstance(val, list):
            mapping[str(key)] = [str(x) for x in val if str(x).strip()]
    return mapping


def _engine_defaults_from_env() -> EngineDefaults:
    """
    Robust builder:
    EngineDefaults fields changed during your refactor (exchange/interval vs default_exchange/default_interval, etc.).
    We introspect the Pydantic model fields and only pass what exists.
    """
    ex = os.getenv("EVALUATOR_DEFAULT_EXCHANGE", str(getattr(cfg, "DEFAULT_EXCHANGE", "")) or "binance")
    it = os.getenv("EVALUATOR_DEFAULT_INTERVAL", str(getattr(cfg, "DEFAULT_INTERVAL", "")) or "1h")

    # pydantic v2: model_fields, v1: __fields__
    mf = getattr(EngineDefaults, "model_fields", None)
    if mf is None:
        mf = getattr(EngineDefaults, "__fields__", {})  # type: ignore[assignment]

    keys = set(getattr(mf, "keys", lambda: [])())  # supports both dict-like variants

    data: Dict[str, Any] = {}

    # exchange
    if "default_exchange" in keys:
        data["default_exchange"] = ex
    elif "exchange" in keys:
        data["exchange"] = ex

    # interval
    if "default_interval" in keys:
        data["default_interval"] = it
    elif "interval" in keys:
        data["interval"] = it

    # clock interval (optional)
    if "clock_interval" in keys:
        # prefer explicit env if present, else use interval fallback
        clk = os.getenv("EVALUATOR_CLOCK_INTERVAL", "") or it
        data["clock_interval"] = clk

    # source (optional) - DO NOT inject legacy "Close" here unless schema actually wants it
    if "source" in keys:
        # only set if explicitly configured, otherwise keep schema default
        src = os.getenv("EVALUATOR_DEFAULT_SOURCE", "").strip()
        if src:
            data["source"] = src

    print(f"[evaluator][DBG] EngineDefaults fields={sorted(list(keys))}")
    print(f"[evaluator][DBG] EngineDefaults data={data}")

    return EngineDefaults(**data)


def _build_engine(*, indicator_base_url: str, status_path: Path, history_path: Path, mapping_path: Path | None) -> EvaluatorEngine:
    defaults = _engine_defaults_from_env()

    engine_cfg = EngineConfig(
        defaults=defaults,
        fetch_ttl_sec=int(os.getenv("EVALUATOR_FETCH_TTL_SEC", "5")),
        group_expand_ttl_sec=int(os.getenv("EVALUATOR_GROUP_EXPAND_TTL_SEC", "10")),
        request_mode=os.getenv("EVALUATOR_REQUEST_MODE", "latest"),
        request_as_of=os.getenv("EVALUATOR_REQUEST_AS_OF", "") or None,
    )

    store = JsonStore(status_path=str(status_path), history_path=str(history_path))
    group_source = StaticMappingSource(_load_group_mapping(mapping_path))
    group_expander = TTLGroupExpander(source=group_source, ttl_sec=engine_cfg.group_expand_ttl_sec)

    client = IndicatorClient(
        ClientConfig(
            base_url=indicator_base_url,
            timeout_sec=int(os.getenv("EVALUATOR_HTTP_TIMEOUT", "10")),
            retries=int(os.getenv("EVALUATOR_HTTP_RETRIES", "2")),
            backoff=float(os.getenv("EVALUATOR_HTTP_BACKOFF", "0.3")),
            verify_ssl=os.getenv("EVALUATOR_HTTP_VERIFY_SSL", "1") not in ("0", "false", "False"),
        )
    )
    return EvaluatorEngine(cfg=engine_cfg, store=store, group_expander=group_expander, client=client)


def _run_once(profiles_path: Path, engine: EvaluatorEngine) -> None:
    profiles = _load_profiles(profiles_path)
    validation = validate_profiles(profiles)
    if not validation.ok:
        raise ValueError(f"validation failed: {validation.errors_n} errors")
    summary = engine.run(profiles)
    print(f"[evaluator][DBG] run summary={summary}")


def main() -> None:
    _prime_sys_path()
    parser = argparse.ArgumentParser(description="Run the notifier evaluator loop")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=float, default=float(os.getenv("EVALUATOR_INTERVAL_SEC", "60")))
    parser.add_argument("--profiles", type=str, default=str(_default_profiles_path()))
    parser.add_argument("--status", type=str, default=str(_default_status_path()))
    parser.add_argument("--history", type=str, default=str(_default_history_path()))
    parser.add_argument("--mapping", type=str, default=os.getenv("EVALUATOR_GROUP_MAPPING_FILE", ""))
    parser.add_argument("--indicator-base-url", type=str, default=os.getenv("EVALUATOR_INDICATOR_BASE_URL", ""))
    args = parser.parse_args()

    indicator_base_url = args.indicator_base_url or os.getenv(
        "INDICATOR_BASE_URL",
        f"http://{cfg.MAIN_IP}:{cfg.NOTIFIER_PORT}",
    )

    print(f"[evaluator][DBG] indicator_base_url={indicator_base_url}")
    print(f"[evaluator][DBG] status_path={args.status}")
    print(f"[evaluator][DBG] history_path={args.history}")
    print(f"[evaluator][DBG] profiles_path={args.profiles}")
    if args.mapping:
        print(f"[evaluator][DBG] mapping_path={args.mapping}")

    engine = _build_engine(
        indicator_base_url=indicator_base_url,
        status_path=Path(args.status),
        history_path=Path(args.history),
        mapping_path=Path(args.mapping) if args.mapping else None,
    )

    if args.once:
        _run_once(Path(args.profiles), engine)
        return

    interval = max(1.0, float(args.interval))
    print(f"[evaluator] loop start interval={interval:.1f}s profiles={args.profiles}")
    while True:
        try:
            _run_once(Path(args.profiles), engine)
        except Exception as e:
            print(f"[evaluator] run failed: {e}")
            traceback.print_exc()
        time.sleep(interval)


if __name__ == "__main__":
    main()
