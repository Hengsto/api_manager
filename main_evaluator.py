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

import config as cfg

from notifier_evaluator.context.group_expander import StaticMappingSource, TTLGroupExpander
from notifier_evaluator.eval.engine import EngineConfig, EvaluatorEngine
from notifier_evaluator.eval.validate import validate_profiles
from notifier_evaluator.fetch.client import ClientConfig, IndicatorClient
from notifier_evaluator.models.schema import EngineDefaults, Profile
from notifier_evaluator.models.normalize import normalize_profile_dict
from notifier_evaluator.state.json_store import JsonStore


def _prime_sys_path() -> None:
    try:
        cwd = str(Path.cwd().resolve())
        here = str(Path(__file__).resolve().parent)
        for p in (cwd, here):
            if p not in sys.path:
                sys.path.insert(0, p)
        print(f"[DEBUG] sys.path primed: cwd={cwd}, here={here}")
    except Exception as e:
        print(f"[DEBUG] sys.path priming failed: {e}")


def _default_profiles_path() -> Path:
    # evaluator reads notifier profiles as source of truth
    return Path(os.getenv("EVALUATOR_PROFILES_FILE", "") or str(getattr(cfg, "PROFILES_NOTIFIER")))


def _default_status_path() -> Path:
    return Path(os.getenv("EVALUATOR_STATUS_FILE", "") or (Path(cfg.EVALUATOR_DATA_DIR) / "evaluator_status.json"))


def _default_history_path() -> Path:
    return Path(os.getenv("EVALUATOR_HISTORY_FILE", "") or (Path(cfg.EVALUATOR_DATA_DIR) / "evaluator_history.json"))


def _dbg(msg: str) -> None:
    try:
        print(msg)
    except Exception:
        pass


def _profile_from_dict(d: dict) -> Profile:
    """
    Pydantic v2/v1 compatible loader.
    - v2: Profile.model_validate(d)
    - v1: Profile.parse_obj(d)
    """
    _dbg(
        f"[evaluator][DBG] Profile type={Profile} "
        f"has model_validate={hasattr(Profile, 'model_validate')} "
        f"parse_obj={hasattr(Profile, 'parse_obj')}"
    )

    if hasattr(Profile, "model_validate"):
        return Profile.model_validate(d)  # type: ignore[attr-defined]
    if hasattr(Profile, "parse_obj"):
        return Profile.parse_obj(d)  # type: ignore[attr-defined]

    raise TypeError(f"Profile is missing model_validate/parse_obj. type={type(Profile)}")


def _to_boolish(v: object) -> bool | None:
    """
    Convert common truthy/falsey string/int into bool.
    Returns None if unknown.
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        if v in (0, 1):
            return bool(v)
        return None
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "y", "on", "enabled", "enable"):
            return True
        if s in ("0", "false", "no", "n", "off", "disabled", "disable"):
            return False
        return None
    return None


def _patch_group_mode_fields(raw_profile: dict) -> dict:
    """
    Drop-in adapter for schema mismatch:
    UI uses strings for:
      - group.single_mode: "symbol" | "all" | ...
      - group.deactivate_on: "auto_off" | "always_on" | ...
    Current evaluator pydantic schema expects bool for those fields.

    Strategy (minimal, to get evaluator running):
      - single_mode: str -> True if "symbol"/truthy else False
      - deactivate_on: str -> True if "auto_off"/truthy else False

    Does NOT mutate input dict.
    """
    if not isinstance(raw_profile, dict):
        return raw_profile

    d = dict(raw_profile)
    groups = d.get("groups")

    if not isinstance(groups, list):
        return d

    new_groups: list = []
    for gi, g in enumerate(groups):
        if not isinstance(g, dict):
            new_groups.append(g)
            continue

        gg = dict(g)

        # DEBUG originals
        sm = gg.get("single_mode")
        do = gg.get("deactivate_on")
        _dbg(f"[evaluator][DBG] group[{gi}] single_mode={sm!r} ({type(sm).__name__}) deactivate_on={do!r} ({type(do).__name__})")

        # single_mode
        if isinstance(sm, str):
            # treat "symbol" as True (single-symbol mode)
            sm_bool = True if sm.strip().lower() in ("symbol", "single", "one") else (_to_boolish(sm) if _to_boolish(sm) is not None else False)
            gg["single_mode"] = sm_bool
            _dbg(f"[evaluator][DBG] group[{gi}] patched single_mode: {sm!r} -> {sm_bool!r}")

        # deactivate_on
        if isinstance(do, str):
            # treat "auto_off" as True (deactivate on trigger)
            do_bool = True if do.strip().lower() in ("auto_off", "autooff", "deactivate", "off") else (_to_boolish(do) if _to_boolish(do) is not None else False)
            gg["deactivate_on"] = do_bool
            _dbg(f"[evaluator][DBG] group[{gi}] patched deactivate_on: {do!r} -> {do_bool!r}")

        new_groups.append(gg)

    d["groups"] = new_groups
    return d


def _normalize_profile_top_keys(raw: dict) -> dict:
    """
    Drop-in adapter:
    UI/Notifier schema -> Evaluator schema (tolerant)
      - id -> profile_id (fallback)
      - enabled -> active (fallback)
    Does NOT mutate input dict.
    """
    if not isinstance(raw, dict):
        return raw

    d = dict(raw)  # shallow copy

    # DEBUG: show incoming schema
    _dbg(f"[evaluator][DBG] raw_profile_keys={sorted(list(d.keys()))}")
    _dbg(f"[evaluator][DBG] raw id/profile_id/pid={d.get('id')}|{d.get('profile_id')}|{d.get('pid')}")
    _dbg(f"[evaluator][DBG] raw enabled/active/is_enabled={d.get('enabled')}|{d.get('active')}|{d.get('is_enabled')}")

    # map id -> profile_id if needed
    if "profile_id" not in d and "pid" not in d and "id" in d:
        d["profile_id"] = d.get("id")
        _dbg(f"[evaluator][DBG] patched profile_id <- id ({d.get('profile_id')})")

    # map enabled -> active if needed
    if "active" not in d and "is_enabled" not in d and "enabled" in d:
        d["active"] = bool(d.get("enabled"))
        _dbg(f"[evaluator][DBG] patched active <- enabled ({d.get('active')})")

    # patch group fields that mismatch the evaluator schema
    d = _patch_group_mode_fields(d)
    _dbg(f"[evaluator][DBG] patched_profile_keys={sorted(list(d.keys()))}")

    return d


def _load_profiles(path: Path) -> list[Profile]:
    print(f"[evaluator] profiles_file={path} exists={path.exists()} size={(path.stat().st_size if path.exists() else 0)}")

    if not path.exists():
        print(f"[evaluator] profiles file missing: {path}")
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[evaluator] profiles read failed: {path} err={e}")
        return []

    raw_profiles = None
    if isinstance(payload, list):
        raw_profiles = payload
    elif isinstance(payload, dict):
        raw_profiles = payload.get("profiles")

    if not isinstance(raw_profiles, list):
        print(f"[evaluator] profiles payload invalid: expected list or {{profiles: []}} in {path}")
        return []

    default_ex = str(getattr(cfg, "DEFAULT_EXCHANGE", "")).strip() or "binance"
    default_it = str(getattr(cfg, "DEFAULT_INTERVAL", "")).strip() or "1h"

    profiles: list[Profile] = []
    bad = 0

    print(f"[evaluator] raw profiles list_len={len(raw_profiles)} default_ex={default_ex!r} default_it={default_it!r}")

    for i, item in enumerate(raw_profiles):
        if not isinstance(item, dict):
            bad += 1
            print(f"[evaluator] profile idx={i} not dict -> skip type={type(item)}")
            continue

        norm = None
        patched_item = None

        try:
            patched_item = _normalize_profile_top_keys(item)

            norm = normalize_profile_dict(
                patched_item,
                default_exchange=default_ex,
                default_interval=default_it,
                debug=True,
            )

            _dbg(f"[evaluator][DBG] norm_profile_keys={sorted(list(norm.keys()))}")

            profiles.append(_profile_from_dict(norm))

        except Exception as e:
            bad += 1
            print(f"[evaluator] profile idx={i} invalid: {repr(e)} keys={list(item.keys())[:50]}")
            try:
                print("[evaluator] --- TRACEBACK START ---")
                traceback.print_exc()
                print("[evaluator] --- TRACEBACK END ---")
            except Exception:
                pass

            try:
                if patched_item is not None:
                    print(f"[evaluator][DUMP] patched_item keys={sorted(list(patched_item.keys()))}")
                    print("[evaluator][DUMP] patched_item json:")
                    print(json.dumps(patched_item, ensure_ascii=False, indent=2)[:8000])
            except Exception as dump_err:
                print(f"[evaluator][DUMP] patched_item failed: {dump_err}")

            try:
                if norm is not None:
                    print(f"[evaluator][DUMP] norm keys={sorted(list(norm.keys()))}")
                    print("[evaluator][DUMP] norm json:")
                    print(json.dumps(norm, ensure_ascii=False, indent=2)[:8000])
            except Exception as dump_err:
                print(f"[evaluator][DUMP] norm failed: {dump_err}")

    print(f"[evaluator] loaded profiles={len(profiles)} bad={bad} from {path}")
    return profiles


def _load_group_mapping(path: Path | None) -> dict[str, list[str]]:
    if not path:
        return {}
    if not path.exists():
        print(f"[evaluator] group mapping file missing: {path}")
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[evaluator] group mapping read failed: {path} err={e}")
        return {}
    if not isinstance(payload, dict):
        print(f"[evaluator] group mapping invalid (expected dict) in {path}")
        return {}
    mapping: dict[str, list[str]] = {}
    for key, val in payload.items():
        if isinstance(val, list):
            mapping[str(key)] = [str(x) for x in val if str(x).strip()]
    print(f"[evaluator] group mapping loaded tags={len(mapping)} from {path}")
    return mapping


def _build_engine(
    *,
    indicator_base_url: str,
    status_path: Path,
    history_path: Path,
    mapping_path: Path | None,
) -> EvaluatorEngine:
    defaults = EngineDefaults(
        exchange=os.getenv("EVALUATOR_DEFAULT_EXCHANGE", str(getattr(cfg, "DEFAULT_EXCHANGE", ""))),
        interval=os.getenv("EVALUATOR_DEFAULT_INTERVAL", str(getattr(cfg, "DEFAULT_INTERVAL", ""))),
        clock_interval=os.getenv("EVALUATOR_DEFAULT_CLOCK_INTERVAL", ""),
        source=os.getenv("EVALUATOR_DEFAULT_SOURCE", "Close"),
    )

    engine_cfg = EngineConfig(
        defaults=defaults,
        fetch_ttl_sec=int(os.getenv("EVALUATOR_FETCH_TTL_SEC", "5")),
        group_expand_ttl_sec=int(os.getenv("EVALUATOR_GROUP_EXPAND_TTL_SEC", "10")),
        request_mode=os.getenv("EVALUATOR_REQUEST_MODE", "latest"),
        request_as_of=os.getenv("EVALUATOR_REQUEST_AS_OF", "") or None,
    )

    print(f"[DEBUG] Evaluator JsonStore status_path={status_path}")
    print(f"[DEBUG] Evaluator JsonStore history_path={history_path}")
    store = JsonStore(status_path=str(status_path), history_path=str(history_path))

    mapping = _load_group_mapping(mapping_path)
    group_source = StaticMappingSource(mapping)
    group_expander = TTLGroupExpander(group_source, ttl_sec=engine_cfg.group_expand_ttl_sec)

    client_cfg = ClientConfig(
        base_url=indicator_base_url,
        timeout_sec=int(os.getenv("EVALUATOR_HTTP_TIMEOUT", "10")),
        retries=int(os.getenv("EVALUATOR_HTTP_RETRIES", "2")),
        backoff=float(os.getenv("EVALUATOR_HTTP_BACKOFF", "0.3")),
        verify_ssl=os.getenv("EVALUATOR_HTTP_VERIFY_SSL", "1") not in ("0", "false", "False"),
    )
    client = IndicatorClient(client_cfg)

    return EvaluatorEngine(
        cfg=engine_cfg,
        store=store,
        group_expander=group_expander,
        client=client,
    )


def _run_once(profiles_path: Path, engine: EvaluatorEngine, allow_invalid: bool) -> None:
    profiles = _load_profiles(profiles_path)
    if not profiles:
        print("[evaluator] no profiles loaded; skipping run")
        return

    validation = None
    try:
        validation = validate_profiles(profiles)
        print(
            f"[evaluator] validate ok={validation.ok} "
            f"errors={len(validation.errors or []) if hasattr(validation, 'errors') else 'n/a'}"
        )

        if not validation.ok and not allow_invalid:
            print("[evaluator] validation failed; set EVALUATOR_ALLOW_INVALID=1 to force run")
            return

    except Exception as e:
        print(f"[evaluator] validate crashed: {repr(e)}")
        try:
            traceback.print_exc()
        except Exception:
            pass

        if not allow_invalid:
            print("[evaluator] validation crashed; set EVALUATOR_ALLOW_INVALID=1 to force run anyway")
            return
        print("[evaluator] allow_invalid=1 -> continuing despite validation crash")


    summary = engine.run(profiles)
    print(
        "[evaluator] run summary profiles=%d groups=%d symbols=%d rows=%d unique_requests=%d pushes=%d events=%d"
        % (
            summary.profiles,
            summary.groups,
            summary.symbols,
            summary.rows,
            summary.unique_requests,
            summary.pushes,
            summary.events,
        )
    )


def main() -> None:
    _prime_sys_path()

    parser = argparse.ArgumentParser(description="Run the notifier evaluator loop")
    parser.add_argument("--once", action="store_true", help="Run a single evaluation cycle")
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
    print(f"[DEBUG] indicator_base_url={indicator_base_url!r}")

    profiles_path = Path(args.profiles)
    status_path = Path(args.status)
    history_path = Path(args.history)
    mapping_path = Path(args.mapping) if args.mapping else None

    engine = _build_engine(
        indicator_base_url=indicator_base_url,
        status_path=status_path,
        history_path=history_path,
        mapping_path=mapping_path,
    )

    allow_invalid = os.getenv("EVALUATOR_ALLOW_INVALID", "0") in ("1", "true", "True")

    if args.once:
        _run_once(profiles_path, engine, allow_invalid)
        return

    interval = max(1.0, float(args.interval))
    print(f"[evaluator] loop start interval={interval}s profiles={profiles_path}")
    while True:
        try:
            _run_once(profiles_path, engine, allow_invalid)
        except Exception as e:
            print(f"[evaluator] run failed: {e}")
            try:
                traceback.print_exc()
            except Exception:
                pass
        time.sleep(interval)


if __name__ == "__main__":
    main()
