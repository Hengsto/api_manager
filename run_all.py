# run_all.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import signal
import subprocess
from pathlib import Path


def _py() -> str:
    # venv-safe: use current interpreter
    return sys.executable


def _spawn(name: str, script: str, extra_env: dict[str, str] | None = None) -> subprocess.Popen:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    cmd = [_py(), script]
    print(f"[RUN_ALL] spawn name={name} cmd={cmd} cwd={Path.cwd().resolve()}")
    if extra_env:
        print(f"[RUN_ALL] {name} extra_env={extra_env}")

    p = subprocess.Popen(cmd, env=env)
    print(f"[RUN_ALL] {name} pid={p.pid}")
    return p


def main() -> int:
    root = Path(__file__).resolve().parent
    os.chdir(root)
    print(f"[RUN_ALL] cwd={Path.cwd().resolve()} py={_py()}")

    api_script = "main_api.py"
    eval_script = "main_evaluator.py"

    if not Path(api_script).exists():
        print(f"[RUN_ALL] ERROR missing {api_script} in {root}")
        return 2
    if not Path(eval_script).exists():
        print(f"[RUN_ALL] ERROR missing {eval_script} in {root}")
        return 2

    # API must NOT start evaluator internally.
    # If you still have legacy env vars, this keeps it off.
    api_env = {"ENABLE_EVALUATOR": "0"}

    api = _spawn("api", api_script, api_env)

    # Give API a head start (bind port etc.)
    time.sleep(1.0)

    evl = _spawn("evaluator", eval_script, None)

    procs = {"api": api, "evaluator": evl}

    def _shutdown(signum=None, frame=None):
        print(f"[RUN_ALL] shutdown requested signum={signum}")
        for n, p in procs.items():
            if p.poll() is None:
                print(f"[RUN_ALL] terminating {n} pid={p.pid}")
                try:
                    p.terminate()
                except Exception as e:
                    print(f"[RUN_ALL] terminate failed {n}: {e}")

        time.sleep(2.0)
        for n, p in procs.items():
            if p.poll() is None:
                print(f"[RUN_ALL] killing {n} pid={p.pid}")
                try:
                    p.kill()
                except Exception as e:
                    print(f"[RUN_ALL] kill failed {n}: {e}")

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        time.sleep(1.0)
        for n, p in procs.items():
            rc = p.poll()
            if rc is not None:
                print(f"[RUN_ALL] PROCESS EXIT name={n} pid={p.pid} rc={rc}")
                _shutdown()
                return rc if rc != 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
