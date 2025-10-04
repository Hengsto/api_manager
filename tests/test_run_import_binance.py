import importlib
import sys
import types
from pathlib import Path


# Ensure the project root (containing `registry_manager`) is importable when tests
# execute from a different working directory context.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_run_import_accepts_binance(monkeypatch):
    monkeypatch.setenv("API_KEY_EODHD", "dummy")
    monkeypatch.setenv("BINANCE_USE_SAMPLE_DATA", "1")
    monkeypatch.setenv("REGISTRY_ENDPOINT", "http://example.test")
    monkeypatch.setenv("LOG_LEVEL", "ERROR")

    # Provide lightweight stubs for third-party HTTP clients when dependencies are
    # not installed in the test environment.
    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")

        class _DummyResponse:
            def __init__(self):
                self.status_code = 200

            def json(self):
                return {}

            def raise_for_status(self):
                return None

        class _DummySession:
            def get(self, *args, **kwargs):
                return _DummyResponse()

            def post(self, *args, **kwargs):
                return _DummyResponse()

            def mount(self, *args, **kwargs):
                return None

        requests_stub.Session = _DummySession
        sys.modules["requests"] = requests_stub

        adapters_stub = types.ModuleType("requests.adapters")

        class _DummyHTTPAdapter:
            def __init__(self, *args, **kwargs):
                pass

        adapters_stub.HTTPAdapter = _DummyHTTPAdapter
        sys.modules["requests.adapters"] = adapters_stub

    if "urllib3.util.retry" not in sys.modules:
        retry_stub = types.ModuleType("urllib3.util.retry")

        class _DummyRetry:
            def __init__(self, *args, **kwargs):
                pass

        retry_stub.Retry = _DummyRetry
        sys.modules["urllib3.util.retry"] = retry_stub

        util_stub = types.ModuleType("urllib3.util")
        util_stub.retry = retry_stub
        sys.modules["urllib3.util"] = util_stub

        urllib3_stub = types.ModuleType("urllib3")
        urllib3_stub.util = util_stub
        sys.modules["urllib3"] = urllib3_stub

    # Import pipeline after env configuration so the adapter can read overrides
    pipeline = importlib.import_module("registry_manager.pipeline")

    class DummyRegistry:
        def __init__(self, base: str | None = None, timeout: float = 15.0):
            self.base = base
            self.timeout = timeout

        def health(self):
            return {"ok": True, "engine": "dummy"}

        def search(self, q: str, limit: int = 50):
            return {}

        def create_asset(self, payload):
            return payload

        def add_listing(self, asset_id: str, listing):
            return {"ok": True}

        def upsert_identifier(self, asset_id: str, key: str, value: str):
            return None

    monkeypatch.setattr(pipeline, "RegistryClient", DummyRegistry)

    # Reload run_import to pick up patched RegistryClient
    run_import = importlib.import_module("registry_manager.run_import")

    test_args = [
        "run_import.py",
        "--source",
        "binance",
        "--exchanges",
        "BINANCE",
        "--limit",
        "2",
        "--dry-run",
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    # The smoke test ensures argparse accepts `--source binance` without raising errors.
    # A SystemExit would indicate an argparse failure.
    run_import.main()
