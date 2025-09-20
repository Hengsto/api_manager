# config.py — unified, env-first, robuste Pfade
# -*- coding: utf-8 -*-
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Basics / Pfade ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PROFILE_DIR = DATA_DIR / "profiles" / "notifier"
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

# Unified-JSON: ENV hat Vorrang; sonst ./data/profiles/notifier/notifier.json
# Tipp (Docker): NOTIFIER_UNIFIED=/data/profiles/notifier/notifier.json
_ENV_UNIFIED = os.getenv("NOTIFIER_UNIFIED", "").strip()
if _ENV_UNIFIED:
    NOTIFIER_UNIFIED = _ENV_UNIFIED  # kann absolut/relativ sein
else:
    NOTIFIER_UNIFIED = str(PROFILE_DIR / "notifier_profiles.json")


# Für UI-Kompatibilität: historischer Alias (API nimmt das als Profile-Datei)
PROFILES_NOTIFIER = NOTIFIER_UNIFIED

# ── Runtime-Dateien (liegen neben NOTIFIER_UNIFIED) ───────────────────────────
_base = Path(NOTIFIER_UNIFIED).resolve().parent
_base.mkdir(parents=True, exist_ok=True)

STATUS_NOTIFIER    = str(_base / "notifier_status.json")
OVERRIDES_NOTIFIER = str(_base / "notifier_overrides.json")
COMMANDS_NOTIFIER  = str(_base / "notifier_commands.json")
ALARMS_NOTIFIER    = str(_base / "notifier_alarms.json")   # <-- ergänzt

# ── Endpoints (lokal als Default) ─────────────────────────────────────────────
MAIN_IP = os.getenv("MAIN_IP", "127.0.0.1")

# Eigener Notifier (UI spricht hierauf)
NOTIFIER_PORT = int(os.getenv("NOTIFIER_PORT", "8099"))
# WICHTIG: /notifier am Ende, damit Evaluator-PATCH /profiles/{pid}/groups/{gid}/active landet
NOTIFIER_ENDPOINT = f"http://{MAIN_IP}:{NOTIFIER_PORT}/notifier"

# Klassische Price-/Indicator-API
PRICE_API_HOST = os.getenv("PRICE_API_HOST", MAIN_IP)
PRICE_API_PORT = int(os.getenv("PRICE_API_PORT", "8000"))
PRICE_API_ENDPOINT = f"http://{PRICE_API_HOST}:{PRICE_API_PORT}"

# Chart-API (UI-kompatibler /indicator)
CHART_API_HOST = os.getenv("CHART_API_HOST", MAIN_IP)
CHART_API_PORT = int(os.getenv("CHART_API_PORT", "7004"))  # lokal: 7004
CHART_API_ENDPOINT = f"http://{CHART_API_HOST}:{CHART_API_PORT}"

# ── Steuerung ────────────────────────────────────────────────────────────────
def _truthy(key: str, default: str = "1") -> bool:
    return os.getenv(key, default).strip().lower() in {"1", "true", "yes", "on", "y"}

ENABLE_PROFILE_WATCH = _truthy("ENABLE_PROFILE_WATCH", "1")
ENABLE_EVALUATOR     = _truthy("ENABLE_EVALUATOR", "1")
EVALUATOR_INTERVAL   = float(os.getenv("EVALUATOR_INTERVAL", "60.0"))

# ── Evaluator/HTTP Tuning (env-first) ────────────────────────────────────────
EVAL_DEBUG_HTTP    = _truthy("EVAL_DEBUG_HTTP", "1")
EVAL_DEBUG_VALUES  = _truthy("EVAL_DEBUG_VALUES", "1")
EVAL_HTTP_TIMEOUT  = float(os.getenv("EVAL_HTTP_TIMEOUT", "15"))
EVAL_HTTP_RETRIES  = int(os.getenv("EVAL_HTTP_RETRIES", "3"))
EVAL_CACHE_MAX     = int(os.getenv("EVAL_CACHE_MAX", "256"))

# Optional: Mindest-Tick-Confirmation (Bar-Schließung) per Gruppe
# (Evaluator nutzt DEFAULT_MIN_TICK nur als Fallback)
EVAL_GROUP_MIN_TICK = int(os.getenv("EVAL_GROUP_MIN_TICK", "1"))

# ── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
try:
    TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0").strip() or "0")
except Exception:
    TELEGRAM_CHAT_ID = 0
AUTHORIZED_USERS = [
    int(x) for x in os.getenv("AUTHORIZED_USERS", "").split(",") if x.strip().isdigit()
]

# ── CORS (für Web-UIs) ───────────────────────────────────────────────────────
# Beispiel: NOTIFIER_CORS_ORIGINS=http://localhost:8050,http://127.0.0.1:8050
NOTIFIER_CORS_ORIGINS = os.getenv("NOTIFIER_CORS_ORIGINS", "http://localhost:8050")

# ── Locks ────────────────────────────────────────────────────────────────────
# ENV > TMPDIR > TEMP > TMP > System-Temp
_lock_base = (
    os.getenv("NOTIFIER_LOCK_DIR")
    or os.getenv("TMPDIR")
    or os.getenv("TEMP")
    or os.getenv("TMP")
    or (str(Path(os.getenv("LOCALAPPDATA", Path.home())) / "Temp") if os.name == "nt" else "/tmp")
)
NOTIFIER_LOCK_DIR = Path(_lock_base) / "notifier_locks"
NOTIFIER_LOCK_DIR.mkdir(parents=True, exist_ok=True)

# ── Debug-Ausgabe (optional) ─────────────────────────────────────────────────
if _truthy("CONFIG_DEBUG_PRINT", "0"):
    print(f"[CFG] NOTIFIER_UNIFIED     = {NOTIFIER_UNIFIED}")
    print(f"[CFG] PROFILES_NOTIFIER    = {PROFILES_NOTIFIER}")
    print(f"[CFG] STATUS_NOTIFIER      = {STATUS_NOTIFIER}")
    print(f"[CFG] OVERRIDES_NOTIFIER   = {OVERRIDES_NOTIFIER}")
    print(f"[CFG] COMMANDS_NOTIFIER    = {COMMANDS_NOTIFIER}")
    print(f"[CFG] ALARMS_NOTIFIER      = {ALARMS_NOTIFIER}")
    print(f"[CFG] NOTIFIER_ENDPOINT    = {NOTIFIER_ENDPOINT}")
    print(f"[CFG] PRICE_API_ENDPOINT   = {PRICE_API_ENDPOINT}")
    print(f"[CFG] CHART_API_ENDPOINT   = {CHART_API_ENDPOINT}")
    print(f"[CFG] ENABLE_EVALUATOR     = {ENABLE_EVALUATOR} (interval={EVALUATOR_INTERVAL}s)")
    print(f"[CFG] EVAL_HTTP_TIMEOUT    = {EVAL_HTTP_TIMEOUT}s retries={EVAL_HTTP_RETRIES} cache_max={EVAL_CACHE_MAX}")
    print(f"[CFG] NOTIFIER_LOCK_DIR    = {NOTIFIER_LOCK_DIR}")
    print(f"[CFG] NOTIFIER_CORS_ORIGINS= {NOTIFIER_CORS_ORIGINS}")
