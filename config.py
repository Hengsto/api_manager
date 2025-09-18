# config.py — unified, env-first, robuste Pfade
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Basics / Pfade ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PROFILE_DIR = DATA_DIR / "profiles" / "notifier"
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

# Unified-JSON: ENV hat Vorrang; sonst unter ./data/profiles/notifier/notifier.json
# Tipp: In Docker docker-compose: NOTIFIER_UNIFIED=/data/profiles/notifier/notifier.json
_ENV_UNIFIED = os.getenv("NOTIFIER_UNIFIED", "").strip()
if _ENV_UNIFIED:
    NOTIFIER_UNIFIED = _ENV_UNIFIED  # benutze exakt den ENV-Pfad (absolut oder relativ zum CWD im Container)
else:
    NOTIFIER_UNIFIED = str(PROFILE_DIR / "notifier.json")  # sauberer lokaler Fallback

PROFILES_NOTIFIER = NOTIFIER_UNIFIED


# ── Endpoints (lokal als Default) ─────────────────────────────────────────────
MAIN_IP = os.getenv("MAIN_IP", "127.0.0.1")

# Eigener Notifier (UI spricht hierauf)
NOTIFIER_PORT = int(os.getenv("NOTIFIER_PORT", "8099"))
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
    return os.getenv(key, default).lower() in {"1", "true", "yes", "on"}

ENABLE_PROFILE_WATCH = _truthy("ENABLE_PROFILE_WATCH", "1")
ENABLE_EVALUATOR     = _truthy("ENABLE_EVALUATOR", "1")
EVALUATOR_INTERVAL   = float(os.getenv("EVALUATOR_INTERVAL", "60.0"))

# ── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
AUTHORIZED_USERS   = [int(x) for x in os.getenv("AUTHORIZED_USERS", "").split(",") if x.strip().isdigit()]

# ── Locks ────────────────────────────────────────────────────────────────────
# ENV > TMPDIR > TEMP > TMP > /tmp
_lock_base = (
    os.getenv("NOTIFIER_LOCK_DIR")
    or os.getenv("TMPDIR")
    or os.getenv("TEMP")
    or os.getenv("TMP")
    or "/tmp"
)
NOTIFIER_LOCK_DIR = Path(_lock_base) / "notifier_locks"
NOTIFIER_LOCK_DIR.mkdir(parents=True, exist_ok=True)