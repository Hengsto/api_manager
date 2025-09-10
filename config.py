# config.py (kurz & knackig)
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ----- Basics / Pfade -----
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
(PROFILE_DIR := DATA_DIR / "profiles" / "notifier").mkdir(parents=True, exist_ok=True)

# JSON-Dateien
PROFILES_NOTIFIER  = PROFILE_DIR / "notifier_profiles.json"
ALARMS_NOTIFIER    = PROFILE_DIR / "notifier_alarms.json"
STATUS_NOTIFIER    = PROFILE_DIR / "notifier_status.json"
OVERRIDES_NOTIFIER = PROFILE_DIR / "notifier_overrides.json"
COMMANDS_NOTIFIER  = PROFILE_DIR / "notifier_commands.json"

# ----- Endpoints (lokal als Default) -----
MAIN_IP = os.getenv("MAIN_IP", "127.0.0.1")

# Eigener Notifier (UI spricht hierauf)
NOTIFIER_PORT = int(os.getenv("NOTIFIER_PORT", "8099"))
NOTIFIER_ENDPOINT = f"http://{MAIN_IP}:{NOTIFIER_PORT}/notifier"

# Klassische Price-/Indicator-API
PRICE_API_HOST = os.getenv("PRICE_API_HOST", MAIN_IP)
PRICE_API_PORT = int(os.getenv("PRICE_API_PORT", "8000"))
PRICE_API_ENDPOINT = f"http://{PRICE_API_HOST}:{PRICE_API_PORT}"

# Chart-API (UI-kompatibler /indicator)
CHART_API_HOST = os.getenv("CHART_API_HOST", os.getenv("MAIN_IP", "127.0.0.1"))
CHART_API_PORT = int(os.getenv("CHART_API_PORT", "7004"))  # lokal: 7004
CHART_API_ENDPOINT = f"http://{CHART_API_HOST}:{CHART_API_PORT}"

# ----- Steuerung -----
ENABLE_PROFILE_WATCH = os.getenv("ENABLE_PROFILE_WATCH", "1").lower() in {"1","true","yes","on"}
ENABLE_EVALUATOR     = os.getenv("ENABLE_EVALUATOR", "1").lower() in {"1","true","yes","on"}
EVALUATOR_INTERVAL   = float(os.getenv("EVALUATOR_INTERVAL", "60.0"))

# ----- Telegram -----
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
AUTHORIZED_USERS   = [int(x) for x in os.getenv("AUTHORIZED_USERS", "").split(",") if x.strip().isdigit()]

# ----- Optional: Locks (Default → Temp) -----
tmp = Path(os.getenv("NOTIFIER_LOCK_DIR") or os.getenv("TMPDIR") or os.getenv("TEMP") or os.getenv("TMP") or "/tmp") / "notifier_locks"
tmp.mkdir(parents=True, exist_ok=True)
NOTIFIER_LOCK_DIR = tmp

# ----- (Optional) LLM -----
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_MODEL      = os.getenv("LLM_MODEL", "gpt-4o-mini")
MIN_SCORE      = float(os.getenv("MIN_SCORE", "0.55"))
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "6"))
