# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# ENV laden
# ─────────────────────────────────────────────────────────────
load_dotenv()

# ─────────────────────────────────────────────────────────────
# Basisverzeichnisse
# ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

PROFILE_PATH = DATA_DIR / "profiles"

# ─────────────────────────────────────────────────────────────
# API / Netzwerk
# ─────────────────────────────────────────────────────────────
MAIN_IP            = os.getenv("MAIN_IP", "127.0.0.1")
NOTIFIER_ENDPOINT  = f"http://{MAIN_IP}:8099/notifier"

# ─────────────────────────────────────────────────────────────
# JSON-Dateien
# ─────────────────────────────────────────────────────────────

PROFILES_NOTIFIER = PROFILE_PATH / "notifier" / "notifier_profiles.json"
ALARMS_NOTIFIER   = PROFILE_PATH / "notifier" / "notifier_alarms.json"


# ─────────────────────────────────────────────────────────────
# GPT / Bewertung
# ─────────────────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_MODEL      = os.getenv("LLM_MODEL", "gpt-4o-mini")
MIN_SCORE      = float(os.getenv("MIN_SCORE", 0.55))
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", 6))

# ─────────────────────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
AUTHORIZED_USERS   = [int(u) for u in os.getenv("AUTHORIZED_USERS", "").split(",") if u]

