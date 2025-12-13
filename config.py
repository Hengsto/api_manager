# config.py — unified, env-first, robuste Pfade
# -*- coding: utf-8 -*-
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Basics / Pfade ────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# Notifier-Daten (alles unter data/notifier)
NOTIFIER_DATA_DIR = DATA_DIR / "notifier"
NOTIFIER_DATA_DIR.mkdir(parents=True, exist_ok=True)



# Haupt-JSON-Dateien des Notifier-Stacks
PROFILES_NOTIFIER  = str(NOTIFIER_DATA_DIR / "notifier_profiles.json")
STATUS_NOTIFIER    = str(NOTIFIER_DATA_DIR / "notifier_status.json")
OVERRIDES_NOTIFIER = str(NOTIFIER_DATA_DIR / "notifier_overrides.json")
COMMANDS_NOTIFIER  = str(NOTIFIER_DATA_DIR / "notifier_commands.json")
ALARMS_NOTIFIER    = str(NOTIFIER_DATA_DIR / "notifier_alarms.json")

# Registry Manager (Asset- & Group-Registry)
REGISTRY_MANAGER_DATA_DIR = DATA_DIR / "registry_manager"
REGISTRY_MANAGER_DATA_DIR.mkdir(parents=True, exist_ok=True)
REGISTRY_MANAGER_DB = str(REGISTRY_MANAGER_DATA_DIR / "registry.sqlite")

# Evaluator-Daten
EVALUATOR_DATA_DIR = DATA_DIR / "evaluator"
EVALUATOR_DATA_DIR.mkdir(parents=True, exist_ok=True)



# ── Endpoints (lokal als Default) ─────────────────────────────────────────────
MAIN_IP = os.getenv("MAIN_IP", "127.0.0.1")

# Ports
NOTIFIER_PORT = int(os.getenv("NOTIFIER_PORT", "8098"))
REGISTRY_MANAGER_PORT = int(os.getenv("REGISTRY_MANAGER_PORT", "8098"))  # nur relevant, wenn NICHT gemountet

# Notifier
NOTIFIER_ENDPOINT = f"http://{MAIN_IP}:{NOTIFIER_PORT}/notifier"

# Price API
PRICE_API_HOST = os.getenv("PRICE_API_HOST", MAIN_IP)
PRICE_API_PORT = int(os.getenv("PRICE_API_PORT", "8000"))
PRICE_API_ENDPOINT = f"http://{PRICE_API_HOST}:{PRICE_API_PORT}"

# Chart-API
CHART_API_HOST = os.getenv("CHART_API_HOST", MAIN_IP)
CHART_API_PORT = int(os.getenv("CHART_API_PORT", "7004"))  # lokal: 7004
CHART_API_ENDPOINT = f"http://{CHART_API_HOST}:{CHART_API_PORT}"





# ── Steuerung ────────────────────────────────────────────────────────────────

# Evaluator

# ── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
try:
    TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0").strip() or "0")
except Exception:
    TELEGRAM_CHAT_ID = 0
AUTHORIZED_USERS = [
    int(x) for x in os.getenv("AUTHORIZED_USERS", "").split(",") if x.strip().isdigit()
]

# ── CORS (für Web-UIs) ───────────────────────
