# alarm_checker.py

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import requests
import time

def _escape_markdown(text: str) -> str:
    # Minimal-escaping für Telegram Markdown (einfach gehalten)
    return (text
            .replace("_", "\\_")
            .replace("*", "\\*")
            .replace("[", "\\[")
            .replace("`", "\\`"))

def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": _escape_markdown(text),
        "parse_mode": "Markdown"  # Bei Bedarf auf MarkdownV2 umstellen und Escape erweitern
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print("⚠️ Telegram Error:", response.text)
        response.raise_for_status()
        print("[DEBUG] Telegram message sent")
    except Exception as e:
        print("💥 Telegram Exception:", e)

def run_alarm_checker(triggered: list[dict]):
    print(f"🔔 {len(triggered)} Alarm(e) prüfen...")
    for alarm in triggered:
        text = f"""
🚨 *Alarm ausgelöst*
*Profil:* {alarm.get('profile_name')}
*Symbol:* {alarm.get('symbol')}
*Bedingung:* `{alarm.get('condition')}`
"""
        send_telegram_message(text.strip())
        # Optional: kleines Delay, um Rate-Limits zu vermeiden
        time.sleep(0.2)
