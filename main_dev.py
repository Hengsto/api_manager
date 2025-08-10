# main_dev.py
import time
from notifier.evaluator import run_check
# from alarm_checker import run_alarm_checker  # später aktivieren

if __name__ == "__main__":
    while True:
        print("🔍 Evaluator läuft ...")
        triggered = run_check()
        print(f"✅ {len(triggered)} Bedingung(en) wurden erfüllt.")
        # run_alarm_checker(triggered)  # später aktivieren
        time.sleep(10)
