# notifier/evaluator.py
import requests
import logging
from typing import List, Dict, Any
from config import NOTIFIER_ENDPOINT

# Operator-Mapping
OPS = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "gt": lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
    "lt": lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("notifier.evaluator")

# Hauptfunktion: prüft alle Profile
def run_check() -> List[Dict[str, Any]]:
    try:
        log.debug("Lade Profile...")
        resp = requests.get(f"{NOTIFIER_ENDPOINT}/profiles", timeout=10)
        resp.raise_for_status()
        profiles = resp.json()
        log.info(f"Profile geladen: {len(profiles)}")
    except Exception as e:
        log.error(f"⚠️ Fehler beim Laden der Profile: {e}")
        return []

    triggered: List[Dict[str, Any]] = []

    for profile in profiles:
        if not profile.get("enabled", True):
            log.debug(f"Profil deaktiviert: {profile.get('name')}")
            continue

        for group in profile.get("condition_groups", []):
            if not group.get("active", True):
                log.debug(f"Gruppe deaktiviert in Profil {profile.get('name')}")
                continue

            conditions = group.get("conditions", [])
            group_result = None
            per_condition_result: List[bool] = []

            for idx, cond in enumerate(conditions):
                res = evaluate_condition(cond)
                per_condition_result.append(res)

                if idx == 0:
                    group_result = res
                else:
                    logic = (cond.get("logic") or "and").lower()
                    if logic == "or":
                        group_result = bool(group_result or res)
                    else:
                        group_result = bool(group_result and res)

                log.debug(f"Profil '{profile.get('name')}' Gruppe-Cond[{idx}] -> {res} (logic={cond.get('logic','and')})")

            if group_result:
                # Bei erfolgreicher Gruppe melden wir alle Conditions, die True waren
                for idx, cond in enumerate(conditions):
                    if per_condition_result[idx]:
                        val_left, val_right = _current_values_for(cond)
                        payload = {
                            "profile_id": profile["id"],
                            "profile_name": profile["name"],
                            "symbol": cond.get("right_symbol", ""),
                            "condition": cond,
                            "value_left": val_left,
                            "value_right": val_right,
                        }
                        triggered.append(payload)
                        log.info(f"Ausgelöst: {payload}")

    log.info(f"Gesamt ausgelöste Bedingungen: {len(triggered)}")
    return triggered


# Prüft eine einzelne Bedingung
def evaluate_condition(cond: Dict[str, Any]) -> bool:
    op = cond.get("op")
    if op not in OPS:
        log.warning(f"❓ Unbekannter Operator: {op}")
        return False

    # Platzhalterwerte (du baust hier echte Werte später ein)
    val_left, val_right = _current_values_for(cond)

    try:
        result = OPS[op](val_left, val_right)
        log.debug(f"Evaluated {val_left} {op} {val_right} → {result}")
        return bool(result)
    except Exception as e:
        log.error(f"💥 Fehler bei Bedingungsauswertung: {e}")
        return False

def _current_values_for(cond: Dict[str, Any]) -> tuple[float, float]:
    """
    Liefert den linken und rechten Wert zur Auswertung basierend auf:
      - left: Name des linken Indikators
      - right: anderer Indikator oder leer
      - right_absolut: fixer Vergleichswert
      - right_change: Prozent-Vergleich zu vorherigem Wert
    """
    # Simulierter DataFrame (hier musst du deinen echten Feed anschließen)
    simulated_df = {
        "rsi": 60.0,
        "momentum": 5.0,
        "golden_cross": 1.0,
        "macd_divergence": 2.5,
        "rsi_divergence": 1.0,
    }

    left_name = cond.get("left")
    right_name = cond.get("right", "")
    right_abs = cond.get("right_absolut")
    right_change = cond.get("right_change")

    # 1. Left-Wert lesen (simuliert)
    val_left = simulated_df.get(left_name, 1.0)

    # 2. Vergleichswert berechnen
    if right_name:  # → Indikatorvergleich
        val_right = simulated_df.get(right_name, 1.0)
    elif right_abs is not None:  # → fester Wert
        val_right = right_abs
    else:  # Fallback
        val_right = 1.0

    # 3. Prozentuelle Anpassung (right_change)
    if right_change is not None:
        val_right = val_right * (1 + right_change / 100)

    print(f"[DEBUG] Auswertung: {left_name}={val_left} vs {val_right} (right={right_name}, abs={right_abs}, %={right_change})")
    return float(val_left), float(val_right)


def run_evaluator():
    print("🔄 Evaluator wird ausgeführt ...")
    results = run_check()
    print(f"✅ {len(results)} Bedingung(en) wurden erfüllt:")
