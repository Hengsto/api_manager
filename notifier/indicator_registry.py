# indicators/registry.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List

# Einfache Signale/Events ohne Parameter
SIMPLE_SIGNALS: List[str] = [
    "golden_cross",
    "death_cross",
    "macd_cross",

]

# Zentrale Indikator-Registry
REGISTERED: Dict[str, Dict[str, Any]] = {
    "ema": {
        "name": "ema",
        "params": {
            "length": {"type": "int", "min": 1},
        },
        "outputs": ["EMA"],
        "enabled": True,
        "scopes": ["notifier", "screener"],
        "deprecated": False,
        "ui_hidden": False,
        "presets": [
            {"label": "EMA_14",  "params": {"length": 14},  "locked_params": ["length"]},
            {"label": "EMA_20",  "params": {"length": 20},  "locked_params": ["length"]},
            {"label": "EMA_50",  "params": {"length": 50},  "locked_params": ["length"]},
            {"label": "EMA_200", "params": {"length": 200}, "locked_params": ["length"]},
        ],
    },
    "rsi": {
        "name": "rsi",
        "params": {
            "length": {"type": "int", "min": 2},
        },
        "outputs": ["RSI"],
        "enabled": True,
        "scopes": ["notifier", "screener"],
        "deprecated": False,
        "ui_hidden": False,
        "presets": [
            {"label": "RSI_14", "params": {"length": 14}, "locked_params": ["length"]},
        ],
    },
    "macd": {
        "name": "macd",
        "params": {
            "fast":   {"type": "int", "min": 1, "default": 12},
            "slow":   {"type": "int", "min": 2, "default": 26},
            "signal": {"type": "int", "min": 1, "default": 9},
        },
        "outputs": ["MACD", "Signal", "Histogram"],
        "enabled": True,
        "scopes": ["screener", "notifier"],
        "deprecated": False,
        "ui_hidden": False,
        "presets": [
            {
                "label": "MACD_12_26_9",
                "params": {"fast": 12, "slow": 26, "signal": 9},
                "locked_params": ["fast", "slow", "signal"],
            }
        ],
    },
}
