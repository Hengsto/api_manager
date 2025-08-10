# indicators/registry.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any, List

# Zentrale Indikator-Registry
REGISTERED: Dict[str, Dict[str, Any]] = {
    "ema": {
        "name": "ema",
        "params": {
            "length": {"type": "int", "min": 1},
            "source": {"type": "enum", "values": ["Open", "High", "Low", "Close"]},
        },
        "outputs": ["EMA"],
        "enabled": True,
        "scopes": ["notifier", "chart"],
        "deprecated": False,
        "ui_hidden": False,
        "presets": [
            {"label": "EMA_14",  "params": {"length": 14,  "source": "Close"}, "locked_params": ["length", "source"]},
            {"label": "EMA_20",  "params": {"length": 20,  "source": "Close"}, "locked_params": ["length", "source"]},
            {"label": "EMA_50",  "params": {"length": 50,  "source": "Close"}, "locked_params": ["length", "source"]},
            {"label": "EMA_200", "params": {"length": 200, "source": "Close"}, "locked_params": ["length", "source"]},
        ],
    },
    "rsi": {
        "name": "rsi",
        "params": {
            "length": {"type": "int", "min": 2},
            "source": {"type": "enum", "values": ["Close"]},
        },
        "outputs": ["RSI"],
        "enabled": True,
        "scopes": ["notifier", "chart"],
        "deprecated": False,
        "ui_hidden": False,
        "presets": [
            {"label": "RSI_14", "params": {"length": 14, "source": "Close"}, "locked_params": ["length", "source"]},
        ],
    },
    "macd": {
        "name": "macd",
        "params": {
            "fast":   {"type": "int", "min": 1, "default": 12},
            "slow":   {"type": "int", "min": 2, "default": 26},
            "signal": {"type": "int", "min": 1, "default": 9},
            "source": {"type": "enum", "values": ["Close"]},
        },
        "outputs": ["MACD", "Signal", "Histogram"],
        "enabled": True,
        "scopes": ["chart", "notifier"],
        "deprecated": False,
        "ui_hidden": False,
        "presets": [
            {
                "label": "MACD_12_26_9",
                "params": {"fast": 12, "slow": 26, "signal": 9, "source": "Close"},
                "locked_params": ["fast", "slow", "signal", "source"],
            }
        ],
    },
}

# Einfache Signale/Events ohne Parameter
SIMPLE_SIGNALS: List[str] = [
    "golden_cross",
    "death_cross",
    "rsi_overbought",
    "rsi_oversold",
    "macd_cross",
    "price_above_ema200",
    "price_below_ema200",
]
