# notifier_evaluator/tests/test_normalize.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pytest

from notifier_evaluator.models.normalize import (
    NormalizationError,
    normalize_profile_dict,
)

def test_normalize_empty_profile():
    """Test normalizing an empty profile."""
    with pytest.raises(NormalizationError) as exc:
        normalize_profile_dict({}, default_exchange="binance", default_interval="1h")
    assert "groups" in str(exc.value)

def test_normalize_invalid_defaults():
    """Test handling invalid default values."""
    profile = {"groups": [{"gid": "test", "symbols": ["BTC"]}]}
    
    with pytest.raises(NormalizationError) as exc:
        normalize_profile_dict(profile, default_exchange="invalid", default_interval="1h")
    assert "Invalid exchange" in str(exc.value)

    with pytest.raises(NormalizationError) as exc:
        normalize_profile_dict(profile, default_exchange="binance", default_interval="invalid")
    assert "Invalid interval" in str(exc.value)

def test_normalize_group_inheritance():
    """Test exchange/interval inheritance in groups."""
    profile = {
        "groups": [
            {
                "gid": "g1",
                "symbols": ["BTC"],
                "exchange": "",  # inherit default
                "interval": "",  # inherit default
                "conditions": [
                    {
                        "rid": "r1",
                        "left": {"name": "price"},
                        "op": "gt",
                        "right": {"name": "value", "value": 100}
                    }
                ]
            }
        ]
    }

    result = normalize_profile_dict(profile, default_exchange="binance", default_interval="1h")
    group = result["groups"][0]
    assert group["exchange"] == "binance"
    assert group["interval"] == "1h"

def test_normalize_condition_inheritance():
    """Test exchange/interval inheritance in conditions."""
    profile = {
        "groups": [
            {
                "gid": "g1",
                "symbols": ["BTC"],
                "exchange": "binance",
                "interval": "1h",
                "conditions": [
                    {
                        "rid": "r1",
                        "left_exchange": "",  # inherit from group
                        "left_interval": "",  # inherit from group
                        "right_exchange": "binance_futures",  # override
                        "right_interval": "4h",  # override
                    }
                ]
            }
        ]
    }

    result = normalize_profile_dict(profile, default_exchange="binance", default_interval="1h")
    condition = result["groups"][0]["conditions"][0]
    assert condition["left_exchange"] == "binance"  # inherited from group
    assert condition["left_interval"] == "1h"  # inherited from group
    assert condition["right_exchange"] == "binance_futures"  # explicitly set
    assert condition["right_interval"] == "4h"  # explicitly set

def test_normalize_invalid_group_values():
    """Test handling invalid values in groups."""
    profile = {
        "groups": [
            {
                "gid": "g1",
                "symbols": ["BTC"],
                "exchange": "invalid",
                "interval": "1h",
                "conditions": []
            }
        ]
    }

    with pytest.raises(NormalizationError) as exc:
        normalize_profile_dict(profile, default_exchange="binance", default_interval="1h")
    assert "Invalid exchange" in str(exc.value)
    assert "Group g1" in str(exc.value)

def test_normalize_invalid_condition_values():
    """Test handling invalid values in conditions."""
    profile = {
        "groups": [
            {
                "gid": "g1",
                "symbols": ["BTC"],
                "exchange": "binance",
                "interval": "1h",
                "conditions": [
                    {
                        "rid": "r1",
                        "left_exchange": "binance",
                        "left_interval": "invalid",
                        "right_exchange": "binance",
                        "right_interval": "1h",
                    }
                ]
            }
        ]
    }

    with pytest.raises(NormalizationError) as exc:
        normalize_profile_dict(profile, default_exchange="binance", default_interval="1h")
    assert "Invalid interval" in str(exc.value)
    assert "Condition r1" in str(exc.value)

def test_normalize_condition_groups_mapping():
    """Test mapping of condition_groups to groups."""
    profile = {
        "condition_groups": [  # old key name
            {
                "gid": "g1",
                "symbols": ["BTC"],
                "exchange": "binance",
                "interval": "1h",
                "conditions": []
            }
        ]
    }

    result = normalize_profile_dict(profile, default_exchange="binance", default_interval="1h")
    assert "groups" in result
    assert len(result["groups"]) == 1
    assert result["groups"][0]["gid"] == "g1"