# notifier_evaluator/tests/test_runtime.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import pytest

from notifier_evaluator.models.runtime import (
    ChainResult,
    ConditionResult,
    FetchResult,
    HistoryEvent,
    ResolvedContext,
    RuntimeValidationError,
    StatusKey,
    StatusState,
    TriState,
)

def test_resolved_context_validation():
    """Test ResolvedContext validation."""
    # Valid case
    ctx = ResolvedContext(
        symbol="BTCUSDT",
        interval="1h",
        exchange="binance",
        clock_interval="5m"
    )
    assert ctx.symbol == "BTCUSDT"

    # Empty symbol
    with pytest.raises(RuntimeValidationError) as exc:
        ResolvedContext(
            symbol="",
            interval="1h",
            exchange="binance",
            clock_interval="5m"
        )
    assert "Symbol cannot be empty" in str(exc.value)

    # Invalid interval
    with pytest.raises(RuntimeValidationError) as exc:
        ResolvedContext(
            symbol="BTCUSDT",
            interval="invalid",
            exchange="binance",
            clock_interval="5m"
        )
    assert "Invalid interval" in str(exc.value)

    # Invalid exchange
    with pytest.raises(RuntimeValidationError) as exc:
        ResolvedContext(
            symbol="BTCUSDT",
            interval="1h",
            exchange="invalid",
            clock_interval="5m"
        )
    assert "Invalid exchange" in str(exc.value)

def test_status_key_validation():
    """Test StatusKey validation."""
    # Valid case
    key = StatusKey(
        profile_id="p1",
        gid="g1",
        symbol="BTCUSDT",
        exchange="binance",
        clock_interval="5m"
    )
    assert key.profile_id == "p1"

    # Empty profile_id
    with pytest.raises(RuntimeValidationError) as exc:
        StatusKey(
            profile_id="",
            gid="g1",
            symbol="BTCUSDT",
            exchange="binance",
            clock_interval="5m"
        )
    assert "profile_id cannot be empty" in str(exc.value)

    # Empty gid
    with pytest.raises(RuntimeValidationError) as exc:
        StatusKey(
            profile_id="p1",
            gid="",
            symbol="BTCUSDT",
            exchange="binance",
            clock_interval="5m"
        )
    assert "gid cannot be empty" in str(exc.value)

def test_fetch_result_validation():
    """Test FetchResult validation."""
    # Valid case
    result = FetchResult(
        ok=True,
        latest_value=100.0,
        latest_ts="2024-02-18T00:00:00Z"
    )
    assert result.ok is True

    # Invalid latest_value (NaN)
    with pytest.raises(RuntimeValidationError) as exc:
        FetchResult(
            ok=True,
            latest_value=float('nan'),
            latest_ts="2024-02-18T00:00:00Z"
        )
    assert "must be a finite number" in str(exc.value)

    # Invalid latest_value (Infinity)
    with pytest.raises(RuntimeValidationError) as exc:
        FetchResult(
            ok=True,
            latest_value=float('inf'),
            latest_ts="2024-02-18T00:00:00Z"
        )
    assert "must be a finite number" in str(exc.value)

    # Invalid series type
    with pytest.raises(RuntimeValidationError) as exc:
        FetchResult(
            ok=True,
            latest_value=100.0,
            latest_ts="2024-02-18T00:00:00Z",
            series="not a list"  # type: ignore
        )
    assert "series must be a list" in str(exc.value)

def test_status_state_validation():
    """Test StatusState validation."""
    # Valid case
    state = StatusState(
        active=True,
        streak_current=5,
        last_true_ts="2024-02-18T00:00:00Z"
    )
    assert state.active is True

    # Negative streak
    with pytest.raises(RuntimeValidationError) as exc:
        StatusState(streak_current=-1)
    assert "streak_current cannot be negative" in str(exc.value)

    # Invalid count_window type
    with pytest.raises(RuntimeValidationError) as exc:
        StatusState(count_window="not a list")  # type: ignore
    assert "count_window must be a list" in str(exc.value)

    # Invalid last_final_state type
    with pytest.raises(RuntimeValidationError) as exc:
        StatusState(last_final_state="not a TriState")  # type: ignore
    assert "last_final_state must be a TriState" in str(exc.value)

def test_history_event_validation():
    """Test HistoryEvent validation."""
    # Valid case
    event = HistoryEvent(
        ts="2024-02-18T00:00:00Z",
        profile_id="p1",
        gid="g1",
        symbol="BTCUSDT",
        exchange="binance",
        event="eval"
    )
    assert event.event == "eval"

    # Empty timestamp
    with pytest.raises(RuntimeValidationError) as exc:
        HistoryEvent(
            ts="",
            profile_id="p1",
            gid="g1",
            symbol="BTCUSDT",
            exchange="binance",
            event="eval"
        )
    assert "Invalid timestamp" in str(exc.value)

    # Invalid left_value
    with pytest.raises(RuntimeValidationError) as exc:
        HistoryEvent(
            ts="2024-02-18T00:00:00Z",
            profile_id="p1",
            gid="g1",
            symbol="BTCUSDT",
            exchange="binance",
            event="eval",
            left_value=float('nan')
        )
    assert "left_value must be a finite number" in str(exc.value)

    # Invalid right_value
    with pytest.raises(RuntimeValidationError) as exc:
        HistoryEvent(
            ts="2024-02-18T00:00:00Z",
            profile_id="p1",
            gid="g1",
            symbol="BTCUSDT",
            exchange="binance",
            event="eval",
            right_value=float('inf')
        )
    assert "right_value must be a finite number" in str(exc.value)