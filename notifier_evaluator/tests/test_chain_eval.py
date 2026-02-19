# notifier_evaluator/tests/test_chain_eval.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pytest

from notifier_evaluator.eval.chain_eval import ChainEvalError, eval_chain
from notifier_evaluator.models.runtime import ConditionResult, TriState

def make_result(rid: str, state: TriState) -> ConditionResult:
    """Helper to create a ConditionResult."""
    return ConditionResult(
        rid=rid,
        state=state,
        op="gt",
        left_value=None,
        right_value=None
    )

def test_empty_chain():
    """Test evaluating an empty chain."""
    result = eval_chain([])
    assert result.partial_true is False
    assert result.final_state == TriState.UNKNOWN

def test_single_condition():
    """Test chain with a single condition."""
    cases = [
        (TriState.TRUE, True),
        (TriState.FALSE, False),
        (TriState.UNKNOWN, False)
    ]
    for state, expected_partial in cases:
        result = eval_chain([make_result("r1", state)])
        assert result.partial_true is expected_partial
        assert result.final_state == state

def test_and_chain():
    """Test AND chain combinations."""
    cases = [
        # (states, expected_final, expected_partial)
        ([TriState.TRUE, TriState.TRUE], TriState.TRUE, True),
        ([TriState.TRUE, TriState.FALSE], TriState.FALSE, True),
        ([TriState.TRUE, TriState.UNKNOWN], TriState.UNKNOWN, True),
        ([TriState.FALSE, TriState.TRUE], TriState.FALSE, True),
        ([TriState.FALSE, TriState.FALSE], TriState.FALSE, False),
        ([TriState.FALSE, TriState.UNKNOWN], TriState.FALSE, False),
        ([TriState.UNKNOWN, TriState.TRUE], TriState.UNKNOWN, True),
        ([TriState.UNKNOWN, TriState.FALSE], TriState.FALSE, False),
        ([TriState.UNKNOWN, TriState.UNKNOWN], TriState.UNKNOWN, False),
    ]

    for states, expected_final, expected_partial in cases:
        results = [make_result(f"r{i}", state) for i, state in enumerate(states)]
        logic = ["and"] * len(results)
        result = eval_chain(results, logic_to_prev=logic)
        assert result.final_state == expected_final
        assert result.partial_true == expected_partial

def test_or_chain():
    """Test OR chain combinations."""
    cases = [
        # (states, expected_final, expected_partial)
        ([TriState.TRUE, TriState.TRUE], TriState.TRUE, True),
        ([TriState.TRUE, TriState.FALSE], TriState.TRUE, True),
        ([TriState.TRUE, TriState.UNKNOWN], TriState.TRUE, True),
        ([TriState.FALSE, TriState.TRUE], TriState.TRUE, True),
        ([TriState.FALSE, TriState.FALSE], TriState.FALSE, False),
        ([TriState.FALSE, TriState.UNKNOWN], TriState.UNKNOWN, False),
        ([TriState.UNKNOWN, TriState.TRUE], TriState.TRUE, True),
        ([TriState.UNKNOWN, TriState.FALSE], TriState.UNKNOWN, False),
        ([TriState.UNKNOWN, TriState.UNKNOWN], TriState.UNKNOWN, False),
    ]

    for states, expected_final, expected_partial in cases:
        results = [make_result(f"r{i}", state) for i, state in enumerate(states)]
        logic = ["or"] * len(results)
        result = eval_chain(results, logic_to_prev=logic)
        assert result.final_state == expected_final
        assert result.partial_true == expected_partial

def test_mixed_chain():
    """Test mixed AND/OR chain."""
    results = [
        make_result("r1", TriState.TRUE),    # TRUE
        make_result("r2", TriState.FALSE),   # TRUE OR FALSE = TRUE
        make_result("r3", TriState.TRUE),    # TRUE AND TRUE = TRUE
        make_result("r4", TriState.UNKNOWN), # TRUE OR UNKNOWN = TRUE
    ]
    logic = ["or", "and", "or"]
    result = eval_chain(results, logic_to_prev=logic)
    assert result.final_state == TriState.TRUE
    assert result.partial_true is True

def test_invalid_logic():
    """Test invalid logic operators."""
    results = [
        make_result("r1", TriState.TRUE),
        make_result("r2", TriState.TRUE),
    ]
    with pytest.raises(ChainEvalError) as exc:
        eval_chain(results, logic_to_prev=["invalid"])
    assert "Invalid logic operator" in str(exc.value)

def test_logic_length_mismatch():
    """Test logic_to_prev length validation."""
    results = [
        make_result("r1", TriState.TRUE),
        make_result("r2", TriState.TRUE),
        make_result("r3", TriState.TRUE),
    ]
    with pytest.raises(ChainEvalError) as exc:
        eval_chain(results, logic_to_prev=["and"])  # Too short
    assert "logic_to_prev length" in str(exc.value)

def test_debug_steps():
    """Test debug step recording."""
    results = [
        make_result("r1", TriState.TRUE),
        make_result("r2", TriState.FALSE),
    ]
    result = eval_chain(results, logic_to_prev=["and"])
    steps = result.debug["steps"]
    assert len(steps) == 2
    assert steps[0]["rid"] == "r1"
    assert steps[0]["state"] == "true"
    assert steps[1]["rid"] == "r2"
    assert steps[1]["logic"] == "and"
    assert steps[1]["before"] == "true"
    assert steps[1]["cur"] == "false"
    assert steps[1]["after"] == "false"