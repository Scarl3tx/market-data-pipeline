
import json

import pytest

from market_data.pipelines.twelvedata.bronze.state import (
    _load_state,
    _save_state,
    initialize_symbols,
    get_symbol_state,
    mark_recent_complete,
    mark_backfill_complete,
)


def test_missing_state_returns_empty_structure(tmp_path):
    state_path = tmp_path / "state.json"

    assert _load_state(state_path) == {
        "symbols": {},
    }


def test_initialize_symbols_creates_default_checkpoints(tmp_path):
    state_path = tmp_path / "state.json"

    initialize_symbols(
        state_path,
        ["NVDA", "AMD"],
    )

    state = _load_state(state_path)

    assert state["symbols"] == {
        "NVDA": {
            "recent_complete": False,
            "backfill_complete": False,
        },
        "AMD": {
            "recent_complete": False,
            "backfill_complete": False,
        },
    }


def test_initialize_symbols_preserves_existing_checkpoints(tmp_path):
    state_path = tmp_path / "state.json"

    _save_state(
        state_path,
        {
            "symbols": {
                "NVDA": {
                    "recent_complete": True,
                    "backfill_complete": False,
                },
            },
        },
    )

    initialize_symbols(
        state_path,
        ["NVDA", "AMD"],
    )

    state = _load_state(state_path)

    assert state["symbols"]["NVDA"]["recent_complete"] is True
    assert state["symbols"]["NVDA"]["backfill_complete"] is False
    assert state["symbols"]["AMD"]["recent_complete"] is False


def test_initialize_symbols_preserves_additional_fields(tmp_path):
    state_path = tmp_path / "state.json"

    _save_state(
        state_path,
        {
            "metadata": {
                "version": 1,
            },
            "symbols": {
                "NVDA": {
                    "recent_complete": True,
                    "backfill_complete": False,
                    "custom_field": "preserve",
                },
            },
        },
    )

    initialize_symbols(
        state_path,
        ["NVDA"],
    )

    state = _load_state(state_path)

    assert state["metadata"] == {"version": 1}
    assert state["symbols"]["NVDA"]["custom_field"] == "preserve"


def test_missing_checkpoint_flags_default_to_false(tmp_path):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        json.dumps({
            "symbols": {
                "NVDA": {},
            },
        }),
        encoding="utf-8",
    )

    assert get_symbol_state(
        state_path,
        "NVDA",
    ) == {
        "recent_complete": False,
        "backfill_complete": False,
    }


def test_missing_symbols_mapping_defaults_to_empty(tmp_path):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        json.dumps({"metadata": {"version": 1}}),
        encoding="utf-8",
    )

    assert _load_state(state_path) == {
        "metadata": {"version": 1},
        "symbols": {},
    }


def test_get_unknown_symbol_returns_default_without_saving(
    tmp_path,
):
    state_path = tmp_path / "state.json"

    initialize_symbols(
        state_path,
        ["NVDA"],
    )

    result = get_symbol_state(
        state_path,
        "AMD",
    )

    assert result == {
        "recent_complete": False,
        "backfill_complete": False,
    }

    assert "AMD" not in _load_state(state_path)["symbols"]


def test_get_symbol_state_returns_copy(tmp_path):
    state_path = tmp_path / "state.json"

    initialize_symbols(
        state_path,
        ["NVDA"],
    )

    result = get_symbol_state(
        state_path,
        "NVDA",
    )

    result["recent_complete"] = True

    assert get_symbol_state(
        state_path,
        "NVDA",
    )["recent_complete"] is False


def test_mark_recent_complete(tmp_path):
    state_path = tmp_path / "state.json"

    initialize_symbols(
        state_path,
        ["NVDA"],
    )

    mark_recent_complete(
        state_path,
        "NVDA",
    )

    assert get_symbol_state(
        state_path,
        "NVDA",
    ) == {
        "recent_complete": True,
        "backfill_complete": False,
    }


def test_mark_backfill_complete(tmp_path):
    state_path = tmp_path / "state.json"

    initialize_symbols(
        state_path,
        ["NVDA"],
    )

    mark_backfill_complete(
        state_path,
        "NVDA",
    )

    assert get_symbol_state(
        state_path,
        "NVDA",
    ) == {
        "recent_complete": False,
        "backfill_complete": True,
    }


def test_mark_functions_create_missing_symbol(tmp_path):
    state_path = tmp_path / "state.json"

    mark_recent_complete(
        state_path,
        "NVDA",
    )

    mark_backfill_complete(
        state_path,
        "AMD",
    )

    assert get_symbol_state(
        state_path,
        "NVDA",
    ) == {
        "recent_complete": True,
        "backfill_complete": False,
    }

    assert get_symbol_state(
        state_path,
        "AMD",
    ) == {
        "recent_complete": False,
        "backfill_complete": True,
    }


def test_invalid_json_raises_value_error(tmp_path):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        "{invalid json",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid TwelveData"):
        _load_state(state_path)


@pytest.mark.parametrize(
    "payload",
    [
        [],
        "invalid",
        123,
        None,
    ],
)
def test_invalid_root_raises_value_error(
    tmp_path,
    payload,
):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="root must be an object"):
        _load_state(state_path)


@pytest.mark.parametrize(
    "symbols",
    [
        [],
        None,
        "NVDA",
        123,
    ],
)
def test_invalid_symbols_mapping_raises_value_error(
    tmp_path,
    symbols,
):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        json.dumps({"symbols": symbols}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="'symbols' must be an object"):
        _load_state(state_path)


@pytest.mark.parametrize(
    "symbol",
    [
        "",
        "   ",
        123,
    ],
)
def test_invalid_symbol_keys_raise_value_error(
    tmp_path,
    symbol,
):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        json.dumps({
            "symbols": {
                symbol: {},
            },
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="symbol keys"):
        _load_state(state_path)


@pytest.mark.parametrize(
    "checkpoint",
    [
        None,
        [],
        "invalid",
        123,
    ],
)
def test_invalid_checkpoint_type_raises_value_error(
    tmp_path,
    checkpoint,
):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        json.dumps({
            "symbols": {
                "NVDA": checkpoint,
            },
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid checkpoint"):
        _load_state(state_path)


@pytest.mark.parametrize(
    "flag,value",
    [
        ("recent_complete", 1),
        ("recent_complete", "true"),
        ("recent_complete", None),
        ("backfill_complete", 0),
        ("backfill_complete", "false"),
        ("backfill_complete", None),
    ],
)
def test_invalid_checkpoint_flags_raise_value_error(
    tmp_path,
    flag,
    value,
):
    state_path = tmp_path / "state.json"

    state_path.write_text(
        json.dumps({
            "symbols": {
                "NVDA": {
                    flag: value,
                },
            },
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must be a boolean"):
        _load_state(state_path)


def test_initialize_symbols_rejects_invalid_symbol_list(
    tmp_path,
):
    state_path = tmp_path / "state.json"

    with pytest.raises(ValueError, match="symbols must be a list"):
        initialize_symbols(
            state_path,
            "NVDA",
        )


@pytest.mark.parametrize(
    "symbols",
    [
        [""],
        ["NVDA", ""],
        ["NVDA", None],
        ["NVDA", 123],
    ],
)
def test_initialize_symbols_rejects_invalid_symbols(
    tmp_path,
    symbols,
):
    state_path = tmp_path / "state.json"

    with pytest.raises(ValueError, match="non-empty string"):
        initialize_symbols(
            state_path,
            symbols,
        )


def test_save_state_creates_parent_directories(tmp_path):
    state_path = tmp_path / "nested" / "state.json"

    _save_state(
        state_path,
        {"symbols": {}},
    )

    assert state_path.exists()


def test_save_state_writes_valid_json(tmp_path):
    state_path = tmp_path / "state.json"

    expected = {
        "symbols": {
            "NVDA": {
                "recent_complete": True,
                "backfill_complete": False,
            },
        },
    }

    _save_state(
        state_path,
        expected,
    )

    with state_path.open("r", encoding="utf-8") as file:
        assert json.load(file) == expected


def test_save_state_removes_temporary_files(tmp_path):
    state_path = tmp_path / "state.json"

    _save_state(
        state_path,
        {"symbols": {}},
    )

    temporary_files = list(
        tmp_path.glob("state.*.tmp")
    )

    assert temporary_files == []


def test_save_state_rejects_invalid_state(tmp_path):
    state_path = tmp_path / "state.json"

    with pytest.raises(ValueError, match="'symbols' must be an object"):
        _save_state(
            state_path,
            {"symbols": []},
        )

    assert not state_path.exists()
