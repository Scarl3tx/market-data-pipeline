
import json
import os
from pathlib import Path
from uuid import uuid4

from market_data.storage.base import StorageBackend


DEFAULT_SYMBOL_STATE = {
    "recent_complete": False,
    "backfill_complete": False,
}


def _validate_state(state: dict) -> dict:
    if not isinstance(state, dict):
        raise ValueError(
            "Invalid TwelveData ingestion state: root must be an object."
        )

    symbols = state.setdefault("symbols", {})

    if not isinstance(symbols, dict):
        raise ValueError(
            "Invalid TwelveData ingestion state: "
            "'symbols' must be an object."
        )

    for symbol, checkpoint in symbols.items():
        if (
            not isinstance(symbol, str)
            or not symbol.strip()
            or symbol.isdigit()
        ):
            raise ValueError(
                "Invalid TwelveData ingestion state: "
                "symbol keys must be non-empty strings."
            )

        if not isinstance(checkpoint, dict):
            raise ValueError(
                f"Invalid checkpoint for {symbol}: "
                "expected an object."
            )

        for flag in DEFAULT_SYMBOL_STATE:
            if flag in checkpoint and not isinstance(
                checkpoint[flag],
                bool,
            ):
                raise ValueError(
                    f"Invalid checkpoint for {symbol}: "
                    f"'{flag}' must be a boolean."
                )

            checkpoint.setdefault(flag, False)

    return state


def _load_state(
    state_path: str | Path,
    storage: StorageBackend | None = None,
) -> dict:
    if storage is not None:
        if not storage.exists(str(state_path)):
            return {"symbols": {}}

        try:
            state = storage.read_json(str(state_path))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(
                f"Invalid TwelveData ingestion state JSON: {state_path}"
            ) from exc

        return _validate_state(state)

    state_path = Path(state_path)

    if not state_path.exists():
        return {"symbols": {}}

    try:
        with state_path.open(
            "r",
            encoding="utf-8-sig",
        ) as file:
            state = json.load(file)

    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Invalid TwelveData ingestion state JSON: {state_path}"
        ) from exc

    return _validate_state(state)


def _save_state(
    state_path: str | Path,
    state: dict,
    storage: StorageBackend | None = None,
) -> None:
    _validate_state(state)

    if storage is not None:
        storage.write_json(
            str(state_path),
            state,
            overwrite=True,
        )
        return

    state_path = Path(state_path)

    state_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = state_path.with_name(
        f"{state_path.stem}.{uuid4().hex}.tmp"
    )

    try:
        with temporary_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                state,
                file,
                indent=4,
            )

            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary_path,
            state_path,
        )

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def initialize_symbols(
    state_path: str | Path,
    symbols: list[str],
    storage: StorageBackend | None = None,
) -> None:
    if not isinstance(symbols, list):
        raise ValueError("symbols must be a list.")

    for symbol in symbols:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(
                "Every symbol must be a non-empty string."
            )

    state = _load_state(
        state_path,
        storage=storage,
    )

    for symbol in symbols:
        state["symbols"].setdefault(
            symbol,
            DEFAULT_SYMBOL_STATE.copy(),
        )

    _save_state(
        state_path,
        state,
        storage=storage,
    )


def get_symbol_state(
    state_path: str | Path,
    symbol: str,
    storage: StorageBackend | None = None,
) -> dict:
    state = _load_state(
        state_path,
        storage=storage,
    )

    checkpoint = state["symbols"].get(
        symbol,
        DEFAULT_SYMBOL_STATE,
    )

    return checkpoint.copy()


def mark_recent_complete(
    state_path: str | Path,
    symbol: str,
    storage: StorageBackend | None = None,
) -> None:
    state = _load_state(
        state_path,
        storage=storage,
    )

    stock_state = state["symbols"].setdefault(
        symbol,
        DEFAULT_SYMBOL_STATE.copy(),
    )

    stock_state["recent_complete"] = True

    _save_state(
        state_path,
        state,
        storage=storage,
    )


def mark_backfill_complete(
    state_path: str | Path,
    symbol: str,
    storage: StorageBackend | None = None,
) -> None:
    state = _load_state(
        state_path,
        storage=storage,
    )

    stock_state = state["symbols"].setdefault(
        symbol,
        DEFAULT_SYMBOL_STATE.copy(),
    )

    stock_state["backfill_complete"] = True

    _save_state(
        state_path,
        state,
        storage=storage,
    )
