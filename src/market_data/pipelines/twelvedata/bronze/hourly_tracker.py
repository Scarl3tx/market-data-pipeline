
import json
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger("twelvedata.bronze")


def _load_state(
    state_path: str | Path,
    storage: StorageBackend | None = None,
) -> dict:
    if storage is not None:
        if not storage.exists(str(state_path)):
            return {}

        try:
            state = storage.read_json(str(state_path))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(
                f"Invalid TwelveData hourly state JSON: {state_path}"
            ) from exc

    else:
        state_path = Path(state_path)

        if not state_path.exists():
            return {}

        try:
            with state_path.open(
                "r",
                encoding="utf-8-sig",
            ) as file:
                state = json.load(file)

        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(
                f"Invalid TwelveData hourly state JSON: {state_path}"
            ) from exc

    if not isinstance(state, dict):
        raise ValueError(
            "Invalid TwelveData hourly state: "
            "root must be an object."
        )

    for symbol, timestamp in state.items():
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(
                "Invalid TwelveData hourly state: "
                "symbol keys must be non-empty strings."
            )

        if not isinstance(timestamp, str):
            raise ValueError(
                f"Invalid hourly timestamp for {symbol}."
            )

        try:
            datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise ValueError(
                f"Invalid hourly timestamp for {symbol}: {timestamp}"
            ) from exc

    return state


def _save_state(
    state_path: str | Path,
    state: dict,
    storage: StorageBackend | None = None,
) -> None:
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


def has_pulled_this_hour(
    state_path: str | Path,
    symbol: str,
    now: datetime,
    storage: StorageBackend | None = None,
) -> bool:
    """Check whether a symbol has already been pulled during this hour."""

    state = _load_state(
        state_path,
        storage=storage,
    )

    last_pull = state.get(symbol)

    if last_pull is None:
        return False

    last_pull_time = datetime.fromisoformat(last_pull)

    already_pulled = (
        last_pull_time.year == now.year
        and last_pull_time.month == now.month
        and last_pull_time.day == now.day
        and last_pull_time.hour == now.hour
    )

    logger.debug(
        "%s hourly pull check: %s.",
        symbol,
        "already pulled" if already_pulled else "not yet pulled",
    )

    return already_pulled


def record_hourly_pull(
    state_path: str | Path,
    symbol: str,
    now: datetime,
    storage: StorageBackend | None = None,
) -> None:
    """Record a successful pull for a symbol."""

    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError(
            "symbol must be a non-empty string."
        )

    if not isinstance(now, datetime):
        raise ValueError(
            "now must be a datetime."
        )

    state = _load_state(
        state_path,
        storage=storage,
    )

    state[symbol] = now.isoformat()

    try:
        _save_state(
            state_path,
            state,
            storage=storage,
        )

        logger.info(
            "Recorded hourly pull for %s at %s.",
            symbol,
            now.isoformat(),
        )

    except Exception:
        logger.exception(
            "Failed to record hourly pull for %s.",
            symbol,
        )
        raise
