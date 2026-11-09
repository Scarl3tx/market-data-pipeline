import json
import math
import os
import tempfile
import threading
import time

from datetime import datetime, timezone
from pathlib import Path

from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)
from market_data.storage.base import StorageBackend


class TwelveDataRateLimiter:
    def __init__(
        self,
        state_path: Path,
        daily_limit: int = 790,
        minute_limit: int = 8,
        minimum_interval: float = 8.1,
        clock=None,
        sleeper=None,
        storage: StorageBackend | None = None,
        storage_key: str = "state/twelvedata/rate_limit.json",
    ):
        if (
            isinstance(daily_limit, bool)
            or not isinstance(daily_limit, int)
            or daily_limit < 1
        ):
            raise ValueError("daily_limit must be at least 1.")

        if (
            isinstance(minute_limit, bool)
            or not isinstance(minute_limit, int)
            or minute_limit < 1
        ):
            raise ValueError("minute_limit must be at least 1.")

        if (
            isinstance(minimum_interval, bool)
            or not isinstance(minimum_interval, (int, float))
            or not math.isfinite(minimum_interval)
            or minimum_interval < 0
        ):
            raise ValueError(
                "minimum_interval must be a finite, non-negative number."
            )

        self.state_path = Path(state_path)
        self.daily_limit = daily_limit
        self.minute_limit = minute_limit
        self.minimum_interval = float(minimum_interval)

        self.clock = clock or time.time
        self.sleeper = sleeper or time.sleep

        self.storage = storage
        self.storage_key: str = str(storage_key)

        self._lock = threading.Lock()

        if self.storage is None:
            self.state_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

    def _today(self) -> str:
        return datetime.fromtimestamp(
            self.clock(),
            tz=timezone.utc,
        ).date().isoformat()

    @staticmethod
    def _empty_state(today: str) -> dict:
        return {
            "date": today,
            "credits_used": 0,
            "request_timestamps": [],
        }

    def _read_state(self) -> dict:
        if self.storage is not None:
            if not self.storage.exists(self.storage_key):
                return self._empty_state(self._today())

            try:
                return self.storage.read_json(self.storage_key)
            except (ValueError, json.JSONDecodeError) as exc:
                raise ValueError(
                    "TwelveData rate-limit state contains invalid JSON: "
                    f"{self.storage_key}. Restore a valid state file before "
                    "continuing."
                ) from exc

        try:
            with self.state_path.open(
                "r",
                encoding="utf-8",
            ) as file:
                return json.load(file)

        except FileNotFoundError:
            return self._empty_state(self._today())

        except json.JSONDecodeError as exc:
            raise ValueError(
                f"TwelveData rate-limit state contains invalid JSON: "
                f"{self.state_path}. Restore a valid state file before "
                "continuing."
            ) from exc

    def _load_daily_usage(self) -> dict:
        today = self._today()
        state = self._read_state()

        if not isinstance(state, dict):
            raise ValueError(
                "TwelveData rate-limit state must be a JSON object."
            )

        stored_date = state.get("date")

        if not isinstance(stored_date, str):
            raise ValueError(
                "TwelveData rate-limit state is missing a valid date."
            )

        try:
            parsed_date = datetime.strptime(
                stored_date,
                "%Y-%m-%d",
            ).date()
        except ValueError as exc:
            raise ValueError(
                "TwelveData rate-limit state contains an invalid date."
            ) from exc

        if parsed_date.isoformat() != stored_date:
            raise ValueError(
                "TwelveData rate-limit state date must use YYYY-MM-DD."
            )

        credits_used = state.get("credits_used")
        timestamps = state.get("request_timestamps")

        if (
            isinstance(credits_used, bool)
            or not isinstance(credits_used, int)
            or credits_used < 0
        ):
            raise ValueError(
                "TwelveData rate-limit state has invalid credits_used."
            )

        if not isinstance(timestamps, list):
            raise ValueError(
                "TwelveData rate-limit state has invalid request_timestamps."
            )

        for timestamp in timestamps:
            if (
                isinstance(timestamp, bool)
                or not isinstance(timestamp, (int, float))
                or not math.isfinite(timestamp)
            ):
                raise ValueError(
                    "TwelveData rate-limit state contains an invalid "
                    "request timestamp."
                )

        if stored_date != today:
            return self._empty_state(today)

        return {
            "date": stored_date,
            "credits_used": credits_used,
            "request_timestamps": timestamps,
        }

    def _save_daily_usage(self, state: dict) -> None:
        if self.storage is not None:
            self.storage.write_json(
                self.storage_key,
                state,
                overwrite=True,
            )
            return

        temporary_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.state_path.parent,
                prefix=f".{self.state_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as file:
                temporary_path = Path(file.name)

                json.dump(
                    state,
                    file,
                    indent=4,
                )

                file.flush()
                os.fsync(file.fileno())

            os.replace(
                temporary_path,
                self.state_path,
            )

        finally:
            if (
                temporary_path is not None
                and temporary_path.exists()
            ):
                temporary_path.unlink()

    def wait_for_slot(self) -> None:
        with self._lock:
            while True:
                state = self._load_daily_usage()
                now = self.clock()

                state["request_timestamps"] = [
                    timestamp
                    for timestamp in state["request_timestamps"]
                    if 0 <= now - timestamp < 60
                ]

                if state["credits_used"] >= self.daily_limit:
                    raise TwelveDataDailyLimitError(
                        "Twelve Data daily credit limit reached. "
                        "Resume after the next UTC reset."
                    )

                wait_times = []

                if (
                    len(state["request_timestamps"])
                    >= self.minute_limit
                ):
                    oldest = state["request_timestamps"][0]
                    wait_times.append(60 - (now - oldest))

                if state["request_timestamps"]:
                    latest = state["request_timestamps"][-1]
                    elapsed = now - latest

                    if elapsed < self.minimum_interval:
                        wait_times.append(
                            self.minimum_interval - elapsed
                        )

                wait_time = max(wait_times, default=0)

                if wait_time > 1e-9:
                    self.sleeper(wait_time)
                    continue

                state["credits_used"] += 1
                state["request_timestamps"].append(now)

                self._save_daily_usage(state)
                return