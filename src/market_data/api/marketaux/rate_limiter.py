
import json
import math
import os
import tempfile
import threading
import time

from pathlib import Path


class MarketauxRateLimiter:
    def __init__(
        self,
        state_path: Path,
        minimum_interval: float = 3.0,
        clock=None,
        sleeper=None,
    ):
        if (
            not isinstance(minimum_interval, (int, float))
            or not math.isfinite(minimum_interval)
            or minimum_interval < 0
        ):
            raise ValueError(
                "minimum_interval must be a finite, non-negative number."
            )

        self.state_path = Path(state_path)
        self.minimum_interval = float(minimum_interval)

        self.clock = clock or time.time
        self.sleeper = sleeper or time.sleep

        self._lock = threading.Lock()

        self.state_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _load_state(self) -> dict:
        try:
            with self.state_path.open(
                "r",
                encoding="utf-8",
            ) as file:
                state = json.load(file)

        except FileNotFoundError:
            return {}

        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Marketaux rate-limit state contains invalid JSON: "
                f"{self.state_path}. Restore a valid state file before "
                "continuing."
            ) from exc

        if not isinstance(state, dict):
            raise ValueError(
                "Marketaux rate-limit state must be a JSON object."
            )

        for key in ("last_request", "cooldown_until"):
            value = state.get(key)

            if value is None:
                continue

            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(
                    f"Marketaux rate-limit state has an invalid {key}."
                )

        return state

    def _save_state(self, state: dict) -> None:
        """Atomically replace the state file."""

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
                state = self._load_state()
                now = self.clock()

                last_request = state.get("last_request")
                cooldown_until = state.get("cooldown_until")

                wait_time = 0.0

                if last_request is not None:
                    interval_wait = (
                        self.minimum_interval
                        - (now - last_request)
                    )

                    wait_time = max(
                        wait_time,
                        interval_wait,
                    )

                if cooldown_until is not None:
                    cooldown_wait = cooldown_until - now

                    wait_time = max(
                        wait_time,
                        cooldown_wait,
                    )

                if wait_time <= 1e-9:
                    break

                self.sleeper(wait_time)

            state["last_request"] = self.clock()

            self._save_state(state)

    def defer_for(self, seconds: float) -> None:
        if (
            isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or seconds < 0
        ):
            raise ValueError(
                "Cooldown duration must be a finite, non-negative number."
            )

        with self._lock:
            state = self._load_state()

            cooldown_until = self.clock() + seconds

            existing_cooldown = state.get("cooldown_until")

            if existing_cooldown is not None:
                cooldown_until = max(
                    cooldown_until,
                    existing_cooldown,
                )

            state["cooldown_until"] = cooldown_until

            self._save_state(state)
