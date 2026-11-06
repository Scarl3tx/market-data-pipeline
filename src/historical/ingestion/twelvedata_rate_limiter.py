
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from src.historical.ingestion.exceptions import TwelveDataDailyLimitError

class TwelveDataRateLimiter:
    def __init__(
        self,
        state_path: Path,
        daily_limit: int = 790,
        minute_limit: int = 8
    ):
        self.state_path = state_path
        self.daily_limit = daily_limit
        self.minute_limit = minute_limit

        self.state_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_daily_usage(self):
        today = datetime.now(timezone.utc).date().isoformat()

        if not self.state_path.exists():
            return {
                "date": today,
                "credits_used": 0,
                "request_timestamps": []
            }

        with open(self.state_path, "r") as file:
            state = json.load(file)

        if state["date"] != today:
            return {
                "date": today,
                "credits_used": 0,
                "request_timestamps": []
            }

        # Support existing state files created by the previous version
        state.setdefault("request_timestamps", [])

        return state

    def _save_daily_usage(self, state):
        with open(self.state_path, "w") as file:
            json.dump(state, file, indent=4)

    def wait_for_slot(self):
        while True:
            state = self._load_daily_usage()
            now = time.time()

            # Remove requests outside the rolling 60-second window
            state["request_timestamps"] = [
                timestamp
                for timestamp in state["request_timestamps"]
                if now - timestamp < 60
            ]

            # Enforce the minute limit
            if len(state["request_timestamps"]) >= self.minute_limit:
                wait_time = 60 - (now - state["request_timestamps"][0])

                print(
                    f"[RATE LIMITER] Minute limit reached. "
                    f"Waiting {wait_time:.1f}s..."
                )

                time.sleep(wait_time)
                continue

            # Enforce the daily limit
            if state["credits_used"] >= self.daily_limit:
                raise TwelveDataDailyLimitError(
                    "Twelve Data daily credit limit reached. "
                    "Resume after the next UTC reset."
                )

            # Reserve one credit before making the request
            state["credits_used"] += 1
            state["request_timestamps"].append(now)

            self._save_daily_usage(state)

            print(
                f"[RATE LIMITER] "
                f"Daily usage: {state['credits_used']}/{self.daily_limit} | "
                f"Minute usage: {len(state['request_timestamps'])}/{self.minute_limit}"
            )

            return
