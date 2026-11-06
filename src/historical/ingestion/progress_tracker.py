
import json
from pathlib import Path
from src.historical.paths import HISTORICAL_PROGRESS_PATH

class HistoricalProgressTracker:

    def __init__(
        self,
        start_date: str,
        end_date: str,
        progress_path: Path | None = None
    ):
        self.progress_path = progress_path or HISTORICAL_PROGRESS_PATH
        self.progress_path.parent.mkdir(parents=True, exist_ok=True)

        self.start_date = start_date
        self.end_date = end_date

        self.progress = self._load_progress()

    def _load_progress(self) -> dict:
        if not self.progress_path.exists():
            return {}

        with open(self.progress_path, "r") as file:
            return json.load(file)

    def _save_progress(self) -> None:
        with open(self.progress_path, "w") as file:
            json.dump(self.progress, file, indent=4)

    def initialise_stock(self, symbol: str) -> None:
        existing = self.progress.get(symbol)

        # Reset progress if the requested date range has changed
        if (
            existing is None
            or existing.get("start_date") != self.start_date
            or existing.get("end_date") != self.end_date
        ):
            self.progress[symbol] = {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "next_end_date": self.end_date,
                "completed": False
            }

            self._save_progress()

    def update_stock(self, symbol: str, next_end_date: str) -> None:
        self.progress[symbol]["next_end_date"] = next_end_date
        self._save_progress()

    def complete_stock(self, symbol: str) -> None:
        self.progress[symbol]["completed"] = True
        self._save_progress()

    def get_stock_progress(self, symbol: str) -> dict:
        return self.progress[symbol]
