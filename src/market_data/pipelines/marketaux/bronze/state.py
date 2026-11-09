
import json

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from market_data.paths import MARKETAUX_NEWS_STATE_PATH
from market_data.storage.base import StorageBackend


S3_STATE_KEY = "state/marketaux/news_state.json"


class MarketauxStateError(Exception):
    """Invalid or unreadable Marketaux ingestion state."""


class MarketauxNewsState:
    def __init__(
        self,
        state_path: str | Path | None = None,
        storage: StorageBackend | None = None,
    ):
        self.storage = storage

        if storage is not None:
            self.state_path: str | Path = (
                str(state_path)
                if state_path is not None
                else S3_STATE_KEY
            )
        else:
            self.state_path = Path(
                state_path
                if state_path is not None
                else MARKETAUX_NEWS_STATE_PATH
            )
            self.state_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

        self.state: dict[str, Any] = self._load_state()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _format_datetime(value: datetime) -> str:
        return value.isoformat()

    @staticmethod
    def _parse_datetime(
        value: str | None,
        field_name: str,
        symbol: str,
    ) -> datetime | None:
        if value is None:
            return None

        if not isinstance(value, str) or not value.strip():
            raise MarketauxStateError(
                f"Invalid {field_name} for {symbol}."
            )

        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise MarketauxStateError(
                f"Invalid {field_name} for {symbol}."
            ) from None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _default_stock(name: str) -> dict[str, Any]:
        return {
            "name": name,
            "last_attempt_at": None,
            "last_successful_check": None,
            "oldest_filled_at": None,
            "pending_since": None,
            "pending_until": None,
            "next_page": None,
            "ingestion_in_progress": False,
            "ingestion_type": None,
        }

    def _load_state(self) -> dict[str, Any]:
        if self.storage is not None:
            if not self.storage.exists(str(self.state_path)):
                return {"stocks": {}}

            try:
                state = self.storage.read_json(str(self.state_path))
            except (json.JSONDecodeError, UnicodeDecodeError):
                raise MarketauxStateError(
                    f"Invalid JSON in Marketaux state: {self.state_path}"
                ) from None
        else:
            local_path = Path(self.state_path)

            if not local_path.exists():
                return {"stocks": {}}

            try:
                with local_path.open(
                    "r",
                    encoding="utf-8-sig",
                ) as file:
                    state = json.load(file)

            except (json.JSONDecodeError, UnicodeDecodeError):
                raise MarketauxStateError(
                    f"Invalid JSON in Marketaux state: {local_path}"
                ) from None

        if (
            not isinstance(state, dict)
            or not isinstance(state.get("stocks"), dict)
        ):
            raise MarketauxStateError(
                "Marketaux state must contain a stocks object."
            )

        for symbol, stock in state["stocks"].items():
            if not isinstance(symbol, str) or not symbol.strip():
                raise MarketauxStateError(
                    "Invalid stock symbol in Marketaux state."
                )

            if not isinstance(stock, dict):
                raise MarketauxStateError(
                    f"Invalid state entry for {symbol}."
                )

            defaults = self._default_stock(symbol)

            for field, value in defaults.items():
                stock.setdefault(field, value)

            if not isinstance(stock["name"], str):
                raise MarketauxStateError(
                    f"Invalid stock name for {symbol}."
                )

            if not isinstance(stock["ingestion_in_progress"], bool):
                raise MarketauxStateError(
                    f"Invalid ingestion status for {symbol}."
                )

            if stock["ingestion_type"] not in (
                None,
                "recent",
                "backfill",
            ):
                raise MarketauxStateError(
                    f"Invalid ingestion type for {symbol}."
                )

            for field in (
                "last_attempt_at",
                "last_successful_check",
                "oldest_filled_at",
                "pending_since",
                "pending_until",
            ):
                self._parse_datetime(
                    stock[field],
                    field,
                    symbol,
                )

            next_page = stock["next_page"]

            if next_page is not None and (
                not isinstance(next_page, int)
                or isinstance(next_page, bool)
                or next_page < 1
            ):
                raise MarketauxStateError(
                    f"Invalid next_page for {symbol}."
                )

            if stock["ingestion_in_progress"]:
                if not self._has_valid_pending_checkpoint(stock):
                    raise MarketauxStateError(
                        f"Invalid pending ingestion checkpoint for {symbol}."
                    )

        return state

    def _save(self) -> None:
        try:
            if self.storage is not None:
                self.storage.write_json(
                    str(self.state_path),
                    self.state,
                    overwrite=True,
                )
                return

            local_path = Path(self.state_path)
            temporary_path = local_path.with_suffix(
                local_path.suffix + ".tmp"
            )

            try:
                with temporary_path.open(
                    "w",
                    encoding="utf-8",
                ) as file:
                    json.dump(self.state, file, indent=4)

                temporary_path.replace(local_path)

            finally:
                temporary_path.unlink(missing_ok=True)

        except OSError as error:
            raise MarketauxStateError(
                f"Failed to save Marketaux state: {self.state_path}"
            ) from error
        except Exception as error:
            if self.storage is not None:
                raise MarketauxStateError(
                    f"Failed to save Marketaux state: {self.state_path}"
                ) from error
            raise

    def initialize_stocks(self, stocks: list[dict[str, str]]) -> None:
        for stock in stocks:
            symbol = stock["symbol"].strip().upper()
            name = stock["name"].strip()

            if symbol not in self.state["stocks"]:
                self.state["stocks"][symbol] = self._default_stock(name)
            else:
                self.state["stocks"][symbol]["name"] = name

        self._save()

    def _get_stock(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.strip().upper()

        if symbol not in self.state["stocks"]:
            raise MarketauxStateError(
                f"Stock not initialized in Marketaux state: {symbol}"
            )

        return self.state["stocks"][symbol]

    def get_stock(self, symbol: str) -> dict[str, Any]:
        return deepcopy(self._get_stock(symbol))

    def record_attempt(self, symbol: str) -> None:
        stock = self._get_stock(symbol)
        stock["last_attempt_at"] = self._format_datetime(self._now())
        self._save()

    @staticmethod
    def _has_valid_pending_checkpoint(
        stock: dict[str, Any],
    ) -> bool:
        try:
            pending_since = datetime.fromisoformat(
                stock["pending_since"]
            )
            pending_until = datetime.fromisoformat(
                stock["pending_until"]
            )
        except (TypeError, ValueError):
            return False

        next_page = stock.get("next_page")

        if (
            not isinstance(next_page, int)
            or isinstance(next_page, bool)
            or next_page < 1
        ):
            return False

        if pending_since.tzinfo is None:
            pending_since = pending_since.replace(tzinfo=timezone.utc)

        if pending_until.tzinfo is None:
            pending_until = pending_until.replace(tzinfo=timezone.utc)

        return pending_since < pending_until

    def _start_window(
        self,
        symbol: str,
        pending_since: datetime,
        pending_until: datetime,
        ingestion_type: str,
    ) -> None:
        stock = self._get_stock(symbol)

        if pending_since >= pending_until:
            raise MarketauxStateError(
                f"Invalid ingestion window for {symbol}."
            )

        stock["pending_since"] = self._format_datetime(pending_since)
        stock["pending_until"] = self._format_datetime(pending_until)
        stock["next_page"] = 1
        stock["ingestion_in_progress"] = True
        stock["ingestion_type"] = ingestion_type

        self._save()

    def start_ingestion(
        self,
        symbol: str,
        initial_lookback_days: int = 30,
        overlap_hours: int = 24,
    ) -> None:
        """Start or resume a recent-news ingestion window."""

        stock = self._get_stock(symbol)

        if initial_lookback_days < 1:
            raise ValueError("initial_lookback_days must be at least 1.")

        if overlap_hours < 0:
            raise ValueError("overlap_hours cannot be negative.")

        if stock["ingestion_in_progress"]:
            if self._has_valid_pending_checkpoint(stock):
                return

            stock["ingestion_in_progress"] = False
            stock["pending_since"] = None
            stock["pending_until"] = None
            stock["next_page"] = None
            stock["ingestion_type"] = None

        now = self._now()
        last_successful_check = stock["last_successful_check"]

        if last_successful_check:
            checkpoint = self._parse_datetime(
                last_successful_check,
                "last_successful_check",
                symbol,
            )
            if checkpoint is None:
                raise MarketauxStateError(
                    f"Invalid last_successful_check for {symbol}."
                )

            pending_since = checkpoint - timedelta(hours=overlap_hours)
        else:
            pending_since = now - timedelta(days=initial_lookback_days)

        self._start_window(
            symbol,
            pending_since,
            now,
            "recent",
        )

    def start_backfill(
        self,
        symbol: str,
        window_days: int = 30,
    ) -> None:
        """Start or resume one historical backfill window."""

        stock = self._get_stock(symbol)

        if window_days < 1:
            raise ValueError("window_days must be at least 1.")

        if stock["ingestion_in_progress"]:
            if self._has_valid_pending_checkpoint(stock):
                return

            stock["ingestion_in_progress"] = False
            stock["pending_since"] = None
            stock["pending_until"] = None
            stock["next_page"] = None
            stock["ingestion_type"] = None

        oldest_filled_at = stock["oldest_filled_at"]

        if oldest_filled_at is None:
            raise MarketauxStateError(
                f"{symbol} has no completed recent window to backfill."
            )

        pending_until = self._parse_datetime(
            oldest_filled_at,
            "oldest_filled_at",
            symbol,
        )

        if pending_until is None:
            raise MarketauxStateError(
                f"Invalid oldest_filled_at for {symbol}."
            )

        pending_since = pending_until - timedelta(days=window_days)

        self._start_window(
            symbol,
            pending_since,
            pending_until,
            "backfill",
        )

    def record_page_saved(self, symbol: str, page: int) -> None:
        stock = self._get_stock(symbol)

        if not stock["ingestion_in_progress"]:
            raise MarketauxStateError(
                f"No ingestion is in progress for {symbol}."
            )

        if stock["next_page"] != page:
            raise MarketauxStateError(
                f"Unexpected page {page} for {symbol}; "
                f"expected {stock['next_page']}."
            )

        stock["next_page"] = page + 1
        self._save()

    def complete_ingestion(self, symbol: str) -> None:
        stock = self._get_stock(symbol)

        if not stock["ingestion_in_progress"]:
            raise MarketauxStateError(
                f"No ingestion is in progress for {symbol}."
            )

        ingestion_type = stock["ingestion_type"]
        pending_since = stock["pending_since"]
        pending_until = stock["pending_until"]

        if not isinstance(pending_since, str) or not isinstance(
            pending_until, str
        ):
            raise MarketauxStateError(
                f"Invalid ingestion window for {symbol}."
            )

        if ingestion_type == "recent":
            stock["last_successful_check"] = pending_until

        existing_oldest = stock["oldest_filled_at"]

        if existing_oldest is None:
            stock["oldest_filled_at"] = pending_since
        else:
            existing_date = self._parse_datetime(
                existing_oldest,
                "oldest_filled_at",
                symbol,
            )
            new_date = self._parse_datetime(
                pending_since,
                "pending_since",
                symbol,
            )

            if existing_date is None or new_date is None:
                raise MarketauxStateError(
                    f"Invalid historical coverage date for {symbol}."
                )

            if new_date < existing_date:
                stock["oldest_filled_at"] = pending_since

        stock["pending_since"] = None
        stock["pending_until"] = None
        stock["next_page"] = None
        stock["ingestion_in_progress"] = False
        stock["ingestion_type"] = None

        self._save()

    def get_stocks_by_priority(self) -> list[tuple[str, dict[str, Any]]]:
        """Prioritize pending ingestion and oldest historical coverage."""

        def sort_key(item: tuple[str, dict[str, Any]]) -> tuple:
            symbol, stock = item
            oldest = stock.get("oldest_filled_at")

            return (
                not stock.get("ingestion_in_progress", False),
                oldest is not None,
                oldest or "",
                stock.get("last_attempt_at") or "",
                symbol,
            )

        return sorted(
            self.state["stocks"].items(),
            key=sort_key,
        )

    def get_stocks_without_recent_coverage(self) -> list[str]:
        return [
            symbol
            for symbol, stock in self.get_stocks_by_priority()
            if stock.get("last_successful_check") is None
        ]

    def get_backfill_candidates(
        self,
    ) -> list[tuple[str, dict[str, Any]]]:
        return [
            (symbol, deepcopy(stock))
            for symbol, stock in self.get_stocks_by_priority()
            if (
                stock.get("oldest_filled_at") is not None
                or stock.get("ingestion_type") == "backfill"
            )
        ]
