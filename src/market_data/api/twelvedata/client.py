
import math
import os
from datetime import date, datetime, time
from market_data.storage.base import StorageBackend
import polars as pl
import requests
from dotenv import load_dotenv
import logging

from market_data.paths import (
    TWELVE_DATA_ENV_PATH,
    TWELVEDATA_RATE_LIMIT_STATE,
)
from market_data.api.twelvedata.exceptions import (
    TwelveDataAPIError,
)
from market_data.api.twelvedata.rate_limiter import (
    TwelveDataRateLimiter,
)


class TwelveDataClient:

    BASE_URL = "https://api.twelvedata.com/time_series"
    OUTPUT_SIZE = 5000
    REQUEST_TIMEOUT = 30

    REQUIRED_COLUMNS = (
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )

    def __init__(
    self,
    api_key: str | None = None,
    rate_limiter=None,
    storage: StorageBackend | None = None,
    ):
        load_dotenv(TWELVE_DATA_ENV_PATH)

        self.api_key = api_key or os.getenv("TWELVE_DATA_API_KEY")

        if not self.api_key:
            raise ValueError(
                "TWELVE_DATA_API_KEY not found in environment variables."
            )


        self.rate_limiter = rate_limiter or TwelveDataRateLimiter(
            state_path=TWELVEDATA_RATE_LIMIT_STATE,
            storage=storage,
        )

    @staticmethod
    def empty_stock_dataframe() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "asset_name": pl.String,
                "date": pl.Datetime("ns"),
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
            }
        )

    @staticmethod
    def _validate_request_dates(
        start_date: str | None,
        end_date: str,
    ) -> None:
        try:
            maximum_date = date.fromisoformat(end_date)

            if maximum_date.isoformat() != end_date:
                raise ValueError

            if start_date is not None:
                minimum_date = date.fromisoformat(start_date)

                if minimum_date.isoformat() != start_date:
                    raise ValueError

                if minimum_date > maximum_date:
                    raise ValueError

        except (TypeError, ValueError) as exc:
            raise ValueError(
                "start_date and end_date must be valid ISO dates, "
                "and start_date cannot be later than end_date."
            ) from exc

    @staticmethod
    def _parse_date(value, symbol: str) -> datetime:
        if not isinstance(value, str):
            raise TwelveDataAPIError(
                f"Invalid date value returned for {symbol}."
            )

        try:
            parsed_date = date.fromisoformat(value)

            if parsed_date.isoformat() != value:
                raise ValueError

            return datetime.combine(parsed_date, time.min)

        except ValueError as exc:
            raise TwelveDataAPIError(
                f"Invalid date returned for {symbol}: {value}"
            ) from exc

    @staticmethod
    def _parse_float(value, column: str, symbol: str) -> float:
        if isinstance(value, bool) or value is None:
            raise TwelveDataAPIError(
                f"Invalid {column} value returned for {symbol}."
            )

        try:
            parsed_value = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise TwelveDataAPIError(
                f"Invalid {column} value returned for {symbol}."
            ) from exc

        if not math.isfinite(parsed_value):
            raise TwelveDataAPIError(
                f"Non-finite {column} value returned for {symbol}."
            )

        return parsed_value

    @staticmethod
    def _parse_volume(value, symbol: str) -> int:
        if isinstance(value, bool) or value is None:
            raise TwelveDataAPIError(
                f"Invalid volume value returned for {symbol}."
            )

        try:
            numeric_value = float(value)

            if (
                not math.isfinite(numeric_value)
                or not numeric_value.is_integer()
                or numeric_value < 0
            ):
                raise ValueError

            return int(numeric_value)

        except (TypeError, ValueError, OverflowError) as exc:
            raise TwelveDataAPIError(
                f"Invalid volume value returned for {symbol}."
            ) from exc

    @classmethod
    def _parse_values(
        cls,
        values: list,
        symbol: str,
    ) -> pl.DataFrame:
        if not isinstance(values, list):
            raise TwelveDataAPIError(
                f"Invalid values format returned for {symbol}: "
                "expected a list."
            )

        if not values:
            return cls.empty_stock_dataframe()

        parsed_rows = []

        for index, row in enumerate(values):
            if not isinstance(row, dict):
                raise TwelveDataAPIError(
                    f"Invalid row {index} returned for {symbol}: "
                    "expected an object."
                )

            missing_columns = [
                column
                for column in cls.REQUIRED_COLUMNS
                if column not in row
            ]

            if missing_columns:
                raise TwelveDataAPIError(
                    f"Missing columns returned for {symbol}: "
                    f"{', '.join(missing_columns)}."
                )

            parsed_rows.append({
                "asset_name": symbol,
                "date": cls._parse_date(row["datetime"], symbol),
                "open": cls._parse_float(row["open"], "open", symbol),
                "high": cls._parse_float(row["high"], "high", symbol),
                "low": cls._parse_float(row["low"], "low", symbol),
                "close": cls._parse_float(row["close"], "close", symbol),
                "volume": cls._parse_volume(row["volume"], symbol),
            })

        try:
            return pl.DataFrame(
                parsed_rows,
                schema={
                    "asset_name": pl.String,
                    "date": pl.Datetime("ns"),
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64,
                },
            ).sort("date")

        except Exception as exc:
            raise TwelveDataAPIError(
                f"Could not construct stock data for {symbol}."
            ) from exc

    def get_stock_history(
        self,
        symbol: str,
        start_date: str | None,
        end_date: str,
    ) -> pl.DataFrame:

        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a non-empty string.")

        symbol = symbol.strip().upper()

        self._validate_request_dates(
            start_date,
            end_date,
        )

        params = {
            "symbol": symbol,
            "interval": "1day",
            "end_date": end_date,
            "adjust": "none",
            "order": "desc",
            "apikey": self.api_key,
            "format": "JSON",
        }

        if start_date is not None:
            params["start_date"] = start_date
        else:
            params["outputsize"] = self.OUTPUT_SIZE

        self.rate_limiter.wait_for_slot()

        try:
            response = requests.get(
                self.BASE_URL,
                params=params,
                timeout=self.REQUEST_TIMEOUT,
            )

        except requests.exceptions.RequestException:
            raise TwelveDataAPIError(
                f"Request failed for {symbol}. "
                "Check network connectivity and API availability."
            ) from None

        try:
            payload = response.json()

        except ValueError as exc:
            raise TwelveDataAPIError(
                f"Twelve Data returned invalid JSON for {symbol}."
            ) from exc

        if response.status_code == 400:
            message = (
                payload.get("message", "")
                if isinstance(payload, dict)
                else ""
            )

            if isinstance(message, str) and "no data" in message.lower():
                return self.empty_stock_dataframe()

        try:
            response.raise_for_status()

        except requests.exceptions.HTTPError:
            if isinstance(payload, dict):
                detail = (
                    payload.get("message")
                    or payload.get("error")
                    or payload.get("description")
                )
                api_code = payload.get("code")
            else:
                detail = None
                api_code = None

            if not detail:
                detail = response.text[:500] or "No response details provided."

            raise TwelveDataAPIError(
                f"Twelve Data HTTP error for {symbol}: "
                f"HTTP {response.status_code}, "
                f"API code={api_code}, "
                f"message={detail}"
            ) from None

        if not isinstance(payload, dict):
            raise TwelveDataAPIError(
                f"Invalid response format returned for {symbol}."
            )

        if payload.get("status") == "error":
            raise TwelveDataAPIError(
                f"Twelve Data API error for {symbol}: "
                f"{payload.get('message', 'Unknown API error')}"
            )

        if "values" not in payload:
            raise TwelveDataAPIError(
                f"Twelve Data response for {symbol} "
                "is missing the 'values' field."
            )

        df = self._parse_values(
            payload["values"],
            symbol,
        )

        if start_date is not None and not df.is_empty():
            minimum_date = date.fromisoformat(start_date)

            df = df.filter(
                pl.col("date").dt.date() >= minimum_date
            )

        return df.select([
            "asset_name",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ])
