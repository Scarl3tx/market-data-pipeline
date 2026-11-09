
import math
import os
import requests
from pathlib import Path
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from dotenv import load_dotenv

from market_data.api.marketaux.exceptions import (
    MarketauxAPIError,
    MarketauxAuthenticationError,
    MarketauxEndpointRestrictedError,
    MarketauxQuotaExceededError,
    MarketauxRateLimitError,
    MarketauxResponseError,
)
from market_data.api.marketaux.quota_tracker import (
    MarketauxQuotaTracker,
    MarketauxDynamoDBQuotaTracker,
)
from market_data.api.marketaux.rate_limiter import (
    MarketauxRateLimiter,
)
from market_data.paths import (
    MARKETAUX_API_USAGE_PATH,
    MARKETAUX_ENV_PATH,
    MARKETAUX_RATE_LIMIT_STATE_PATH,
)

BASE_URL = "https://api.marketaux.com/v1/news/all"


class MarketauxClient:
    def __init__(
        self,
        api_token=None,
        timeout=30,
        quota_tracker=None,
        daily_limit=None,
        rate_limiter=None,
    ):
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError(
                "timeout must be a finite, positive number."
            )

        load_dotenv(MARKETAUX_ENV_PATH)

        self.api_token = (
            api_token or os.getenv("MARKETAUX_API_TOKEN")
        )

        if not self.api_token:
            raise ValueError(
                "Marketaux API token not found."
            )

        self.timeout = float(timeout)

        if daily_limit is None:
            daily_limit = int(
                os.getenv(
                    "MARKETAUX_DAILY_REQUEST_LIMIT",
                    "100",
                )
            )

        self.articles_per_request = int(
            os.getenv(
                "MARKETAUX_ARTICLES_PER_REQUEST",
                "3",
            )
        )

        if self.articles_per_request < 1:
            raise ValueError(
                "articles_per_request must be at least 1."
            )

        if quota_tracker is None:
            quota_backend = os.getenv(
                "MARKETAUX_QUOTA_BACKEND",
                "local",
            ).strip().lower()

            if quota_backend == "dynamodb":
                quota_tracker = MarketauxDynamoDBQuotaTracker(
                    table_name=os.getenv(
                        "MARKETAUX_QUOTA_TABLE",
                        "market-data-api-quotas",
                    ),
                    daily_limit=daily_limit,
                )

            elif quota_backend == "local":
                quota_tracker = MarketauxQuotaTracker(
                    db_path=Path(MARKETAUX_API_USAGE_PATH),
                    daily_limit=daily_limit,
                )

            else:
                raise ValueError(
                    f"Unsupported Marketaux quota backend: {quota_backend}"
                )

        self.quota_tracker = quota_tracker

        if rate_limiter is None:
            rate_limiter = MarketauxRateLimiter(
                state_path=MARKETAUX_RATE_LIMIT_STATE_PATH,
                minimum_interval=float(
                    os.getenv(
                        "MARKETAUX_MINIMUM_INTERVAL",
                        "3.0",
                    )
                ),
            )

        self.rate_limiter = rate_limiter

        self.rate_limit_fallback_seconds = float(
            os.getenv(
                "MARKETAUX_RATE_LIMIT_COOLDOWN_SECONDS",
                "300",
            )
        )

        if (
            not math.isfinite(self.rate_limit_fallback_seconds)
            or self.rate_limit_fallback_seconds < 0
        ):
            raise ValueError(
                "MARKETAUX_RATE_LIMIT_COOLDOWN_SECONDS "
                "must be finite and non-negative."
            )

    @staticmethod
    def _get_retry_after_seconds(value, fallback):
        if value is None:
            return fallback

        try:
            seconds = float(value)

            if math.isfinite(seconds):
                return max(0.0, seconds)

        except (TypeError, ValueError, OverflowError):
            pass

        try:
            retry_at = parsedate_to_datetime(value)

            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(
                    tzinfo=timezone.utc
                )

            now = datetime.now(timezone.utc)

            seconds = (retry_at - now).total_seconds()

            if math.isfinite(seconds):
                return max(0.0, seconds)

        except (
            TypeError,
            ValueError,
            OverflowError,
            IndexError,
        ):
            pass

        return fallback

    def get_news(
        self,
        symbols,
        page=1,
        limit=None,
        published_after=None,
        published_before=None,
        published_on=None,
        language="en",
    ):
        if isinstance(symbols, str):
            symbols = [symbols]

        if not isinstance(symbols, (list, tuple)):
            raise ValueError(
                "symbols must be a string or a list of strings."
            )

        if not all(
            isinstance(symbol, str)
            for symbol in symbols
        ):
            raise ValueError(
                "Every ticker symbol must be a string."
            )

        symbols = [
            symbol.strip().upper()
            for symbol in symbols
            if symbol.strip()
        ]

        if not symbols:
            raise ValueError(
                "At least one ticker symbol is required."
            )

        if (
            isinstance(page, bool)
            or not isinstance(page, int)
            or page < 1
        ):
            raise ValueError(
                "page must be a positive integer."
            )

        if limit is None:
            limit = self.articles_per_request

        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
        ):
            raise ValueError(
                "limit must be a positive integer."
            )

        if limit > self.articles_per_request:
            raise ValueError(
                "Requested limit exceeds configured "
                "articles-per-request allowance."
            )

        params = {
            "api_token": self.api_token,
            "symbols": ",".join(symbols),
            "filter_entities": "true",
            "must_have_entities": "true",
            "group_similar": "true",
            "language": language,
            "limit": limit,
            "page": page,
        }

        if published_after:
            params["published_after"] = published_after

        if published_before:
            params["published_before"] = published_before

        if published_on:
            params["published_on"] = published_on

        self.rate_limiter.wait_for_slot()

        try:
            self.quota_tracker.reserve_request()
        except RuntimeError as exc:
            raise MarketauxQuotaExceededError(
                str(exc)
            ) from exc

        try:
            response = requests.get(
                BASE_URL,
                params=params,
                timeout=self.timeout,
            )

        except requests.Timeout as exc:
            raise MarketauxAPIError(
                f"Marketaux request timed out: {exc}"
            ) from exc

        except requests.ConnectionError as exc:
            raise MarketauxAPIError(
                f"Marketaux connection failed: {exc}"
            ) from exc

        except requests.RequestException as exc:
            raise MarketauxAPIError(
                f"Marketaux request failed: {exc}"
            ) from exc

        if response.status_code == 401:
            raise MarketauxAuthenticationError(
                "Marketaux rejected the API token."
            )

        if response.status_code == 402:
            raise MarketauxQuotaExceededError(
                "Marketaux subscription usage limit reached."
            )

        if response.status_code == 403:
            raise MarketauxEndpointRestrictedError(
                "Marketaux endpoint is restricted by the plan."
            )

        if response.status_code == 429:
            retry_after = self._get_retry_after_seconds(
                response.headers.get("Retry-After"),
                self.rate_limit_fallback_seconds,
            )

            self.rate_limiter.defer_for(retry_after)

            raise MarketauxRateLimitError(
                "Marketaux short-term rate limit reached. "
                f"Cooldown recorded for {retry_after:.0f} seconds."
            )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise MarketauxAPIError(
                f"Marketaux HTTP error "
                f"{response.status_code}: {exc}"
            ) from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise MarketauxResponseError(
                "Marketaux returned invalid JSON."
            ) from exc

        if not isinstance(data, dict):
            raise MarketauxResponseError(
                "Marketaux response must be a JSON object."
            )

        if "error" in data:
            error = data["error"]

            message = (
                error.get("message", "Unknown API error.")
                if isinstance(error, dict)
                else str(error)
            )

            raise MarketauxResponseError(message)

        if not isinstance(data.get("meta"), dict):
            raise MarketauxResponseError(
                "Marketaux response is missing metadata."
            )

        if not isinstance(data.get("data"), list):
            raise MarketauxResponseError(
                "Marketaux response is missing article data."
            )

        return data
