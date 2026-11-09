import os
import requests

from dotenv import load_dotenv

from market_data.api.alphavantage.exceptions import (
    AlphaVantageAPIError,
    AlphaVantageRateLimitError,
    AlphaVantageQuotaExceededError,
)
from market_data.paths import ALPHAVANTAGE_ENV_PATH
from market_data.paths import ALPHAVANTAGE_API_USAGE_PATH

from market_data.api.alphavantage.quota_storage import QuotaStorage
from market_data.api.alphavantage.rate_limiter import AlphaVantageRateLimiter

BASE_URL = "https://www.alphavantage.co/query"

class AlphaVantageClient:

    def __init__(self, api_key=None, timeout=30, rate_limiter=None):
        load_dotenv(ALPHAVANTAGE_ENV_PATH)

        self.api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY")
        self.timeout = timeout

        if not self.api_key:
            raise ValueError("Alpha Vantage API key not found.")

        if rate_limiter is None:
            storage = QuotaStorage(ALPHAVANTAGE_API_USAGE_PATH)
            rate_limiter = AlphaVantageRateLimiter(storage)

        self.rate_limiter = rate_limiter

    def get_news_sentiment(
    self,
    tickers: list[str] | None = None,
    time_from: str | None = None,
    time_to: str | None = None,
    limit: int = 1000,
    sort: str = "LATEST",
) -> dict:

        params = {
            "function": "NEWS_SENTIMENT",
            "apikey": self.api_key,
            "limit": limit,
            "sort": sort,
        }

        if tickers:
            params["tickers"] = ",".join(tickers)

        if time_from:
            params["time_from"] = time_from

        if time_to:
            params["time_to"] = time_to

        self.rate_limiter.acquire()

        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000.")

        if sort not in {"LATEST", "EARLIEST", "RELEVANCE"}:
            raise ValueError(
                "sort must be LATEST, EARLIEST, or RELEVANCE."
            )

        try:
            response = requests.get(
                BASE_URL,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()

        except requests.exceptions.RequestException as error:
            raise AlphaVantageAPIError(
                f"Alpha Vantage request failed: {error}"
            ) from error

        except ValueError as error:
            raise AlphaVantageAPIError(
                "Alpha Vantage returned invalid JSON."
            ) from error

        if "Error Message" in data:
            raise AlphaVantageAPIError(data["Error Message"])

        if "Note" in data:
            raise AlphaVantageRateLimitError(data["Note"])

        if "Information" in data:
            raise AlphaVantageQuotaExceededError(data["Information"])

        if "feed" not in data:
            raise AlphaVantageAPIError(
                "Unexpected Alpha Vantage response format."
            )

        return data