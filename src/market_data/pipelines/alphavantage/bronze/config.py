import json
from datetime import date, datetime

from market_data.paths import STOCKS_CONFIG_PATH


def load_news_tickers() -> list[str]:
    with open(
        STOCKS_CONFIG_PATH,
        "r",
        encoding="utf-8",
    ) as file:
        config = json.load(file)

    stocks = config.get("stocks")

    if not isinstance(stocks, list):
        raise ValueError("Invalid stocks configuration.")

    tickers = []

    for stock in stocks:
        if not isinstance(stock, dict):
            raise ValueError(
                "Each stock configuration must be a dictionary."
            )

        if not isinstance(stock.get("enabled"), bool):
            raise ValueError(
                "Stock enabled field must be a boolean."
            )

        if not stock["enabled"]:
            continue

        symbol = stock.get("symbol")

        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(
                "Enabled stocks must have a valid symbol."
            )

        tickers.append(symbol.strip().upper())

    return tickers


def load_news_start_time() -> str | None:
    """
    Return the earliest enabled stock start_date as an
    Alpha Vantage time_from timestamp.

    Uses midnight (00:00) because the stock configuration
    stores dates rather than times.
    """

    with open(
        STOCKS_CONFIG_PATH,
        "r",
        encoding="utf-8",
    ) as file:
        config = json.load(file)

    stocks = config.get("stocks")

    if not isinstance(stocks, list):
        raise ValueError("Invalid stocks configuration.")

    start_dates = []

    for stock in stocks:
        if not isinstance(stock, dict):
            raise ValueError(
                "Each stock configuration must be a dictionary."
            )

        if not isinstance(stock.get("enabled"), bool):
            raise ValueError(
                "Stock enabled field must be a boolean."
            )

        if not stock["enabled"]:
            continue

        start_date = stock.get("start_date")

        if not isinstance(start_date, str):
            raise ValueError(
                "Enabled stocks must have a valid start_date."
            )

        try:
            parsed_date = date.fromisoformat(start_date)
        except ValueError as error:
            raise ValueError(
                f"Invalid stock start_date: {start_date}"
            ) from error

        start_dates.append(parsed_date)

    if not start_dates:
        return None

    earliest_date = min(start_dates)

    return datetime.combine(
        earliest_date,
        datetime.min.time(),
    ).strftime("%Y%m%dT%H%M")