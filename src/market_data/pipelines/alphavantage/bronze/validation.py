class AlphaVantageNewsValidationError(Exception):
    """Raised when Alpha Vantage news data is invalid."""


def validate_news_response(data: dict) -> None:
    if not isinstance(data, dict):
        raise AlphaVantageNewsValidationError(
            "News response must be a dictionary."
        )

    if "feed" not in data:
        raise AlphaVantageNewsValidationError(
            "News response is missing the feed field."
        )

    if not isinstance(data["feed"], list):
        raise AlphaVantageNewsValidationError(
            "News feed must be a list."
        )

    for index, article in enumerate(data["feed"]):
        if not isinstance(article, dict):
            raise AlphaVantageNewsValidationError(
                f"Article at index {index} must be a dictionary."
            )

        required_fields = ["title", "time_published", "ticker_sentiment"]

        missing_fields = [
            field for field in required_fields
            if field not in article
        ]

        if missing_fields:
            raise AlphaVantageNewsValidationError(
                f"Article at index {index} is missing fields: "
                f"{missing_fields}"
            )

        if not isinstance(article["ticker_sentiment"], list):
            raise AlphaVantageNewsValidationError(
                f"Article at index {index} has invalid ticker_sentiment."
            )