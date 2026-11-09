class AlphaVantageAPIError(Exception):
    """Raised when Alpha Vantage returns an API or HTTP error."""
    pass


class AlphaVantageRateLimitError(Exception):
    """Raised when Alpha Vantage's request rate limit is reached."""
    pass


class AlphaVantageQuotaExceededError(Exception):
    """Raised when Alpha Vantage's daily request quota is exhausted."""

    def __init__(
        self,
        retry_after: float | str | None = None,
        message: str | None = None,
    ):
        self.retry_after = None

        # Allow a text message to be supplied positionally.
        if isinstance(retry_after, str):
            message = retry_after
            retry_after = None

        if retry_after is not None:
            self.retry_after = float(retry_after)

        if message is None:
            if self.retry_after is not None:
                message = (
                    "Alpha Vantage daily quota exceeded. "
                    f"Retry after {self.retry_after:.2f} seconds."
                )
            else:
                message = "Alpha Vantage daily quota exceeded."

        super().__init__(message)