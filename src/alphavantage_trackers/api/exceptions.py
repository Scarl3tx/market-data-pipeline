class AlphaVantageRateLimitError(Exception):
    """Raised when Alpha Vantage returns a rate-limit response."""
    pass

class AlphaVantageQuotaExceededError(Exception):
    """Raised when the rolling 24-hour API request quota is exhausted."""

    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__(
            f"Alpha Vantage daily quota exhausted. "
            f"Try again in {retry_after:.0f} seconds."
        )