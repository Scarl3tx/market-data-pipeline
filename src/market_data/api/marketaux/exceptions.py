
class MarketauxAPIError(Exception):
    """Base exception for Marketaux API errors."""


class MarketauxAuthenticationError(MarketauxAPIError):
    """Invalid or missing API token."""


class MarketauxQuotaExceededError(MarketauxAPIError):
    """Daily API request allowance exhausted."""


class MarketauxRateLimitError(MarketauxAPIError):
    """Too many requests in a short period."""


class MarketauxEndpointRestrictedError(MarketauxAPIError):
    """Requested endpoint is not available on the subscription."""


class MarketauxResponseError(MarketauxAPIError):
    """Invalid or unexpected API response."""
