import pytest

from market_data.api.alphavantage.exceptions import (
    AlphaVantageAPIError,
    AlphaVantageRateLimitError,
    AlphaVantageQuotaExceededError,
)


@pytest.mark.parametrize(
    "exception",
    [
        AlphaVantageAPIError,
        AlphaVantageRateLimitError,
        AlphaVantageQuotaExceededError,
    ],
)
def test_exceptions_can_be_raised(exception):
    with pytest.raises(exception):
        raise exception("Test error")