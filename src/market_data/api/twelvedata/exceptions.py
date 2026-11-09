class TwelveDataDailyLimitError(Exception):
    """Raised when Twelve Data's daily credit limit is reached."""
    pass


class TwelveDataAPIError(Exception):
    """Raised when Twelve Data returns an API or HTTP error."""
    pass
