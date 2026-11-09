
import json

from market_data.paths import STOCKS_CONFIG_PATH


class MarketauxConfigError(Exception):
    """Invalid Marketaux ingestion configuration."""


def load_enabled_stocks(config_path=STOCKS_CONFIG_PATH):
    """
    Load enabled stocks from the shared stock configuration.

    Returns:
        list[dict]: Enabled stocks containing symbol and name.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = json.load(file)

    except FileNotFoundError:
        raise MarketauxConfigError(
            f"Stock configuration not found: {config_path}"
        ) from None

    except json.JSONDecodeError:
        raise MarketauxConfigError(
            f"Invalid JSON in stock configuration: {config_path}"
        ) from None

    if not isinstance(config, dict):
        raise MarketauxConfigError(
            "Stock configuration must be a JSON object."
        )

    stocks = config.get("stocks")

    if not isinstance(stocks, list):
        raise MarketauxConfigError(
            "Stock configuration must contain a stocks list."
        )

    enabled_stocks = []
    seen_symbols = set()

    for index, stock in enumerate(stocks):
        if not isinstance(stock, dict):
            raise MarketauxConfigError(
                f"Stock entry at index {index} must be an object."
            )

        enabled = stock.get("enabled", False)

        if not isinstance(enabled, bool):
            raise MarketauxConfigError(
                f"Stock entry at index {index} has an invalid enabled value. "
                "Expected true or false."
            )

        if not enabled:
            continue

        symbol = stock.get("symbol")
        name = stock.get("name")

        if not isinstance(symbol, str) or not symbol.strip():
            raise MarketauxConfigError(
                f"Enabled stock at index {index} "
                "has an invalid symbol."
            )

        if not isinstance(name, str) or not name.strip():
            raise MarketauxConfigError(
                f"Enabled stock at index {index} "
                "has an invalid name."
            )

        symbol = symbol.strip().upper()
        name = name.strip()

        if symbol in seen_symbols:
            raise MarketauxConfigError(
                f"Duplicate enabled stock symbol: {symbol}"
            )

        seen_symbols.add(symbol)

        enabled_stocks.append({
            "symbol": symbol,
            "name": name,
        })

    return enabled_stocks
