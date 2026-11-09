
import json
from datetime import date
from pathlib import Path

from market_data.paths import STOCKS_CONFIG_PATH


def _validate_stock_entry(stock: object, index: int) -> dict:
    """Validate and normalize one stock configuration entry."""

    prefix = f"Invalid stock configuration at stocks[{index}]"

    if not isinstance(stock, dict):
        raise ValueError(f"{prefix}: expected a JSON object.")

    # Symbol
    symbol = stock.get("symbol")

    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError(
            f"{prefix}: 'symbol' must be a non-empty string."
        )

    symbol = symbol.strip().upper()

    if any(character.isspace() for character in symbol):
        raise ValueError(
            f"{prefix}: 'symbol' must not contain whitespace."
        )

    # Name
    name = stock.get("name")

    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            f"{prefix}: 'name' must be a non-empty string."
        )

    # Enabled
    enabled = stock.get("enabled", True)

    if not isinstance(enabled, bool):
        raise ValueError(
            f"{prefix}: 'enabled' must be a boolean."
        )

    # Start date
    start_date = stock.get("start_date")

    if not isinstance(start_date, str):
        raise ValueError(
            f"{prefix}: 'start_date' must be a string in YYYY-MM-DD format."
        )

    try:
        parsed_date = date.fromisoformat(start_date)
    except ValueError as exc:
        raise ValueError(
            f"{prefix}: invalid 'start_date' {start_date!r}. "
            "Expected YYYY-MM-DD."
        ) from exc

    if parsed_date.isoformat() != start_date:
        raise ValueError(
            f"{prefix}: invalid 'start_date' {start_date!r}. "
            "Expected YYYY-MM-DD."
        )

    # Return a normalized copy without modifying the loaded JSON object.
    validated_stock = stock.copy()
    validated_stock["symbol"] = symbol
    validated_stock["name"] = name.strip()
    validated_stock["enabled"] = enabled

    return validated_stock


def load_stocks_config() -> list[dict]:
    """Load, validate and return enabled stock configurations."""

    config_path = Path(STOCKS_CONFIG_PATH)

    try:
        with config_path.open("r", encoding="utf-8") as file:
            config = json.load(file)

    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in stocks configuration file "
            f"{config_path}: {exc}"
        ) from exc

    if not isinstance(config, dict):
        raise ValueError(
            "Stocks configuration must contain a JSON object."
        )

    stocks = config.get("stocks")

    if not isinstance(stocks, list):
        raise ValueError(
            "Stocks configuration must contain a 'stocks' list."
        )

    validated_stocks = []
    seen_symbols = set()

    for index, stock in enumerate(stocks):
        validated_stock = _validate_stock_entry(stock, index)

        symbol = validated_stock["symbol"]

        if symbol in seen_symbols:
            raise ValueError(
                f"Duplicate stock symbol {symbol!r} "
                f"found at stocks[{index}]."
            )

        seen_symbols.add(symbol)
        validated_stocks.append(validated_stock)

    return [
        stock
        for stock in validated_stocks
        if stock["enabled"]
    ]
