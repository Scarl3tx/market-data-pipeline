
import json
import uuid

from datetime import datetime, timezone
from pathlib import Path

from market_data.paths import MARKETAUX_BRONZE_NEWS_PATH
from market_data.storage.base import StorageBackend


S3_NEWS_PREFIX = "bronze/marketaux/news"


class MarketauxStorageError(Exception):
    """Invalid Marketaux raw data supplied for storage."""


def save_raw_news(
    response,
    symbols,
    page=1,
    output_dir=MARKETAUX_BRONZE_NEWS_PATH,
    limit=None,
    published_after=None,
    published_before=None,
    storage: StorageBackend | None = None,
):
    if not isinstance(response, dict):
        raise MarketauxStorageError(
            "API response must be a dictionary."
        )

    if not isinstance(response.get("data"), list):
        raise MarketauxStorageError(
            "API response must contain a data list."
        )

    if isinstance(symbols, str):
        symbols = [symbols]

    if not isinstance(symbols, (list, tuple)) or not symbols:
        raise MarketauxStorageError(
            "Symbols must be a non-empty string or list of strings."
        )

    normalized_symbols = []

    for symbol in symbols:
        if not isinstance(symbol, str) or not symbol.strip():
            raise MarketauxStorageError(
                "Every symbol must be a non-empty string."
            )

        normalized_symbols.append(symbol.strip().upper())

    if (
        not isinstance(page, int)
        or isinstance(page, bool)
        or page < 1
    ):
        raise MarketauxStorageError(
            "Page must be a positive integer."
        )

    if limit is not None and (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or limit < 1
    ):
        raise MarketauxStorageError(
            "Limit must be a positive integer."
        )

    for field_name, value in (
        ("published_after", published_after),
        ("published_before", published_before),
    ):
        if value is not None and (
            not isinstance(value, str) or not value.strip()
        ):
            raise MarketauxStorageError(
                f"{field_name} must be a non-empty string."
            )

    retrieved_at = datetime.now(timezone.utc)
    timestamp = retrieved_at.strftime("%Y%m%dT%H%M%S_%fZ")

    filename = (
        f"{timestamp}_page_{page:03d}_"
        f"{uuid.uuid4().hex[:8]}.json"
    )

    request_metadata = {
        "symbols": normalized_symbols,
        "page": page,
    }

    if limit is not None:
        request_metadata["limit"] = limit

    if published_after is not None:
        request_metadata["published_after"] = published_after

    if published_before is not None:
        request_metadata["published_before"] = published_before

    raw_record = {
        "retrieved_at": retrieved_at.isoformat(),
        "request": request_metadata,
        "response": response,
    }

    try:
        if storage is not None:
            storage_key = (
                f"{S3_NEWS_PREFIX}/"
                f"{retrieved_at:%Y/%m/%d}/"
                f"{filename}"
            )

            storage.write_json(
                storage_key,
                raw_record,
                overwrite=False,
            )

            return storage_key

        date_directory = (
            Path(output_dir)
            / retrieved_at.strftime("%Y")
            / retrieved_at.strftime("%m")
            / retrieved_at.strftime("%d")
        )
        date_directory.mkdir(parents=True, exist_ok=True)

        output_path = date_directory / filename
        temporary_path = output_path.with_suffix(".json.tmp")

        try:
            with open(
                temporary_path,
                "w",
                encoding="utf-8",
            ) as file:
                json.dump(
                    raw_record,
                    file,
                    ensure_ascii=False,
                    indent=4,
                )

            temporary_path.replace(output_path)

        finally:
            temporary_path.unlink(missing_ok=True)

        return output_path

    except Exception as error:
        raise MarketauxStorageError(
            "Failed to save Marketaux raw response."
        ) from error
