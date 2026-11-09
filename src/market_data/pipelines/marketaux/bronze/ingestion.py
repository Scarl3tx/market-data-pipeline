
import os

from datetime import datetime, timezone

from market_data.storage.base import StorageBackend
from market_data.api.marketaux.client import MarketauxClient
from market_data.api.marketaux.exceptions import (
    MarketauxAPIError,
    MarketauxAuthenticationError,
    MarketauxEndpointRestrictedError,
    MarketauxQuotaExceededError,
    MarketauxRateLimitError,
    MarketauxResponseError,
)
from market_data.logging_config import get_pipeline_logger

from market_data.pipelines.marketaux.bronze.config import (
    load_enabled_stocks,
)
from market_data.pipelines.marketaux.bronze.state import (
    MarketauxNewsState,
)
from market_data.pipelines.marketaux.bronze.storage import (
    save_raw_news,
)


logger = get_pipeline_logger("marketaux.bronze")


class _StopMarketauxRun(Exception):
    """Stop further API requests for the current pipeline run."""


def _format_api_datetime(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("A valid ISO datetime string is required.")

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Invalid ISO datetime: {value}") from None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    parsed = parsed.astimezone(timezone.utc)

    return parsed.strftime("%Y-%m-%dT%H:%M:%S")


def _is_last_page(metadata, page, limit):
    if not isinstance(page, int) or isinstance(page, bool) or page < 1:
        raise ValueError("Invalid pagination page.")

    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("Invalid pagination limit.")

    if not isinstance(metadata, dict):
        raise ValueError("Invalid Marketaux response metadata.")

    returned = metadata.get("returned")
    found = metadata.get("found")

    if not isinstance(returned, int) or isinstance(returned, bool) or returned < 0:
        raise ValueError("Invalid Marketaux metadata: returned.")

    if not isinstance(found, int) or isinstance(found, bool) or found < 0:
        raise ValueError("Invalid Marketaux metadata: found.")

    if returned > limit:
        raise ValueError("Marketaux returned more articles than requested.")

    if returned == 0:
        return True

    return returned < limit or page * limit >= found


def _get_int_env(name, default, minimum):
    value = int(os.getenv(name, str(default)))

    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}.")

    return value


def _classify_error(error):
    if isinstance(error, MarketauxQuotaExceededError):
        return "quota_exceeded", False

    if isinstance(error, MarketauxRateLimitError):
        return "rate_limit", True

    if isinstance(error, MarketauxAuthenticationError):
        return "authentication", False

    if isinstance(error, MarketauxEndpointRestrictedError):
        return "endpoint_restricted", False

    if isinstance(error, MarketauxResponseError):
        return "invalid_response", True

    if isinstance(error, MarketauxAPIError):
        return "api_error", True

    return "unexpected_error", True


def _record_failure(
    symbol,
    ingestion_type,
    page,
    state,
    error,
    results,
):
    error_category, retry_next_run = _classify_error(error)

    try:
        stock_state = state.get_stock(symbol)

        checkpoint_resumable = (
            stock_state.get("ingestion_in_progress", False)
            and stock_state.get("next_page") == page
            and stock_state.get("ingestion_type") == ingestion_type
        )
    except Exception:
        checkpoint_resumable = False

    failure = {
        "symbol": symbol,
        "type": ingestion_type,
        "page": page,
        "error_category": error_category,
        "error": str(error),
        "checkpoint_resumable": checkpoint_resumable,
        "retry_next_run": retry_next_run and checkpoint_resumable,
    }

    results["failed"].append(failure)

    logger.error(
        "Marketaux ingestion failed | Symbol: %s | Type: %s | "
        "Page: %s | Category: %s | Checkpoint resumable: %s | "
        "Retry next run: %s | Error: %s",
        symbol,
        ingestion_type,
        page,
        error_category,
        checkpoint_resumable,
        failure["retry_next_run"],
        error,
    )

    return failure


def _process_symbol_window(
    symbol,
    state,
    client,
    limit,
    results,
    storage=None,
):
    stock_state = state.get_stock(symbol)

    page = stock_state["next_page"]
    ingestion_type = stock_state["ingestion_type"]

    published_after = _format_api_datetime(
        stock_state["pending_since"]
    )
    published_before = _format_api_datetime(
        stock_state["pending_until"]
    )

    state.record_attempt(symbol)

    response = client.get_news(
        symbols=[symbol],
        page=page,
        limit=limit,
        published_after=published_after,
        published_before=published_before,
    )

    metadata = response.get("meta", {})

    last_page = _is_last_page(metadata, page, limit)

    output_path = save_raw_news(
        response=response,
        symbols=[symbol],
        page=page,
        limit=limit,
        published_after=published_after,
        published_before=published_before,
        storage=storage,
    )

    # Advance only after the raw response has been saved.
    state.record_page_saved(symbol, page)

    results["successful"].append({
        "symbol": symbol,
        "page": page,
        "path": str(output_path),
    })

    if last_page:
        state.complete_ingestion(symbol)

        if ingestion_type == "recent":
            results["recent_completed"].append(symbol)

        elif ingestion_type == "backfill":
            results["backfill_completed"].append(symbol)

        logger.info(
            "Marketaux %s ingestion completed for %s",
            ingestion_type,
            symbol,
        )

    return last_page


def _handle_symbol_failure(
    symbol,
    ingestion_type,
    state,
    error,
    results,
):
    try:
        stock_state = state.get_stock(symbol)
        page = stock_state.get("next_page")
    except Exception:
        page = None

    _record_failure(
        symbol,
        ingestion_type,
        page,
        state,
        error,
        results,
    )

    if isinstance(
        error,
        (
            MarketauxRateLimitError,
            MarketauxAuthenticationError,
            MarketauxEndpointRestrictedError,
        ),
    ):
        raise _StopMarketauxRun(
            f"Stopping Marketaux requests after {type(error).__name__}."
        ) from error


def _process_recent_coverage(
    symbols,
    state,
    client,
    limit,
    initial_lookback_days,
    overlap_hours,
    requests_per_ticker,
    results,
    storage=None,
):
    """
    Give every enabled ticker recent coverage before
    processing historical backfill.
    """

    active = list(symbols)

    while active:
        next_round = []

        for symbol in active:
            try:
                stock_state = state.get_stock(symbol)

                if not stock_state["ingestion_in_progress"]:
                    state.start_ingestion(
                        symbol,
                        initial_lookback_days=initial_lookback_days,
                        overlap_hours=overlap_hours,
                    )

                stock_state = state.get_stock(symbol)

                if stock_state["ingestion_type"] != "recent":
                    continue

                pages_processed = 0

                while pages_processed < requests_per_ticker:
                    completed = _process_symbol_window(
                        symbol,
                        state,
                        client,
                        limit,
                        results,
                        storage=storage,
                    )

                    pages_processed += 1

                    if completed:
                        break

                if state.get_stock(symbol)["ingestion_in_progress"]:
                    next_round.append(symbol)

            except MarketauxQuotaExceededError:
                raise

            except _StopMarketauxRun:
                raise

            except Exception as error:
                _handle_symbol_failure(
                    symbol,
                    "recent",
                    state,
                    error,
                    results,
                )

        active = next_round


def _process_backfill(
    candidates,
    state,
    client,
    limit,
    window_days,
    requests_per_ticker,
    results,
    storage=None,
):
    """
    Process each backfill candidate at most once per run.
    """

    for symbol, _ in candidates:
        try:
            stock_state = state.get_stock(symbol)

            if not stock_state["ingestion_in_progress"]:
                state.start_backfill(
                    symbol,
                    window_days=window_days,
                )

            stock_state = state.get_stock(symbol)

            if stock_state["ingestion_type"] != "backfill":
                continue

            pages_processed = 0

            while pages_processed < requests_per_ticker:
                completed = _process_symbol_window(
                    symbol,
                    state,
                    client,
                    limit,
                    results,
                    storage=storage,
                )

                pages_processed += 1

                if completed:
                    break

        except MarketauxQuotaExceededError:
            raise

        except _StopMarketauxRun:
            raise

        except Exception as error:
            _handle_symbol_failure(
                symbol,
                "backfill",
                state,
                error,
                results,
            )


def run_marketaux_bronze(
    client=None,
    state=None,
    stocks=None,
    storage: StorageBackend | None = None,
):
    client = client or MarketauxClient()

    if state is None:
        state = MarketauxNewsState(storage=storage)

    if stocks is None:
        stocks = load_enabled_stocks()

    state.initialize_stocks(stocks)

    initial_lookback_days = _get_int_env(
        "MARKETAUX_INITIAL_LOOKBACK_DAYS",
        30,
        1,
    )

    overlap_hours = int(
        os.getenv("MARKETAUX_OVERLAP_HOURS", "24")
    )

    if overlap_hours < 0:
        raise ValueError(
            "MARKETAUX_OVERLAP_HOURS cannot be negative."
        )

    backfill_window_days = _get_int_env(
        "MARKETAUX_BACKFILL_WINDOW_DAYS",
        30,
        1,
    )

    requests_per_ticker = _get_int_env(
        "MARKETAUX_REQUESTS_PER_TICKER",
        1,
        1,
    )

    limit = client.articles_per_request

    enabled_symbols = [
        stock["symbol"].strip().upper()
        for stock in stocks
    ]

    results = {
        "status": "success",
        "successful": [],
        "failed": [],
        "recent_completed": [],
        "backfill_completed": [],
    }

    backfill_candidates = state.get_backfill_candidates()

    try:
        pending_recent = [
            symbol
            for symbol in enabled_symbols
            if (
                (
                    state.get_stock(symbol)["ingestion_in_progress"]
                    and state.get_stock(symbol)["ingestion_type"] == "recent"
                )
                or (
                    not state.get_stock(symbol)["ingestion_in_progress"]
                    and state.get_stock(symbol)["last_successful_check"] is not None
                )
                or (
                    state.get_stock(symbol)["last_successful_check"] is None
                    and not state.get_stock(symbol)["ingestion_in_progress"]
                )
            )
        ]

        _process_recent_coverage(
            pending_recent,
            state,
            client,
            limit,
            initial_lookback_days,
            overlap_hours,
            requests_per_ticker,
            results,
            storage=storage,
        )

        _process_backfill(
            backfill_candidates,
            state,
            client,
            limit,
            backfill_window_days,
            requests_per_ticker,
            results,
            storage=storage,
        )

    except MarketauxQuotaExceededError:
        logger.warning(
            "Marketaux daily quota exhausted. "
            "Remaining work will resume on the next run."
        )

        results["status"] = "partial"

    except _StopMarketauxRun as error:
        logger.warning(
            "%s Remaining work will resume on the next run.",
            error,
        )

        results["status"] = "partial"

    if results["failed"] and results["status"] == "success":
        results["status"] = "partial"

    logger.info(
        "Marketaux Bronze completed: %s pages saved, %s failures.",
        len(results["successful"]),
        len(results["failed"]),
    )

    return results
