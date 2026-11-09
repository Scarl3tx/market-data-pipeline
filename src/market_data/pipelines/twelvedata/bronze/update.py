
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

from market_data.config import load_stocks_config
from market_data.paths import (
    TWELVEDATA_BRONZE_PATH,
    TWELVEDATA_HOURLY_STATE,
)
from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)
from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend

from market_data.pipelines.twelvedata.bronze.storage import (
    get_latest_stock_date,
    get_oldest_stock_date,
)
from market_data.pipelines.twelvedata.bronze.stocks import (
    ingest_stock_batch,
)
from market_data.pipelines.twelvedata.bronze.hourly_tracker import (
    has_pulled_this_hour,
    record_hourly_pull,
)
from market_data.pipelines.twelvedata.bronze.state import (
    initialize_symbols,
    get_symbol_state,
    mark_recent_complete,
    mark_backfill_complete,
)


logger = get_pipeline_logger("twelvedata.bronze")

SINGAPORE_TIMEZONE = ZoneInfo("Asia/Singapore")
NEW_YORK_TIMEZONE = ZoneInfo("America/New_York")

S3_HOURLY_STATE_KEY = "state/twelvedata/hourly_state.json"
S3_INGESTION_STATE_KEY = "state/twelvedata/ingestion_state.json"


def _get_state_path(bronze_path: Path) -> Path:
    return (
        bronze_path.parent
        / "state"
        / "twelvedata_ingestion_state.json"
    )


def _get_latest_completed_session(now: datetime) -> date | None:
    """Return the latest NASDAQ session whose market close has passed."""

    if now.tzinfo is None:
        now = now.replace(tzinfo=SINGAPORE_TIMEZONE)

    now_utc = now.astimezone(timezone.utc)
    market_date = now.astimezone(NEW_YORK_TIMEZONE).date()

    calendar = mcal.get_calendar("NASDAQ")

    schedule = calendar.schedule(
        start_date=market_date - timedelta(days=30),
        end_date=market_date,
    )

    if schedule.empty:
        return None

    completed_sessions = schedule[
        schedule["market_close"] <= now_utc
    ]

    if completed_sessions.empty:
        return None

    return completed_sessions.index[-1].date()


def _call_with_storage(
    function,
    *args,
    storage: StorageBackend | None = None,
    **kwargs,
):
    """Preserve existing function signatures when using local storage."""

    if storage is not None:
        kwargs["storage"] = storage

    return function(*args, **kwargs)


def _is_successful(result: dict) -> bool:
    return result.get("success", False)


def _has_data(result: dict) -> bool:
    return result.get(
        "has_data",
        result.get("rows", 0) > 0,
    )


def _hourly_skip_result(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "success": True,
        "skipped": True,
        "reason": "Already pulled this hour",
    }


def _current_skip_result(
    symbol: str,
    latest_date: date,
    latest_session: date,
) -> dict:
    return {
        "symbol": symbol,
        "success": True,
        "skipped": True,
        "reason": "Already up to date",
        "latest_date": latest_date.isoformat(),
        "latest_completed_session": latest_session.isoformat(),
    }


def _failed_ingestion_result(
    symbol: str,
    error: Exception,
) -> dict:
    return {
        "symbol": symbol,
        "success": False,
        "rows": 0,
        "earliest_date": None,
        "latest_date": None,
        "has_data": False,
        "error": str(error),
    }


def _ingest(
    provider,
    symbol: str,
    start_date: str,
    end_date: str,
    bronze_path: Path,
    state_path: str | Path,
    now: datetime,
    storage: StorageBackend | None = None,
) -> dict:
    try:
        result = _call_with_storage(
            ingest_stock_batch,
            provider=provider,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            bronze_path=bronze_path,
            storage=storage,
        )

        if _is_successful(result):
            _call_with_storage(
                record_hourly_pull,
                state_path=state_path,
                symbol=symbol,
                now=now,
                storage=storage,
            )

        return result

    except TwelveDataDailyLimitError:
        logger.warning(
            "Twelve Data daily limit reached while processing %s.",
            symbol,
        )
        raise

    except Exception as exc:
        logger.exception(
            "Unexpected ingestion failure for %s. "
            "Continuing with remaining symbols.",
            symbol,
        )

        return _failed_ingestion_result(
            symbol=symbol,
            error=exc,
        )


def _refresh_completed_stocks(
    provider,
    stocks: list[dict],
    bronze_path: Path,
    state_path: str | Path,
    ingestion_state_path: str | Path,
    now: datetime,
    latest_session: date,
    storage: StorageBackend | None = None,
) -> list[dict]:
    """Refresh stocks whose historical backfill has been completed."""

    results = []

    for stock in stocks:
        symbol = stock["symbol"]

        stock_state = _call_with_storage(
            get_symbol_state,
            ingestion_state_path,
            symbol,
            storage=storage,
        )

        if not (
            stock_state.get("recent_complete", False)
            and stock_state.get("backfill_complete", False)
        ):
            continue

        latest_date = _call_with_storage(
            get_latest_stock_date,
            bronze_path,
            symbol,
            storage=storage,
        )

        if latest_date is None:
            logger.warning(
                "Skipping refresh for %s: no stored stock data.",
                symbol,
            )
            continue

        if latest_date >= latest_session:
            logger.info(
                "Skipping %s: already current through %s.",
                symbol,
                latest_session,
            )
            results.append(
                _current_skip_result(
                    symbol,
                    latest_date,
                    latest_session,
                )
            )
            continue

        already_pulled = _call_with_storage(
            has_pulled_this_hour,
            state_path=state_path,
            symbol=symbol,
            now=now,
            storage=storage,
        )

        if already_pulled:
            logger.info(
                "Skipping %s: already pulled this hour.",
                symbol,
            )
            results.append(_hourly_skip_result(symbol))
            continue

        incremental_start = latest_date + timedelta(days=1)

        logger.info(
            "Refreshing %s: %s to %s.",
            symbol,
            incremental_start,
            latest_session,
        )

        result = _ingest(
            provider=provider,
            symbol=symbol,
            start_date=incremental_start.isoformat(),
            end_date=latest_session.isoformat(),
            bronze_path=bronze_path,
            state_path=state_path,
            now=now,
            storage=storage,
        )

        results.append(result)

    return results


def update_stocks(
    provider,
    bronze_path=TWELVEDATA_BRONZE_PATH,
    state_path=None,
    ingestion_state_path=None,
    today=None,
    now=None,
    storage: StorageBackend | None = None,
) -> list[dict]:
    """Run recent coverage, historical backfill, and incremental refresh."""

    now = now or datetime.now()
    today = today or now.date()

    bronze_path = Path(bronze_path)

    if state_path is None:
        state_path = (
            S3_HOURLY_STATE_KEY
            if storage is not None
            else TWELVEDATA_HOURLY_STATE
        )

    if ingestion_state_path is None:
        ingestion_state_path = (
            S3_INGESTION_STATE_KEY
            if storage is not None
            else _get_state_path(bronze_path)
        )

    if storage is None:
        state_path = Path(state_path)
        ingestion_state_path = Path(ingestion_state_path)

    results = []
    stocks = load_stocks_config()

    logger.info(
        "Starting stock updates: %s configured stocks.",
        len(stocks),
    )

    symbols = [stock["symbol"] for stock in stocks]

    _call_with_storage(
        initialize_symbols,
        state_path=ingestion_state_path,
        symbols=symbols,
        storage=storage,
    )

    latest_session = _get_latest_completed_session(now)

    # =========================================================
    # PHASE 1: RECENT COVERAGE
    # =========================================================

    pending_recent = []

    for stock in stocks:
        symbol = stock["symbol"]

        stock_state = _call_with_storage(
            get_symbol_state,
            ingestion_state_path,
            symbol,
            storage=storage,
        )

        if stock_state.get("recent_complete", False):
            continue

        latest_date = _call_with_storage(
            get_latest_stock_date,
            bronze_path,
            symbol,
            storage=storage,
        )

        pending_recent.append({
            **stock,
            "latest_date": latest_date,
        })

    if pending_recent:
        pending_recent.sort(
            key=lambda stock: (
                stock["latest_date"] or date.min,
                stock["symbol"],
            )
        )

        logger.info(
            "Recent coverage phase: %s stocks remaining.",
            len(pending_recent),
        )

        for stock in pending_recent:
            symbol = stock["symbol"]

            already_pulled = _call_with_storage(
                has_pulled_this_hour,
                state_path=state_path,
                symbol=symbol,
                now=now,
                storage=storage,
            )

            if already_pulled:
                logger.info(
                    "Skipping %s: already pulled this hour.",
                    symbol,
                )
                results.append(_hourly_skip_result(symbol))
                continue

            configured_start = date.fromisoformat(
                stock["start_date"]
            )

            logger.info(
                "Recent coverage priority: %s | Latest filled: %s",
                symbol,
                stock["latest_date"],
            )

            result = _ingest(
                provider=provider,
                symbol=symbol,
                start_date=configured_start.isoformat(),
                end_date=today.isoformat(),
                bronze_path=bronze_path,
                state_path=state_path,
                now=now,
                storage=storage,
            )

            results.append(result)

            if not _is_successful(result):
                continue

            _call_with_storage(
                mark_recent_complete,
                ingestion_state_path,
                symbol,
                storage=storage,
            )

            if not _has_data(result):
                _call_with_storage(
                    mark_backfill_complete,
                    ingestion_state_path,
                    symbol,
                    storage=storage,
                )

        logger.info("Recent coverage round completed.")
        return results

    # =========================================================
    # PHASE 2: HISTORICAL BACKFILL
    # =========================================================

    pending_backfill = []

    for stock in stocks:
        symbol = stock["symbol"]

        stock_state = _call_with_storage(
            get_symbol_state,
            ingestion_state_path,
            symbol,
            storage=storage,
        )

        if stock_state.get("backfill_complete", False):
            continue

        oldest_date = _call_with_storage(
            get_oldest_stock_date,
            bronze_path,
            symbol,
            storage=storage,
        )

        pending_backfill.append({
            **stock,
            "oldest_date": oldest_date,
        })

    if pending_backfill:
        pending_backfill.sort(
            key=lambda stock: (
                stock["oldest_date"] or date.min,
                stock["symbol"],
            ),
            reverse=True,
        )

        logger.info(
            "Historical backfill phase: %s stocks remaining.",
            len(pending_backfill),
        )

        for stock in pending_backfill:
            symbol = stock["symbol"]

            already_pulled = _call_with_storage(
                has_pulled_this_hour,
                state_path=state_path,
                symbol=symbol,
                now=now,
                storage=storage,
            )

            if already_pulled:
                logger.info(
                    "Skipping %s: already pulled this hour.",
                    symbol,
                )
                results.append(_hourly_skip_result(symbol))
                continue

            configured_start = date.fromisoformat(
                stock["start_date"]
            )

            oldest_date = stock["oldest_date"]

            if oldest_date is None:
                logger.warning(
                    "%s has no stored historical data.",
                    symbol,
                )

                _call_with_storage(
                    mark_backfill_complete,
                    ingestion_state_path,
                    symbol,
                    storage=storage,
                )
                continue

            if oldest_date <= configured_start:
                logger.info(
                    "%s has reached its configured start date.",
                    symbol,
                )

                _call_with_storage(
                    mark_backfill_complete,
                    ingestion_state_path,
                    symbol,
                    storage=storage,
                )
                continue

            latest_date = _call_with_storage(
                get_latest_stock_date,
                bronze_path,
                symbol,
                storage=storage,
            )

            # Catch up recent data before fetching older history.
            if (
                latest_date is not None
                and latest_session is not None
                and latest_date < latest_session
            ):
                incremental_start = latest_date + timedelta(days=1)

                logger.info(
                    "Updating recent data for %s: %s to %s.",
                    symbol,
                    incremental_start,
                    latest_session,
                )

                result = _ingest(
                    provider=provider,
                    symbol=symbol,
                    start_date=incremental_start.isoformat(),
                    end_date=latest_session.isoformat(),
                    bronze_path=bronze_path,
                    state_path=state_path,
                    now=now,
                    storage=storage,
                )

                results.append(result)
                continue

            # Fetch the next historical batch.
            backfill_end = oldest_date - timedelta(days=1)

            logger.info(
                "Backfilling %s: %s to %s.",
                symbol,
                configured_start,
                backfill_end,
            )

            result = _ingest(
                provider=provider,
                symbol=symbol,
                start_date=configured_start.isoformat(),
                end_date=backfill_end.isoformat(),
                bronze_path=bronze_path,
                state_path=state_path,
                now=now,
                storage=storage,
            )

            results.append(result)

            if not _is_successful(result):
                continue

            if not _has_data(result):
                logger.info(
                    "%s: no more historical data available.",
                    symbol,
                )

                _call_with_storage(
                    mark_backfill_complete,
                    ingestion_state_path,
                    symbol,
                    storage=storage,
                )
                continue

            returned_oldest = datetime.strptime(
                result["earliest_date"],
                "%Y%m%d",
            ).date()

            if returned_oldest <= configured_start:
                logger.info(
                    "%s reached its configured historical start date.",
                    symbol,
                )

                _call_with_storage(
                    mark_backfill_complete,
                    ingestion_state_path,
                    symbol,
                    storage=storage,
                )

        logger.info("Historical backfill round completed.")
        return results

    # =========================================================
    # PHASE 3: RECURRING INCREMENTAL REFRESH
    # =========================================================

    if latest_session is None:
        logger.info(
            "No completed NASDAQ session is available for refresh."
        )
        return results

    logger.info(
        "Recurring refresh phase: latest completed NASDAQ session %s.",
        latest_session,
    )

    results.extend(
        _refresh_completed_stocks(
            provider=provider,
            stocks=stocks,
            bronze_path=bronze_path,
            state_path=state_path,
            ingestion_state_path=ingestion_state_path,
            now=now,
            latest_session=latest_session,
            storage=storage,
        )
    )

    logger.info("Recurring stock refresh completed.")

    return results
