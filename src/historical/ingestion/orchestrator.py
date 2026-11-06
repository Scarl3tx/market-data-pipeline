
from datetime import datetime, timedelta

from src.historical.ingestion.stocks import (
    ingest_historical_stock_batch,
    update_historical_stock
)
from src.historical.ingestion.exceptions import (
    TwelveDataDailyLimitError
)
from src.historical.ingestion.progress_tracker import (
    HistoricalProgressTracker
)


def run_historical_ingestion(
    provider,
    stock_symbols: list[str],
    commodity_symbols: list[str],
    start_date: str,
    end_date: str
) -> str:

    print("\n========== HISTORICAL INGESTION STARTED ==========")

    progress_tracker = HistoricalProgressTracker(
        start_date=start_date,
        end_date=end_date
    )

    # Initialise progress for all configured stocks
    for symbol in stock_symbols:
        progress_tracker.initialise_stock(symbol=symbol)

    round_number = 1

    # ==========================================
    # BREADTH-FIRST HISTORICAL BACKFILL
    # ==========================================

    while True:

        pending_symbols = [
            symbol
            for symbol in stock_symbols
            if not progress_tracker.get_stock_progress(symbol)["completed"]
        ]

        if not pending_symbols:
            break

        print(
            f"\n========== HISTORICAL ROUND {round_number} =========="
        )

        for symbol in pending_symbols:

            stock_state = progress_tracker.get_stock_progress(symbol)
            batch_end_date = stock_state["next_end_date"]

            try:
                result = ingest_historical_stock_batch(
                    provider=provider,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=batch_end_date
                )

            except TwelveDataDailyLimitError:
                print(
                    "\n[HISTORICAL INGESTION] "
                    "Daily API quota exhausted. Stopping."
                )
                return "quota_exhausted"

            if not result["success"]:
                print(
                    f"[HISTORICAL INGESTION] "
                    f"{symbol} failed. Stopping this run; "
                    "progress will resume on the next execution."
                )
                return "failed"

            if not result["has_data"]:
                progress_tracker.complete_stock(symbol)
                continue

            earliest_date = result["earliest_date"]

            # Reached the requested earliest date
            if earliest_date <= start_date.replace("-", ""):
                progress_tracker.complete_stock(symbol)
                continue

            # Move backwards for the next batch
            next_end_date = (
                datetime.strptime(
                    earliest_date,
                    "%Y%m%d"
                ) - timedelta(days=1)
            ).strftime("%Y-%m-%d")

            progress_tracker.update_stock(
                symbol=symbol,
                next_end_date=next_end_date
            )

        round_number += 1

    print(
        "\n[HISTORICAL STOCKS] "
        "All configured stocks completed."
    )

    # ==========================================
    # INCREMENTAL HISTORICAL UPDATES
    # ==========================================

    print("\n========== HISTORICAL INCREMENTAL UPDATES ==========")

    for symbol in stock_symbols:

        try:
            result = update_historical_stock(
                provider=provider,
                symbol=symbol,
                buffer_days=30
            )

        except TwelveDataDailyLimitError:
            print(
                "\n[HISTORICAL UPDATE] "
                "Daily API quota exhausted. Stopping."
            )
            return "quota_exhausted"

        if not result["success"]:
            print(
                f"[HISTORICAL UPDATE] "
                f"{symbol} update failed."
            )

    # ==========================================
    # COMMODITIES
    # ==========================================

    print(
        "\n[HISTORICAL COMMODITIES] "
        "Provider not yet implemented. Skipping."
    )

    print("\n========== HISTORICAL INGESTION COMPLETED ==========")

    return "completed"
