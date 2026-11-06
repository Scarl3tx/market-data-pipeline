
from datetime import datetime, timezone

from alphavantage_trackers.pipelines.av_stocks.bronze.extract_alphavantage_stocks_data import (
    extract_raw_stock,
    save_raw_to_disk as save_stock_to_disk
)

from alphavantage_trackers.pipelines.av_commodities.bronze.extract_alphavantage_commodities_data import (
    extract_raw_commodities,
    save_raw_to_disk as save_commodities_to_disk
)

from alphavantage_trackers.paths import (
    STOCKS_PATH,
    COMMODITIES_PATH
)

from alphavantage_trackers.logging_config import get_pipeline_logger
from alphavantage_trackers.api.exceptions import AlphaVantageRateLimitError

logger = get_pipeline_logger("av_worker")


def run_av_queue_worker(queue, max_attempts=3) -> bool:

    logger.info("Starting Alpha Vantage queue worker.")

    overall_success = True
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H")

    while True:

        job = queue.get_next_job()

        if job is None:
            break

        job_id = job["job_id"]
        asset_type = job["asset_type"]
        symbol = job["symbol"]
        config = job["config"]

        logger.info(
            "[QUEUE] Processing job %s: %s - %s (Attempt %s)",
            job_id,
            asset_type,
            symbol,
            job["attempts"]
        )

        try:

            # --------------------------------
            # STOCK EXTRACTION
            # --------------------------------

            if asset_type == "stock":

                expected_file = (
                    STOCKS_PATH
                    / symbol
                    / f"{timestamp}_{symbol}_raw.pq"
                )

                if expected_file.exists():
                    logger.info(
                        "[STOCK] %s already exists. Skipping extraction.",
                        symbol
                    )
                    queue.complete_job(job_id)
                    continue

                payload = extract_raw_stock(symbol, config)

                if not payload:
                    raise RuntimeError(
                        f"Stock extraction returned no data for {symbol}"
                    )

                if not save_stock_to_disk(symbol, payload):
                    raise RuntimeError(
                        f"Failed to save stock data for {symbol}"
                    )

            # --------------------------------
            # COMMODITY EXTRACTION
            # --------------------------------

            elif asset_type == "commodities":

                expected_file = (
                    COMMODITIES_PATH
                    / symbol
                    / f"{timestamp}_{symbol}_raw.pq"
                )

                if expected_file.exists():
                    logger.info(
                        "[COMMODITIES] %s already exists. Skipping extraction.",
                        symbol
                    )
                    queue.complete_job(job_id)
                    continue

                payload = extract_raw_commodities(symbol, config)

                if not payload:
                    raise RuntimeError(
                        f"Commodity extraction returned no data for {symbol}"
                    )

                if not save_commodities_to_disk(symbol, payload):
                    raise RuntimeError(
                        f"Failed to save commodity data for {symbol}"
                    )

            else:
                raise ValueError(
                    f"Unsupported asset type: {asset_type}"
                )

            # --------------------------------
            # JOB COMPLETION
            # --------------------------------

            queue.complete_job(job_id)

            logger.info(
                "[QUEUE] Job %s completed successfully.",
                job_id
            )

        # --------------------------------
        # RATE LIMIT HANDLING
        # --------------------------------

        except AlphaVantageRateLimitError as exc:

            logger.warning(
                "[QUEUE] Rate limit response for %s: %s",
                symbol,
                exc
            )

            queue.fail_job(
                job_id,
                str(exc),
                max_attempts=max_attempts
            )

            overall_success = False

            logger.warning(
                "[QUEUE] Pausing worker because Alpha Vantage "
                "returned a rate-limit response."
            )

            break

        # --------------------------------
        # GENERAL FAILURE HANDLING
        # --------------------------------

        except Exception as exc:

            logger.exception(
                "[QUEUE] Job %s failed: %s",
                job_id,
                exc
            )

            queue.fail_job(
                job_id,
                str(exc),
                max_attempts=max_attempts
            )

            if job["attempts"] >= max_attempts:

                overall_success = False

                logger.error(
                    "[QUEUE] Job %s exhausted its retry attempts.",
                    job_id
                )

    logger.info("Alpha Vantage queue worker finished.")

    return overall_success
