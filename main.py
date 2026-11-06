
import asyncio

from alphavantage_trackers.pipelines.queue.av_bronze_coordinator import (
    run_av_bronze_coordinator
)

from alphavantage_trackers.pipelines.av_commodities.av_commodities_pipeline import (
    run_commodities_pipeline
)

from alphavantage_trackers.pipelines.av_stocks.av_stocks_pipeline import (
    run_stocks_pipeline
)

from alphavantage_trackers.pipelines.gold.gold_pipeline import (
    run_gold_pipeline
)

from alphavantage_trackers.logging_config import get_pipeline_logger

from src.historical.historical_orchestrator import (
    run_historical_pipeline
)

from src.historical.transformation.orchestrator import (
    run_historical_silver_pipeline
)

logger = get_pipeline_logger("main")


async def main():

    print("\n========== ALPHAVANTAGE TRACKER ==========")
    logger.info("Main Pipeline Started")

    # BRONZE: Run both providers concurrently
    logger.info("Starting Alpha Vantage and Twelve Data Bronze pipelines.")

    results = await asyncio.gather(
        asyncio.to_thread(run_av_bronze_coordinator),
        asyncio.to_thread(run_historical_pipeline),
        return_exceptions=True
    )

    av_bronze_result, historical_result = results

    # Evaluate Alpha Vantage Bronze
    av_bronze_success = (
        not isinstance(av_bronze_result, Exception)
        and av_bronze_result is not False
    )

    if av_bronze_success:
        logger.info("Alpha Vantage Bronze completed successfully.")
    else:
        if isinstance(av_bronze_result, Exception):
            logger.error(
                "Alpha Vantage Bronze encountered an error.",
                exc_info=(
                    type(av_bronze_result),
                    av_bronze_result,
                    av_bronze_result.__traceback__
                )
            )
        else:
            logger.error("Alpha Vantage Bronze failed.")

    # Evaluate Twelve Data Bronze
    if isinstance(historical_result, Exception):
        logger.error(
            "Twelve Data Historical Bronze encountered an error.",
            exc_info=(
                type(historical_result),
                historical_result,
                historical_result.__traceback__
            )
        )
    elif historical_result == "completed":
        logger.info("Twelve Data Historical Bronze completed successfully.")
    elif historical_result == "quota_exhausted":
        logger.warning(
            "Twelve Data daily quota exhausted. "
            "Historical ingestion will resume on the next run."
        )
    elif historical_result == "failed":
        logger.error(
            "Twelve Data Historical Bronze failed. "
            "Progress will resume on the next run."
        )
    else:
        logger.warning(
            "Twelve Data returned an unexpected result: %s",
            historical_result
        )

    # HISTORICAL SILVER
    # Process all available historical Bronze data,
    # even if Twelve Data ingestion was incomplete.
    logger.info("Starting Historical Silver pipeline.")

    historical_silver_result = await asyncio.to_thread(
        run_historical_silver_pipeline
    )

    if historical_silver_result:
        logger.info("Historical Silver completed successfully.")
    else:
        logger.error("Historical Silver encountered failures.")

    # ALPHA VANTAGE SILVER
    # Only depends on Alpha Vantage Bronze.
    if not av_bronze_success:
        logger.error(
            "Alpha Vantage Bronze failed. "
            "Skipping Alpha Vantage Silver and Gold."
        )
        print("\n========== ALPHAVANTAGE PIPELINE FAILED ==========")
        return

    pipelines = {
        "COMMODITIES": run_commodities_pipeline,
        "STOCKS": run_stocks_pipeline
    }

    results = await asyncio.gather(
        *[asyncio.to_thread(pipeline) for pipeline in pipelines.values()],
        return_exceptions=True
    )

    overall_success = True

    for name, result in zip(pipelines.keys(), results):
        if isinstance(result, Exception):
            logger.error(
                "[%s] Pipeline encountered an error.",
                name,
                exc_info=(type(result), result, result.__traceback__)
            )
            overall_success = False

        elif result is False:
            logger.error("[%s] Pipeline failed.", name)
            overall_success = False

        else:
            logger.info("[%s] Pipeline completed successfully.", name)

    # GOLD
    # Historical Silver is not integrated into Gold yet.
    if overall_success:
        logger.info("All Alpha Vantage Silver pipelines succeeded. Starting Gold.")

        gold_success = await asyncio.to_thread(run_gold_pipeline)

        if gold_success:
            logger.info("Gold pipeline completed successfully.")
        else:
            logger.error("Gold pipeline failed.")
            overall_success = False

    else:
        logger.warning("At least one Alpha Vantage Silver pipeline failed. Skipping Gold.")

    if overall_success:
        print("\n========== ALPHAVANTAGE PIPELINES COMPLETED ==========")
        logger.info("All Alpha Vantage pipelines completed successfully.")
    else:
        print("\n========== SOME ALPHAVANTAGE PIPELINES FAILED ==========")
        logger.warning("Some Alpha Vantage pipelines failed.")


if __name__ == "__main__":
    asyncio.run(main())
