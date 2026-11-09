
import logging

from market_data.pipelines.main import run_market_data_pipeline


logger = logging.getLogger(__name__)


def main() -> int:
    """Run the market-data pipeline and return a process exit code."""

    try:
        results = run_market_data_pipeline()
    except Exception:
        logger.exception("Market-data pipeline terminated unexpectedly.")
        return 2

    status = results.get("status", "failed")

    exit_codes = {
        "success": 0,
        "skipped": 0,
        "partial_failure": 1,
        "failed": 2,
    }

    exit_code = exit_codes.get(status, 2)

    logger.info(
        "Market-data CLI completed with status=%s, exit_code=%s",
        status,
        exit_code,
    )

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
