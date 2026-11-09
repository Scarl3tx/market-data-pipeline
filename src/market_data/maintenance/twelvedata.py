
from datetime import date
from pathlib import Path

import polars as pl

from market_data.logging_config import get_pipeline_logger
from market_data.paths import (
    TWELVEDATA_BRONZE_PATH,
    TWELVEDATA_STOCKS_BRONZE_PATH,
)
from market_data.maintenance.parquet_compaction import (
    compact_parquet_files,
)

logger = get_pipeline_logger("maintenance.twelvedata")


def _get_date_range(
    source_files: list[Path],
) -> tuple[date, date]:
    earliest = None
    latest = None

    for file_path in source_files:
        data = pl.read_parquet(
            file_path,
            columns=["date"],
        )

        if data.is_empty():
            continue

        dates = data.select(
            pl.col("date").cast(pl.Date, strict=True)
        )["date"]

        if dates.null_count():
            raise ValueError(
                f"Null dates found in Bronze file: {file_path}"
            )

        date_values = dates.to_list()

        if not date_values:
            continue

        file_earliest = min(date_values)
        file_latest = max(date_values)

        if file_earliest is None or file_latest is None:
            continue

        earliest = (
            file_earliest
            if earliest is None
            else min(earliest, file_earliest)
        )

        latest = (
            file_latest
            if latest is None
            else max(latest, file_latest)
        )

    if earliest is None or latest is None:
        raise ValueError(
            "No valid dates found in the Bronze files."
        )

    return earliest, latest


def compact_twelvedata_bronze(
    bronze_path: Path = TWELVEDATA_BRONZE_PATH,
    dry_run: bool = True,
    delete_sources: bool = False,
) -> dict:
    """
    Compact Twelve Data Bronze Parquet files independently per symbol.

    Local structure:
        bronze/twelvedata/stocks/{SYMBOL}/*.pq

    Defaults to dry-run mode and never deletes source files unless
    explicitly requested.
    """

    bronze_path = Path(bronze_path)
    stocks_path = (
        TWELVEDATA_STOCKS_BRONZE_PATH
        if bronze_path == TWELVEDATA_BRONZE_PATH
        else bronze_path / "stocks"
    )

    if not bronze_path.exists():
        return {
            "status": "skipped",
            "symbols_processed": 0,
            "results": {},
        }

    if not bronze_path.is_dir():
        raise NotADirectoryError(bronze_path)

    if not stocks_path.exists():
        return {
            "status": "skipped",
            "symbols_processed": 0,
            "results": {},
        }

    if not stocks_path.is_dir():
        raise NotADirectoryError(stocks_path)

    results = {}

    for symbol_path in sorted(stocks_path.iterdir()):
        if not symbol_path.is_dir():
            continue

        symbol = symbol_path.name

        source_files = sorted(
            symbol_path.glob(f"{symbol}_*_raw.pq")
        )

        if len(source_files) < 2:
            results[symbol] = {
                "status": "skipped",
                "files_found": len(source_files),
                "reason": "Fewer than two source files.",
            }
            continue

        try:
            earliest, latest = _get_date_range(source_files)

            output_path = symbol_path / (
                f"{symbol}_{earliest:%Y%m%d}_"
                f"{latest:%Y%m%d}_raw.pq"
            )

            result = compact_parquet_files(
                source_files=source_files,
                output_path=output_path,
                deduplicate_by=["date"],
                sort_by=["date"],
                dry_run=dry_run,
                delete_sources=delete_sources,
            )

            results[symbol] = result

        except Exception as error:
            logger.exception(
                "Twelve Data Bronze compaction failed for %s.",
                symbol,
            )

            results[symbol] = {
                "status": "failed",
                "files_found": len(source_files),
                "error": str(error),
            }

    statuses = [
        result["status"]
        for result in results.values()
    ]

    if not statuses or all(
        status == "skipped" for status in statuses
    ):
        overall_status = "skipped"

    elif "failed" in statuses:
        overall_status = (
            "partial"
            if any(
                status in {"success", "dry_run"}
                for status in statuses
            )
            else "failed"
        )

    elif "partial" in statuses:
        overall_status = "partial"

    elif "dry_run" in statuses:
        overall_status = "dry_run"

    else:
        overall_status = "success"

    return {
        "status": overall_status,
        "symbols_processed": len(results),
        "results": results,
    }
