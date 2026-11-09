
from pathlib import Path
from posixpath import join

import polars as pl

from market_data.logging_config import get_pipeline_logger
from market_data.paths import (
    MARKETAUX_BRONZE_NEWS_PATH,
    MARKETAUX_NEWS_SILVER_PATH,
)
from market_data.pipelines.marketaux.silver.pipeline import (
    run_marketaux_silver_pipeline,
)
from market_data.pipelines.marketaux.silver.storage import (
    load_silver_news,
    save_silver_news,
)
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger(
    "market_data.marketaux.silver.coordinator"
)


S3_BRONZE_PREFIX = "bronze/marketaux/news"
S3_SILVER_PREFIX = "silver/marketaux/news"


def _get_final_status(
    pipeline_status: str,
    valid_files: int,
    failed_files: int,
    consolidation_failed: bool = False,
) -> str:
    if consolidation_failed:
        return "partial" if valid_files else "failed"

    if pipeline_status == "failed":
        return "failed"

    if pipeline_status == "partial":
        return "partial"

    if failed_files:
        return "partial" if valid_files else "failed"

    if pipeline_status == "skipped" and not valid_files:
        return "skipped"

    if not valid_files:
        return "skipped"

    return "success"


def run_marketaux_silver_coordinator(
    bronze_path: Path = MARKETAUX_BRONZE_NEWS_PATH,
    silver_path: Path = MARKETAUX_NEWS_SILVER_PATH,
    storage: StorageBackend | None = None,
) -> dict:
    if storage is not None:
        return _run_storage_coordinator(storage)

    return _run_local_coordinator(
        Path(bronze_path),
        Path(silver_path),
    )


def _run_local_coordinator(
    bronze_path: Path,
    silver_path: Path,
) -> dict:
    files_path = silver_path / "files"
    consolidated_file = silver_path / "news_silver.pq"

    pipeline_result = run_marketaux_silver_pipeline(
        bronze_path,
        files_path,
    )

    pipeline_status = pipeline_result.get("status", "unknown")

    silver_files = (
        sorted(files_path.rglob("*.pq"))
        if files_path.exists()
        else []
    )

    valid_batches = []
    failed_files = []

    for file_path in silver_files:
        try:
            batch = load_silver_news(file_path)

            if batch.height:
                valid_batches.append(batch)

        except Exception as error:
            failed_files.append(
                {
                    "file": str(file_path),
                    "error": str(error),
                }
            )

            logger.exception(
                "Unable to load Silver file: %s",
                file_path,
            )

    return _consolidate(
        pipeline_result=pipeline_result,
        pipeline_status=pipeline_status,
        valid_batches=valid_batches,
        failed_files=failed_files,
        consolidated_file=consolidated_file,
    )


def _run_storage_coordinator(
    storage: StorageBackend,
) -> dict:
    files_prefix = join(S3_SILVER_PREFIX, "files")
    consolidated_key = join(
        S3_SILVER_PREFIX,
        "news_silver.pq",
    )

    pipeline_result = run_marketaux_silver_pipeline(
        S3_BRONZE_PREFIX,
        files_prefix,
        storage=storage,
    )

    pipeline_status = pipeline_result.get("status", "unknown")

    silver_files = sorted(
        key
        for key in storage.list_files(files_prefix)
        if key.lower().endswith(".pq")
    )

    valid_batches = []
    failed_files = []

    for file_key in silver_files:
        try:
            batch = load_silver_news(
                file_key,
                storage=storage,
            )

            if batch.height:
                valid_batches.append(batch)

        except Exception as error:
            failed_files.append(
                {
                    "file": file_key,
                    "error": str(error),
                }
            )

            logger.exception(
                "Unable to load Silver object: %s",
                file_key,
            )

    return _consolidate(
        pipeline_result=pipeline_result,
        pipeline_status=pipeline_status,
        valid_batches=valid_batches,
        failed_files=failed_files,
        consolidated_file=consolidated_key,
        storage=storage,
    )


def _consolidate(
    pipeline_result: dict,
    pipeline_status: str,
    valid_batches: list,
    failed_files: list,
    consolidated_file: Path | str,
    storage: StorageBackend | None = None,
) -> dict:
    valid_files = len(valid_batches)

    if not valid_batches:
        exists = (
            storage.exists(str(consolidated_file))
            if storage is not None
            else Path(consolidated_file).exists()
        )

        status = _get_final_status(
            pipeline_status=pipeline_status,
            valid_files=0,
            failed_files=len(failed_files),
        )

        logger.info(
            "Marketaux Silver consolidation skipped | "
            "Pipeline status: %s | Failed files: %s",
            pipeline_status,
            len(failed_files),
        )

        return {
            "status": status,
            "pipeline_status": pipeline_status,
            "files_processed": pipeline_result.get(
                "files_processed",
                0,
            ),
            "files_loaded": 0,
            "files_failed": len(failed_files),
            "failed_files": failed_files,
            "articles_processed": 0,
            "saved_file": consolidated_file if exists else None,
            "consolidation_error": None,
        }

    try:
        combined_data = pl.concat(
            valid_batches,
            how="vertical",
        )

        if storage is not None:
            consolidated_exists = storage.exists(
                str(consolidated_file)
            )
        else:
            consolidated_exists = Path(
                consolidated_file
            ).exists()

        if consolidated_exists:
            existing_data = load_silver_news(
                consolidated_file,
                storage=storage,
            )

            combined_data = pl.concat(
                [existing_data, combined_data],
                how="vertical",
            )

        combined_data = (
            combined_data
            .unique(
                subset=["article_id", "ticker"],
                keep="last",
                maintain_order=True,
            )
            .sort(["published_at", "ticker"])
        )

    except Exception as error:
        logger.exception(
            "Marketaux Silver consolidation preparation failed."
        )

        return {
            "status": "failed",
            "pipeline_status": pipeline_status,
            "files_processed": pipeline_result.get(
                "files_processed",
                0,
            ),
            "files_loaded": valid_files,
            "files_failed": len(failed_files),
            "failed_files": failed_files,
            "articles_processed": 0,
            "saved_file": consolidated_file,
            "consolidation_error": str(error),
        }

    try:
        save_silver_news(
            combined_data,
            consolidated_file,
            overwrite=True,
            storage=storage,
        )

    except Exception as error:
        logger.exception(
            "Unable to save consolidated Marketaux Silver data."
        )

        return {
            "status": "partial" if valid_files else "failed",
            "pipeline_status": pipeline_status,
            "files_processed": pipeline_result.get(
                "files_processed",
                0,
            ),
            "files_loaded": valid_files,
            "files_failed": len(failed_files),
            "failed_files": failed_files,
            "articles_processed": 0,
            "saved_file": consolidated_file,
            "consolidation_error": str(error),
        }

    status = _get_final_status(
        pipeline_status=pipeline_status,
        valid_files=valid_files,
        failed_files=len(failed_files),
    )

    logger.info(
        "Marketaux Silver consolidation completed | "
        "Status: %s | Files loaded: %s | Failed files: %s | "
        "Articles: %s",
        status,
        valid_files,
        len(failed_files),
        combined_data.height,
    )

    return {
        "status": status,
        "pipeline_status": pipeline_status,
        "files_processed": pipeline_result.get(
            "files_processed",
            0,
        ),
        "files_loaded": valid_files,
        "files_failed": len(failed_files),
        "failed_files": failed_files,
        "articles_processed": combined_data.height,
        "saved_file": consolidated_file,
        "consolidation_error": None,
    }
