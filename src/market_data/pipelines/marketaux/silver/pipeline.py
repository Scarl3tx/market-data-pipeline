
import json
from pathlib import Path
from posixpath import join, relpath

from market_data.pipelines.marketaux.silver.storage import (
    save_silver_news,
)
from market_data.pipelines.marketaux.silver.transform import (
    transform_news,
)
from market_data.logging_config import get_pipeline_logger
from market_data.storage.base import StorageBackend


logger = get_pipeline_logger("market_data.marketaux.silver")


def _run_local_pipeline(
    input_path: Path,
    output_path: Path,
) -> dict:
    if not input_path.exists():
        logger.warning(
            "Marketaux Bronze input directory does not exist: %s",
            input_path,
        )
        return {
            "status": "skipped",
            "files_processed": 0,
            "articles_processed": 0,
            "files_skipped": 0,
            "failed": [],
        }

    if not input_path.is_dir():
        logger.error(
            "Marketaux Bronze input path is not a directory: %s",
            input_path,
        )
        return {
            "status": "failed",
            "files_processed": 0,
            "articles_processed": 0,
            "files_skipped": 0,
            "failed": [
                {
                    "file": str(input_path),
                    "error": "Input path is not a directory.",
                }
            ],
        }

    bronze_files = sorted(input_path.rglob("*.json"))

    if not bronze_files:
        logger.info("No Marketaux Bronze files found.")
        return {
            "status": "skipped",
            "files_processed": 0,
            "articles_processed": 0,
            "files_skipped": 0,
            "failed": [],
        }

    files_processed = 0
    articles_processed = 0
    files_skipped = 0
    failed = []

    for bronze_file in bronze_files:
        relative_path = bronze_file.relative_to(input_path)
        silver_file = (output_path / relative_path).with_suffix(".pq")

        if silver_file.exists():
            logger.info(
                "Skipping existing Marketaux Silver file: %s",
                silver_file,
            )
            files_skipped += 1
            continue

        try:
            with bronze_file.open("r", encoding="utf-8") as file:
                raw_data = json.load(file)

            transformed_data = transform_news(raw_data)

            if transformed_data.is_empty():
                logger.info(
                    "No article-entity records found in: %s",
                    bronze_file,
                )
                files_skipped += 1
                continue

            save_silver_news(
                transformed_data,
                silver_file,
            )

            files_processed += 1
            articles_processed += transformed_data.height

            logger.info(
                "Processed Marketaux Silver file: %s (%s rows)",
                bronze_file,
                transformed_data.height,
            )

        except Exception as error:
            failure = {
                "file": relative_path.as_posix(),
                "error": str(error),
            }
            failed.append(failure)

            logger.exception(
                "Failed to process Marketaux Bronze file: %s",
                bronze_file,
            )

    if failed:
        status = "partial" if files_processed else "failed"
    elif files_processed:
        status = "success"
    else:
        status = "skipped"

    result = {
        "status": status,
        "files_processed": files_processed,
        "articles_processed": articles_processed,
        "files_skipped": files_skipped,
        "failed": failed,
    }

    logger.info(
        "Marketaux Silver completed: status=%s, processed=%s, "
        "rows=%s, skipped=%s, failed=%s",
        status,
        files_processed,
        articles_processed,
        files_skipped,
        len(failed),
    )

    return result


def _run_storage_pipeline(
    input_prefix: str,
    output_prefix: str,
    storage: StorageBackend,
) -> dict:
    input_prefix = input_prefix.strip("/")
    output_prefix = output_prefix.strip("/")

    bronze_files = sorted(
        key
        for key in storage.list_files(input_prefix)
        if key.lower().endswith(".json")
    )

    if not bronze_files:
        logger.info("No Marketaux Bronze files found in storage.")
        return {
            "status": "skipped",
            "files_processed": 0,
            "articles_processed": 0,
            "files_skipped": 0,
            "failed": [],
        }

    files_processed = 0
    articles_processed = 0
    files_skipped = 0
    failed = []

    for bronze_key in bronze_files:
        relative_path = relpath(
            bronze_key,
            input_prefix,
        ).replace("\\", "/")

        silver_key = join(
            output_prefix,
            relative_path,
        ).rsplit(".", 1)[0] + ".pq"

        try:
            if storage.exists(silver_key):
                logger.info(
                    "Skipping existing Marketaux Silver file: %s",
                    silver_key,
                )
                files_skipped += 1
                continue

            raw_data = storage.read_json(bronze_key)
            transformed_data = transform_news(raw_data)

            if transformed_data.is_empty():
                logger.info(
                    "No article-entity records found in: %s",
                    bronze_key,
                )
                files_skipped += 1
                continue

            saved = save_silver_news(
                transformed_data,
                silver_key,
                storage=storage,
            )

            if not saved:
                files_skipped += 1
                continue

            files_processed += 1
            articles_processed += transformed_data.height

            logger.info(
                "Processed Marketaux Silver file: %s (%s rows)",
                bronze_key,
                transformed_data.height,
            )

        except Exception as error:
            failed.append(
                {
                    "file": relative_path,
                    "error": str(error),
                }
            )

            logger.exception(
                "Failed to process Marketaux Bronze object: %s",
                bronze_key,
            )

    if failed:
        status = "partial" if files_processed else "failed"
    elif files_processed:
        status = "success"
    else:
        status = "skipped"

    result = {
        "status": status,
        "files_processed": files_processed,
        "articles_processed": articles_processed,
        "files_skipped": files_skipped,
        "failed": failed,
    }

    logger.info(
        "Marketaux Silver completed: status=%s, processed=%s, "
        "rows=%s, skipped=%s, failed=%s",
        status,
        files_processed,
        articles_processed,
        files_skipped,
        len(failed),
    )

    return result


def run_marketaux_silver_pipeline(
    input_path: Path | str,
    output_path: Path | str,
    storage: StorageBackend | None = None,
) -> dict:
    if storage is not None:
        return _run_storage_pipeline(
            str(input_path),
            str(output_path),
            storage,
        )

    return _run_local_pipeline(
        Path(input_path),
        Path(output_path),
    )
