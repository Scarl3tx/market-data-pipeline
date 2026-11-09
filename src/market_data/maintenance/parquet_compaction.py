
import os
import tempfile
from pathlib import Path

import polars as pl

from market_data.logging_config import get_pipeline_logger


logger = get_pipeline_logger("maintenance.parquet_compaction")


def compact_parquet_files(
    source_files: list[Path],
    output_path: Path,
    deduplicate_by: list[str] | None = None,
    sort_by: list[str] | None = None,
    dry_run: bool = True,
    delete_sources: bool = False,
) -> dict:
    """
    Consolidate multiple Parquet files into one verified Parquet file.

    This function is provider-independent.

    Source files are retained unless delete_sources=True.
    Dry-run mode never writes or deletes files.
    """

    output_path = Path(output_path)

    source_files = sorted({
        Path(file).resolve()
        for file in source_files
        if Path(file).is_file()
    })

    output_resolved = output_path.resolve()

    if not source_files:
        return {
            "status": "skipped",
            "files_found": 0,
            "files_removed": 0,
            "rows_before": 0,
            "rows_after": 0,
            "bytes_before": 0,
            "bytes_after": 0,
            "output_path": None,
            "error": None,
        }

    bytes_before = sum(
        file.stat().st_size
        for file in source_files
    )

    if dry_run:
        logger.info(
            "Parquet compaction dry run | Files: %s | Bytes: %s",
            len(source_files),
            bytes_before,
        )

        return {
            "status": "dry_run",
            "files_found": len(source_files),
            "files_removed": 0,
            "rows_before": None,
            "rows_after": None,
            "bytes_before": bytes_before,
            "bytes_after": None,
            "output_path": output_path,
            "error": None,
        }

    temporary_path = None

    try:
        dataframes = [
            pl.read_parquet(file)
            for file in source_files
        ]

        combined = pl.concat(
            dataframes,
            how="vertical",
        )

        rows_before = combined.height

        if deduplicate_by:
            missing_columns = [
                column
                for column in deduplicate_by
                if column not in combined.columns
            ]

            if missing_columns:
                raise ValueError(
                    "Deduplication columns missing from dataset: "
                    f"{missing_columns}"
                )

            combined = combined.unique(
                subset=deduplicate_by,
                keep="last",
                maintain_order=True,
            )

        if sort_by:
            missing_columns = [
                column
                for column in sort_by
                if column not in combined.columns
            ]

            if missing_columns:
                raise ValueError(
                    f"Sort columns missing from dataset: {missing_columns}"
                )

            combined = combined.sort(sort_by)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{output_path.name}.",
            suffix=".tmp.pq",
            dir=output_path.parent,
        )

        os.close(file_descriptor)
        temporary_path = Path(temporary_name)

        combined.write_parquet(temporary_path)

        # Verify the written Parquet file before replacement.
        saved = pl.read_parquet(temporary_path)

        if saved.shape != combined.shape:
            raise ValueError(
                "Compacted Parquet file failed row/column verification."
            )

        if saved.schema != combined.schema:
            raise ValueError(
                "Compacted Parquet file failed schema verification."
            )

        if not saved.equals(combined):
            raise ValueError(
                "Compacted Parquet file failed data verification."
            )

        # Atomic replacement after successful verification.
        temporary_path.replace(output_path)
        temporary_path = None

        bytes_after = output_path.stat().st_size

        files_removed = 0
        deletion_errors = []

        if delete_sources:
            for source_file in source_files:
                # Never delete the output file itself.
                if source_file == output_resolved:
                    continue

                try:
                    source_file.unlink()
                    files_removed += 1

                except OSError as error:
                    deletion_errors.append(
                        {
                            "file": str(source_file),
                            "error": str(error),
                        }
                    )

        status = (
            "partial"
            if deletion_errors
            else "success"
        )

        logger.info(
            "Parquet compaction completed | "
            "Files: %s | Rows: %s -> %s | "
            "Bytes: %s -> %s | Removed: %s",
            len(source_files),
            rows_before,
            saved.height,
            bytes_before,
            bytes_after,
            files_removed,
        )

        return {
            "status": status,
            "files_found": len(source_files),
            "files_removed": files_removed,
            "rows_before": rows_before,
            "rows_after": saved.height,
            "bytes_before": bytes_before,
            "bytes_after": bytes_after,
            "output_path": output_path,
            "deletion_errors": deletion_errors,
            "error": None,
        }

    except Exception as error:
        logger.exception(
            "Parquet compaction failed: %s",
            output_path,
        )

        return {
            "status": "failed",
            "files_found": len(source_files),
            "files_removed": 0,
            "rows_before": None,
            "rows_after": None,
            "bytes_before": bytes_before,
            "bytes_after": None,
            "output_path": None,
            "error": str(error),
        }

    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def compact_parquet_directory(
    directory: Path,
    pattern: str,
    output_path: Path,
    deduplicate_by: list[str] | None = None,
    sort_by: list[str] | None = None,
    recursive: bool = False,
    dry_run: bool = True,
    delete_sources: bool = False,
) -> dict:
    """Find matching Parquet files in a directory and compact them."""

    directory = Path(directory)

    if not directory.exists():
        return {
            "status": "skipped",
            "files_found": 0,
            "files_removed": 0,
            "rows_before": 0,
            "rows_after": 0,
            "bytes_before": 0,
            "bytes_after": 0,
            "output_path": None,
            "error": None,
        }

    if not directory.is_dir():
        raise NotADirectoryError(directory)

    iterator = (
        directory.rglob(pattern)
        if recursive
        else directory.glob(pattern)
    )

    return compact_parquet_files(
        source_files=list(iterator),
        output_path=output_path,
        deduplicate_by=deduplicate_by,
        sort_by=sort_by,
        dry_run=dry_run,
        delete_sources=delete_sources,
    )
