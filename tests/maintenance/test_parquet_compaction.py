from pathlib import Path
from datetime import date

import polars as pl

from market_data.maintenance.parquet_compaction import (
    compact_parquet_files,
    compact_parquet_directory,
)


def _write_batch(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        rows,
        schema={
            "symbol": pl.String,
            "date": pl.Date,
            "close": pl.Float64,
        },
    ).write_parquet(path)


def _row(day, close):
    return {
        "symbol": "NVDA",
        "date": date(2026, 1, day),
        "close": close,
    }


def test_dry_run_does_not_modify_files(tmp_path):
    source = tmp_path / "batch_1.pq"
    output = tmp_path / "combined.pq"

    _write_batch(source, [_row(1, 100.0)])

    result = compact_parquet_files(
        source_files=[source],
        output_path=output,
        dry_run=True,
        delete_sources=True,
    )

    assert result["status"] == "dry_run"
    assert result["files_found"] == 1
    assert result["files_removed"] == 0
    assert source.exists()
    assert not output.exists()


def test_compaction_deduplicates_and_sorts(tmp_path):
    source_1 = tmp_path / "batch_1.pq"
    source_2 = tmp_path / "batch_2.pq"
    output = tmp_path / "combined.pq"

    _write_batch(
        source_1,
        [
            _row(2, 110.0),
            _row(1, 100.0),
        ],
    )

    _write_batch(
        source_2,
        [
            _row(2, 115.0),
            _row(3, 120.0),
        ],
    )

    result = compact_parquet_files(
        source_files=[source_1, source_2],
        output_path=output,
        deduplicate_by=["symbol", "date"],
        sort_by=["date"],
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["rows_before"] == 4
    assert result["rows_after"] == 3

    saved = pl.read_parquet(output)

    assert saved["date"].is_sorted()
    assert saved.height == 3
    assert saved.filter(
        pl.col("date") == date(2026, 1, 2)
    )["close"].item() == 115.0

    assert source_1.exists()
    assert source_2.exists()


def test_compaction_deletes_sources_only_after_success(tmp_path):
    source_1 = tmp_path / "batch_1.pq"
    source_2 = tmp_path / "batch_2.pq"
    output = tmp_path / "combined.pq"

    _write_batch(source_1, [_row(1, 100.0)])
    _write_batch(source_2, [_row(1, 100.0)])

    result = compact_parquet_files(
        source_files=[source_1, source_2],
        output_path=output,
        deduplicate_by=["symbol", "date"],
        sort_by=["date"],
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "success"
    assert result["files_removed"] == 2
    assert not source_1.exists()
    assert not source_2.exists()
    assert output.exists()
    assert pl.read_parquet(output).height == 1


def test_schema_mismatch_preserves_source_files(tmp_path):
    source_1 = tmp_path / "batch_1.pq"
    source_2 = tmp_path / "batch_2.pq"
    output = tmp_path / "combined.pq"

    _write_batch(source_1, [_row(1, 100.0)])

    pl.DataFrame(
        {
            "symbol": ["NVDA"],
            "date": ["2026-01-02"],
        }
    ).write_parquet(source_2)

    result = compact_parquet_files(
        source_files=[source_1, source_2],
        output_path=output,
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "failed"
    assert source_1.exists()
    assert source_2.exists()
    assert not output.exists()


def test_empty_directory_is_skipped(tmp_path):
    result = compact_parquet_directory(
        directory=tmp_path,
        pattern="*.pq",
        output_path=tmp_path / "combined.pq",
        dry_run=False,
    )

    assert result["status"] == "skipped"
    assert result["files_found"] == 0


def test_directory_pattern_finds_matching_files(tmp_path):
    source = tmp_path / "NVDA" / "batch_1.pq"
    output = tmp_path / "NVDA" / "combined.pq"

    _write_batch(source, [_row(1, 100.0)])

    result = compact_parquet_directory(
        directory=source.parent,
        pattern="*_1.pq",
        output_path=output,
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["files_found"] == 1


def test_corrupt_source_preserves_all_files(tmp_path):
    source_1 = tmp_path / "batch_1.pq"
    source_2 = tmp_path / "batch_2.pq"
    output = tmp_path / "combined.pq"

    _write_batch(source_1, [_row(1, 100.0)])
    source_2.write_bytes(b"not a valid parquet file")

    result = compact_parquet_files(
        source_files=[source_1, source_2],
        output_path=output,
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "failed"
    assert source_1.exists()
    assert source_2.exists()
    assert not output.exists()


def test_missing_deduplication_column_preserves_sources(tmp_path):
    source_1 = tmp_path / "batch_1.pq"
    source_2 = tmp_path / "batch_2.pq"
    output = tmp_path / "combined.pq"

    _write_batch(source_1, [_row(1, 100.0)])
    _write_batch(source_2, [_row(2, 110.0)])

    result = compact_parquet_files(
        source_files=[source_1, source_2],
        output_path=output,
        deduplicate_by=["missing_column"],
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "failed"
    assert "missing_column" in result["error"]
    assert source_1.exists()
    assert source_2.exists()
    assert not output.exists()


def test_failed_output_verification_preserves_sources(
    tmp_path,
    monkeypatch,
):
    source_1 = tmp_path / "batch_1.pq"
    source_2 = tmp_path / "batch_2.pq"
    output = tmp_path / "combined.pq"

    _write_batch(source_1, [_row(1, 100.0)])
    _write_batch(source_2, [_row(2, 110.0)])

    original_read_parquet = pl.read_parquet

    def corrupted_verification_read(path, *args, **kwargs):
        result = original_read_parquet(path, *args, **kwargs)

        if Path(path).name.startswith(".combined.pq."):
            return result.with_columns(
                (pl.col("close") + 1).alias("close")
            )

        return result

    monkeypatch.setattr(
        pl,
        "read_parquet",
        corrupted_verification_read,
    )

    result = compact_parquet_files(
        source_files=[source_1, source_2],
        output_path=output,
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "failed"
    assert "data verification" in result["error"]
    assert source_1.exists()
    assert source_2.exists()
    assert not output.exists()


def test_source_deletion_failure_returns_partial(
    tmp_path,
    monkeypatch,
):
    source_1 = tmp_path / "batch_1.pq"
    source_2 = tmp_path / "batch_2.pq"
    output = tmp_path / "combined.pq"

    _write_batch(source_1, [_row(1, 100.0)])
    _write_batch(source_2, [_row(2, 110.0)])

    original_unlink = Path.unlink

    def fail_first_source_deletion(path, *args, **kwargs):
        if path == source_1:
            raise OSError("Simulated deletion failure")

        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(
        Path,
        "unlink",
        fail_first_source_deletion,
    )

    result = compact_parquet_files(
        source_files=[source_1, source_2],
        output_path=output,
        dry_run=False,
        delete_sources=True,
    )

    assert result["status"] == "partial"
    assert result["files_removed"] == 1
    assert source_1.exists()
    assert not source_2.exists()
    assert output.exists()
    assert pl.read_parquet(output).height == 2
    assert len(result["deletion_errors"]) == 1
