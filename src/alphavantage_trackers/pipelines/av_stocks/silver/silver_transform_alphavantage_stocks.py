from __future__ import annotations
from typing import Any

import polars as pl
import pandas_market_calendars as mcal

from alphavantage_trackers.paths import (
    RAW_PATH,
    STOCKS_SILVER_PATH,
    PROJECT_ROOT,
    AV_STOCKS_PATH,
)
from alphavantage_trackers.func import load_json_file
from alphavantage_trackers.logging_config import get_pipeline_logger

logger = get_pipeline_logger("stocks")
nyse = mcal.get_calendar("NYSE")

# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _polars_dtype(dtype: Any) -> pl.DataType:
    """Translate the dtype strings in the existing JSON schemas to Polars."""
    if isinstance(dtype, pl.DataType):
        return dtype

    dtype_name = str(dtype).lower().replace(" ", "")

    dtype_map = {
        "str": pl.String,
        "string": pl.String,
        "object": pl.String,
        "int": pl.Int64,
        "int64": pl.Int64,
        "int32": pl.Int32,
        "float": pl.Float64,
        "float64": pl.Float64,
        "float32": pl.Float32,
        "bool": pl.Boolean,
        "boolean": pl.Boolean,
        "date": pl.Date,
        "datetime": pl.Datetime("ns"),
        "datetime64[ns]": pl.Datetime("ns"),
        "datetime64[us]": pl.Datetime("us"),
    }

    if dtype_name not in dtype_map:
        raise ValueError(f"Unsupported schema dtype: {dtype}")

    return dtype_map[dtype_name]


def _parse_date_column(df: pl.DataFrame, column: str = "date") -> pl.DataFrame:
    """Parse a date column consistently and reject invalid/missing dates."""
    if column not in df.columns:
        raise KeyError(f"Required date column '{column}' is missing.")

    # Casting to string first handles both Parquet Datetime and String inputs.
    parsed = df.with_columns(
        pl.col(column)
        .cast(pl.String)
        .str.to_datetime(strict=False)
        .alias(column)
    )

    invalid_count = parsed.select(pl.col(column).is_null().sum()).item()

    if invalid_count:
        examples = (
            parsed.filter(pl.col(column).is_null())
            .head(10)
            .to_dicts()
        )
        raise ValueError(
            f"{invalid_count} rows have missing or invalid '{column}' values. "
            f"Examples: {examples}"
        )

    return parsed


# ---------------------------------------------------------------------------
# Bronze Parquet reader
# ---------------------------------------------------------------------------

def pq_to_df(asset_name: str) -> pl.DataFrame:
    """Read the newest raw Parquet file for an asset."""
    parquet_files = sorted(
        RAW_PATH.rglob(f"*_{asset_name}_raw.pq")
    )

    if not parquet_files:
        logger.warning(
            f"No raw Parquet files located for {asset_name}."
        )
        return pl.DataFrame()

    latest_file_path = parquet_files[-1]

    logger.info(
        f"Reading newest raw file for {asset_name}: {latest_file_path}"
    )

    try:
        return pl.read_parquet(latest_file_path)
    except Exception:
        logger.exception(
            f"Error reading raw Parquet file for {asset_name}."
        )
        return pl.DataFrame()


# ---------------------------------------------------------------------------
# Raw Alpha Vantage structure flattening
# ---------------------------------------------------------------------------

def _clean_av_field_name(field_name: str) -> str:
    """Convert '1. open' into 'open', etc."""
    return field_name.split(". ", maxsplit=1)[-1].strip().replace(" ", "_")

def flatten_alphavantage_ohlcv(
    asset_name: str,
    df_raw: pl.DataFrame,
    asset_config: dict,
) -> pl.DataFrame:
    """
    Flatten Alpha Vantage raw structures into a conventional OHLCV table.

    Supports:
      - equity_daily: Meta Data + Time Series (Daily) struct columns
      - commodity_gold: data containing a list/struct of observations
    """
    if df_raw.is_empty():
        return pl.DataFrame()

    schema_type = asset_config.get("schema_type")

    try:
        raw_rows = df_raw.to_dicts()
        records: list[dict[str, Any]] = []
            
        if schema_type == "equity_daily":
            required = {"Meta Data", "Time Series (Daily)"}

            if not required.issubset(df_raw.columns):
                logger.error(
                    "Missing expected columns for equity_daily: %s",
                    required - set(df_raw.columns)
                )
                return pl.DataFrame()

            observation_rows = [
                row
                for row in raw_rows
                if isinstance(row.get("Time Series (Daily)"), dict)
            ]

            records = [
                {
                    "date": row["Meta Data"],
                    **{
                        key.split(". ", 1)[-1].strip().replace(" ", "_"): value
                        for key, value in row["Time Series (Daily)"].items()
                    }
                }
                for row in observation_rows
            ]

        elif schema_type == "commodity_gold":
            if "data" in df_raw.columns:
                for row in raw_rows:
                    observations = row.get("data")

                    if observations is None:
                        continue

                    if isinstance(observations, dict):
                        observations = [observations]

                    if isinstance(observations, list):
                        for observation in observations:
                            if isinstance(observation, dict):
                                records.append(dict(observation))
            else:
                records = raw_rows

            # Alpha Vantage commodity responses use 'price'.
            records = [
                {
                    **record,
                    **(
                        {"close": record["price"]}
                        if "price" in record and "close" not in record
                        else {}
                    ),
                }
                for record in records
            ]

            records = [
                {key: value for key, value in record.items() if key != "price"}
                for record in records
            ]

        else:
            logger.warning(
                f"Skipping {asset_name}: unsupported schema_type "
                f"'{schema_type}'."
            )
            return pl.DataFrame()

        if not records:
            logger.warning(
                f"No observations could be extracted for {asset_name}."
            )
            return pl.DataFrame()

        df = pl.DataFrame(records, infer_schema_length=None)

        if "asset_name" not in df.columns:
            df = df.with_columns(
                pl.lit(asset_name).alias("asset_name")
            )

        if "date" in df.columns:
            df = _parse_date_column(df, "date")

        numeric_columns = [
            column
            for column in ["open", "high", "low", "close", "volume"]
            if column in df.columns
        ]

        if numeric_columns:
            df = df.with_columns(
                [
                    pl.col(column)
                    .cast(pl.Float64, strict=False)
                    .alias(column)
                    for column in numeric_columns
                ]
            )

        # Volume should be an integer where possible. Keep it Float64 here
        # until the configured schema firewall performs the final cast.
        logger.info(
            f"Flattened {df.height} rows for {asset_name} "
            f"using '{schema_type}'."
        )
        return df

    except Exception:
        logger.exception(
            f"Failed flattening raw Alpha Vantage data for {asset_name}."
        )
        return pl.DataFrame()


# ---------------------------------------------------------------------------
# Schema firewall
# ---------------------------------------------------------------------------

def df_column_check(df: pl.DataFrame,
    expected_schema: dict,
    ) -> pl.DataFrame:
    """
    Enforce required columns, order, and configured Polars data types.

    Returns an empty DataFrame when validation fails, matching the previous
    pipeline's success/failure convention.
    """
    if df.is_empty() or not expected_schema:
        return pl.DataFrame()

    try:
        missing_columns = [
            column
            for column in expected_schema
            if column not in df.columns
        ]

        if missing_columns:
            logger.error(
                f"Schema validation failed. Missing columns: "
                f"{missing_columns}"
            )
            return pl.DataFrame()

        validated = df.select(list(expected_schema.keys()))

        expressions = []

        for column, configured_dtype in expected_schema.items():
            target_dtype = _polars_dtype(configured_dtype)

            if column == "date":
                expressions.append(
                    pl.col(column)
                    .cast(pl.String)
                    .str.to_datetime(strict=True)
                    .alias(column)
                )
            else:
                expression = pl.col(column)

                # Polars distinguishes NaN from null. Convert NaN to null
                # before casting so missing numeric values are handled
                # explicitly rather than causing integer-cast exceptions.
                if validated.schema[column].is_float():
                    expression = expression.fill_nan(None)

                expressions.append(
                    expression.cast(target_dtype, strict=True).alias(column)
                )

        validated = validated.with_columns(expressions)

        if "date" in validated.columns:
            null_dates = validated.select(
                pl.col("date").is_null().sum()
            ).item()

            if null_dates:
                raise ValueError(
                    f"Schema validation found {null_dates} null dates."
                )

        return validated

    except Exception:
        logger.exception("Schema enforcement failed.")
        return pl.DataFrame()


# ---------------------------------------------------------------------------
# Time attributes
# ---------------------------------------------------------------------------

def derive_time_attributes(
    df: pl.DataFrame,
    date_column: str = "date",
) -> pl.DataFrame:
    """Derive calendar fields from the actual observation date."""
    if df.is_empty():
        return df

    parsed = _parse_date_column(df, date_column)

    return parsed.with_columns(
        pl.col(date_column).dt.year().cast(pl.Int64).alias("year"),
        pl.col(date_column).dt.month().cast(pl.Int64).alias("month"),
        pl.col(date_column).dt.day().cast(pl.Int64).alias("day"),
        # Monday = 0, Sunday = 6, matching pandas dayofweek.
        (pl.col(date_column).dt.weekday() - 1)
        .cast(pl.Int64)
        .alias("day_of_week"),
    )


# ---------------------------------------------------------------------------
# Logical OHLCV integrity firewall
# ---------------------------------------------------------------------------

def df_integrity_check(df: pl.DataFrame) -> pl.DataFrame:
    """Remove rows that violate basic positive-price and OHLC rules."""
    if df.is_empty():
        return df

    required_price_columns = {"open", "high", "low", "close"}

    if not required_price_columns.issubset(df.columns):
        logger.warning(
            "OHLC integrity rules skipped because one or more OHLC "
            "columns are absent."
        )
        return df

    before = df.height

    cleaned = df.filter(
        (pl.col("open") > 0)
        & (pl.col("high") > 0)
        & (pl.col("low") > 0)
        & (pl.col("close") > 0)
        & (pl.col("high") >= pl.col("open"))
        & (pl.col("high") >= pl.col("close"))
        & (pl.col("low") <= pl.col("open"))
        & (pl.col("low") <= pl.col("close"))
    )

    removed = before - cleaned.height

    if removed:
        logger.warning(
            f"OHLC integrity firewall removed {removed} invalid rows "
            f"for {df['asset_name'][0] if 'asset_name' in df.columns else 'asset'}."
        )

    return cleaned

# ---------------------------------------------------------------------------
# Silver Parquet writer
# ---------------------------------------------------------------------------

def save_silver_to_pq(
    asset_name: str,
    df: pl.DataFrame,
) -> None:
    if df.is_empty():
        raise ValueError(
            f"Cannot save Silver data for {asset_name}: DataFrame is empty."
        )

    silver_dir = STOCKS_SILVER_PATH / asset_name
    silver_dir.mkdir(parents=True, exist_ok=True)

    file_path = silver_dir / f"{asset_name}_silver.pq"

    df.write_parquet(file_path)

    logger.info(f"Saved Silver Parquet file to: {file_path}")


# ---------------------------------------------------------------------------
# Silver pipeline
# ---------------------------------------------------------------------------

def run_alphavantage_stocks_silver(
    configs: dict,
    asset_schemas: dict,
    silver_output_schema: dict,
    ) -> bool:
    success = True

    for asset_name, asset_config in configs.items():
        try:
            # 1. Read the latest Bronze file.
            df_raw = pq_to_df(asset_name)

            if df_raw.is_empty():
                logger.warning(f"Skipping {asset_name}: no raw data found.")
                success = False
                continue

            # 2. Flatten the source-specific Alpha Vantage structure.
            df_flattened = flatten_alphavantage_ohlcv(
                asset_name,
                df_raw,
                asset_config,)

            if df_flattened.is_empty():
                logger.warning(f"Skipping {asset_name}: raw flattening returned no data.")
                success = False
                continue

            # 3. Validate the source schema.
            schema_key = asset_config.get("schema_type")
            target_schema = asset_schemas.get(schema_key)

            if not target_schema:
                logger.warning(f"Skipping {asset_name}: schema type "
                                f"'{schema_key}' was not found.")
                success = False
                continue

            df_schema_approved = df_column_check(
                df_flattened,
                expected_schema=target_schema,)

            if df_schema_approved.is_empty():
                logger.warning(f"Skipping {asset_name}: source schema validation failed.")
                success = False
                continue

            # 4. Derive date attributes from the validated observation dates.
            df_enriched = derive_time_attributes(
                df_schema_approved,
                date_column="date",)

            # 5. Apply logical OHLCV integrity checks.
            df_clean = df_integrity_check(df_enriched)

            if df_clean.is_empty():
                logger.warning(f"Skipping {asset_name}: integrity check returned "
                    f"no valid rows.")
                success = False
                continue

            # 6. Enforce the final Silver output schema.
            df_silver_approved = df_column_check(
                df_clean,
                expected_schema=silver_output_schema,
            )

            if df_silver_approved.is_empty():
                logger.warning(
                    f"Skipping {asset_name}: Silver output schema "
                    f"validation failed."
                )
                success = False
                continue

            # 7. Persist Silver output.
            save_silver_to_pq(
                asset_name,
                df_silver_approved,
            )

            logger.info(
                f"Silver transformation successful for {asset_name} "
                f"using '{schema_key}' schema."
            )

        except Exception:
            logger.exception(
                f"Silver transformation failed for {asset_name}."
            )
            success = False

    return success


if __name__ == "__main__":
    config_path = PROJECT_ROOT / "config.json"
    asset_schema_path = PROJECT_ROOT / "alphavantage_asset_schemas.json"
    silver_output_path = (
        AV_STOCKS_PATH
        / "silver"
        / "alphavantage_asset_schemas.json"
    )

    config_data = load_json_file(config_path)
    asset_schema = load_json_file(asset_schema_path)
    silver_output_schema = load_json_file(silver_output_path)

    run_alphavantage_stocks_silver(
        config_data,
        asset_schema,
        silver_output_schema,
    )
