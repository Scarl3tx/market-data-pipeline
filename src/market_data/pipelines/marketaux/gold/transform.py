import polars as pl


SENTIMENT_THRESHOLD = 0.1

GOLD_SCHEMA = {
    "ticker": pl.String,
    "date": pl.Date,
    "article_count": pl.UInt32,
    "avg_sentiment_score": pl.Float64,
    "avg_match_score": pl.Float64,
    "bullish_count": pl.UInt32,
    "neutral_count": pl.UInt32,
    "bearish_count": pl.UInt32,
}


def transform_news_gold(data: pl.DataFrame) -> pl.DataFrame:
    if data.is_empty():
        return pl.DataFrame(schema=GOLD_SCHEMA)

    return (
        data.with_columns(
            pl.col("ticker").str.strip_chars().str.to_uppercase(),
            pl.col("publication_date").alias("date"),
        )
        .group_by(["ticker", "date"])
        .agg(
            pl.len()
            .cast(pl.UInt32)
            .alias("article_count"),

            pl.col("sentiment_score")
            .mean()
            .cast(pl.Float64)
            .alias("avg_sentiment_score"),

            pl.col("match_score")
            .mean()
            .cast(pl.Float64)
            .alias("avg_match_score"),

            (pl.col("sentiment_score") > SENTIMENT_THRESHOLD)
            .sum()
            .cast(pl.UInt32)
            .alias("bullish_count"),

            (
                pl.col("sentiment_score").is_between(
                    -SENTIMENT_THRESHOLD,
                    SENTIMENT_THRESHOLD,
                    closed="both",
                )
            )
            .sum()
            .cast(pl.UInt32)
            .alias("neutral_count"),

            (pl.col("sentiment_score") < -SENTIMENT_THRESHOLD)
            .sum()
            .cast(pl.UInt32)
            .alias("bearish_count"),
        )
        .sort(["ticker", "date"])
    )