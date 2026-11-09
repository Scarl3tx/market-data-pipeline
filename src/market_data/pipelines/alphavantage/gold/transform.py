import polars as pl


def transform_news_gold(data: pl.DataFrame) -> pl.DataFrame:
    if data.is_empty():
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.Date,
                "article_count": pl.UInt32,
                "avg_sentiment_score": pl.Float64,
                "bullish_count": pl.UInt32,
                "neutral_count": pl.UInt32,
                "bearish_count": pl.UInt32,
            }
        )

    return (
        data.with_columns(
            pl.col("ticker").str.to_uppercase(),
            pl.col("ticker_sentiment_label")
            .str.to_lowercase()
            .alias("_sentiment_label"),
        )
        .group_by(["ticker", "date"])
        .agg(
            pl.len().cast(pl.UInt32).alias("article_count"),
            pl.col("ticker_sentiment_score")
            .mean()
            .alias("avg_sentiment_score"),
            pl.col("_sentiment_label")
            .is_in(["bullish", "somewhat-bullish"])
            .sum()
            .cast(pl.UInt32)
            .alias("bullish_count"),
            pl.col("_sentiment_label")
            .is_in(["neutral"])
            .sum()
            .cast(pl.UInt32)
            .alias("neutral_count"),
            pl.col("_sentiment_label")
            .is_in(["bearish", "somewhat-bearish"])
            .sum()
            .cast(pl.UInt32)
            .alias("bearish_count"),
        )
        .sort(["ticker", "date"])
    )