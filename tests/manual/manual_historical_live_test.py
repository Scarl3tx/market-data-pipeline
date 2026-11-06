
from src.historical.providers.twelve_data import TwelveDataProvider
from src.historical.ingestion.stocks import (
    ingest_historical_stock_batch
)


def main():
    provider = TwelveDataProvider()

    result = ingest_historical_stock_batch(
        provider=provider,
        symbol="NVDA",
        start_date="2026-10-01",
        end_date="2026-11-03"
    )

    print("\n========== LIVE TEST RESULT ==========")
    print(result)

    assert result["success"], "Historical ingestion failed"
    assert result["has_data"], "No historical data returned"
    assert result["rows"] > 0, "No rows were saved"


if __name__ == "__main__":
    main()
