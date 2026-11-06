from src.historical.transformation.stocks import transform_historical_stocks


if __name__ == "__main__":
    result = transform_historical_stocks(["NVDA"])
    print(f"\nHistorical Silver result: {result}")