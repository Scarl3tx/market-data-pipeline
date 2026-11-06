from src.historical.ingestion.twelvedata_rate_limiter import TwelveDataRateLimiter
from src.historical.paths import TWELVE_DATA_RATE_LIMIT_STATE


limiter = TwelveDataRateLimiter(
    state_path=TWELVE_DATA_RATE_LIMIT_STATE
)

for i in range(3):
    limiter.wait_for_slot()
    print(f"Request slot {i + 1} granted.")