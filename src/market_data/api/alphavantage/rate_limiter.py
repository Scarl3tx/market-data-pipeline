import time

from contextlib import closing

from market_data.api.alphavantage.quota_storage import QuotaStorage
from market_data.api.alphavantage.exceptions import (
    AlphaVantageQuotaExceededError,
)


class AlphaVantageRateLimiter:

    def __init__(
        self,
        storage: QuotaStorage,
        max_requests: int = 25,
        window_seconds: int = 86400,
        min_interval: float = 1.5,
    ):
        self.storage = storage
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.min_interval = min_interval

    def acquire(self):

        while True:

            wait_duration = 0

            with closing(self.storage.connect()) as connection:

                connection.execute("BEGIN IMMEDIATE")

                now = time.time()
                cutoff = now - self.window_seconds

                connection.execute(
                    """
                    DELETE FROM api_requests
                    WHERE requested_at <= ?
                    """,
                    (cutoff,),
                )

                requests = connection.execute(
                    """
                    SELECT requested_at
                    FROM api_requests
                    WHERE requested_at > ?
                    ORDER BY requested_at ASC
                    """,
                    (cutoff,),
                ).fetchall()

                if len(requests) >= self.max_requests:

                    retry_after = (
                        requests[0][0] + self.window_seconds - now
                    )

                    connection.commit()

                    raise AlphaVantageQuotaExceededError(
                        retry_after=retry_after
                    )

                if requests:

                    wait_until = requests[-1][0] + self.min_interval

                    if wait_until > now:
                        wait_duration = wait_until - now

                if wait_duration == 0:

                    connection.execute(
                        """
                        INSERT INTO api_requests (requested_at)
                        VALUES (?)
                        """,
                        (now,),
                    )

                    connection.commit()
                    return

                connection.commit()

            if wait_duration > 0:
                time.sleep(wait_duration)