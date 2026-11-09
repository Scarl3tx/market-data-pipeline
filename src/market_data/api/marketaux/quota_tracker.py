
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


class MarketauxQuotaTracker:
    def __init__(
        self,
        db_path: Path,
        daily_limit: int = 100,
    ):
        if isinstance(daily_limit, bool) or not isinstance(daily_limit, int):
            raise ValueError("daily_limit must be a positive integer.")

        if daily_limit < 1:
            raise ValueError("daily_limit must be a positive integer.")

        self.db_path = Path(db_path)
        self.daily_limit = daily_limit

        self.db_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialize_database()

    def _connect(self):
        return sqlite3.connect(
            self.db_path,
            timeout=30,
        )

    def _initialize_database(self):
        with closing(self._connect()) as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS api_usage (
                    usage_date TEXT PRIMARY KEY,
                    request_count INTEGER NOT NULL DEFAULT 0
                )
            """)
            connection.commit()

    @staticmethod
    def _today():
        return datetime.now(timezone.utc).date().isoformat()

    def reserve_request(self):
        """
        Reserve one API request before making the HTTP call.

        Returns the number of requests used today.
        Raises RuntimeError if the daily limit is exhausted.
        """
        today = self._today()

        with closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")

                connection.execute("""
                    INSERT OR IGNORE INTO api_usage (
                        usage_date,
                        request_count
                    )
                    VALUES (?, 0)
                """, (today,))

                row = connection.execute("""
                    SELECT request_count
                    FROM api_usage
                    WHERE usage_date = ?
                """, (today,)).fetchone()

                current_count = row[0]

                if current_count >= self.daily_limit:
                    connection.rollback()
                    raise RuntimeError(
                        "Marketaux daily request limit exhausted."
                    )

                new_count = current_count + 1

                connection.execute("""
                    UPDATE api_usage
                    SET request_count = ?
                    WHERE usage_date = ?
                """, (new_count, today))

                connection.commit()

                return new_count

            except Exception:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def get_usage(self):
        today = self._today()

        with closing(self._connect()) as connection:
            row = connection.execute("""
                SELECT request_count
                FROM api_usage
                WHERE usage_date = ?
            """, (today,)).fetchone()

        used = row[0] if row else 0

        return {
            "used": used,
            "limit": self.daily_limit,
            "remaining": max(
                self.daily_limit - used,
                0,
            ),
        }


import boto3
from botocore.exceptions import ClientError


class MarketauxDynamoDBQuotaTracker:
    """Shared daily Marketaux quota tracker backed by DynamoDB."""

    def __init__(
        self,
        table_name: str = "market-data-api-quotas",
        daily_limit: int = 100,
        client=None,
    ):
        if isinstance(daily_limit, bool) or not isinstance(daily_limit, int):
            raise ValueError("daily_limit must be a positive integer.")

        if daily_limit < 1:
            raise ValueError("daily_limit must be a positive integer.")

        if not table_name or not table_name.strip():
            raise ValueError("table_name cannot be empty.")

        self.table_name = table_name.strip()
        self.daily_limit = daily_limit
        self.client = client or boto3.client("dynamodb")

    @staticmethod
    def _today():
        return datetime.now(timezone.utc).date().isoformat()

    def reserve_request(self):
        """
        Atomically reserve one request.

        Returns the updated daily request count.
        Raises RuntimeError when the daily quota is exhausted.
        """
        today = self._today()

        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key={
                    "usage_date": {"S": today},
                },
                UpdateExpression=(
                    "SET request_count = "
                    "if_not_exists(request_count, :zero) + :one"
                ),
                ConditionExpression=(
                    "attribute_not_exists(request_count) "
                    "OR request_count < :limit"
                ),
                ExpressionAttributeValues={
                    ":zero": {"N": "0"},
                    ":one": {"N": "1"},
                    ":limit": {"N": str(self.daily_limit)},
                },
                ReturnValues="UPDATED_NEW",
            )

            return int(
                response["Attributes"]["request_count"]["N"]
            )

        except ClientError as exc:
            error_code = exc.response.get(
                "Error", {}
            ).get("Code")

            if error_code == "ConditionalCheckFailedException":
                raise RuntimeError(
                    "Marketaux daily request limit exhausted."
                ) from exc

            raise

    def get_usage(self):
        today = self._today()

        response = self.client.get_item(
            TableName=self.table_name,
            Key={
                "usage_date": {"S": today},
            },
            ConsistentRead=True,
        )

        item = response.get("Item", {})
        used = int(item.get("request_count", {}).get("N", "0"))

        return {
            "used": used,
            "limit": self.daily_limit,
            "remaining": max(
                self.daily_limit - used,
                0,
            ),
        }
