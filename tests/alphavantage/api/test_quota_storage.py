
import sqlite3
from contextlib import closing

from market_data.api.alphavantage.quota_storage import QuotaStorage


def test_creates_database_file(tmp_path):
    database_path = tmp_path / "state" / "usage.sqlite3"

    QuotaStorage(database_path)

    assert database_path.exists()


def test_creates_api_requests_table(tmp_path):
    database_path = tmp_path / "usage.sqlite3"

    storage = QuotaStorage(database_path)

    with closing(storage.connect()) as connection:
        result = connection.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            AND name = 'api_requests'
        """).fetchone()

    assert result is not None


def test_can_record_api_request(tmp_path):
    database_path = tmp_path / "usage.sqlite3"

    storage = QuotaStorage(database_path)

    with closing(storage.connect()) as connection:
        connection.execute("""
            INSERT INTO api_requests (requested_at)
            VALUES (?)
        """, (1000.0,))
        connection.commit()

    with closing(storage.connect()) as connection:
        result = connection.execute(
            "SELECT requested_at FROM api_requests"
        ).fetchone()

    assert result == (1000.0,)


def test_initialisation_is_idempotent(tmp_path):
    database_path = tmp_path / "usage.sqlite3"

    QuotaStorage(database_path)
    QuotaStorage(database_path)

    with closing(sqlite3.connect(database_path)) as connection:
        result = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()

    assert ("api_requests",) in result


def test_database_path_accepts_string(tmp_path):
    database_path = tmp_path / "state" / "usage.sqlite3"

    storage = QuotaStorage(str(database_path))

    assert storage.database_path == database_path
    assert database_path.exists()


def test_connection_uses_configured_busy_timeout(tmp_path):
    storage = QuotaStorage(tmp_path / "usage.sqlite3")

    with closing(storage.connect()) as connection:
        timeout = connection.execute(
            "PRAGMA busy_timeout"
        ).fetchone()[0]

    assert timeout == 30000
