import duckdb

import market_data.database.connection as database_connection


def test_connection_creates_database_directory(tmp_path, monkeypatch):
    database_dir = tmp_path / "database"
    database_path = database_dir / "market_data.duckdb"

    monkeypatch.setattr(
        database_connection,
        "DATABASE_DIR",
        database_dir,
    )
    monkeypatch.setattr(
        database_connection,
        "DATABASE_PATH",
        database_path,
    )

    assert not database_dir.exists()

    connection = database_connection.get_database_connection()

    try:
        assert database_dir.exists()
        assert database_path.exists()
        assert connection.execute("SELECT 1").fetchone() == (1,)
    finally:
        connection.close()


def test_connection_uses_configured_database_path(tmp_path, monkeypatch):
    database_dir = tmp_path / "custom_database"
    database_path = database_dir / "custom.duckdb"

    monkeypatch.setattr(
        database_connection,
        "DATABASE_DIR",
        database_dir,
    )
    monkeypatch.setattr(
        database_connection,
        "DATABASE_PATH",
        database_path,
    )

    connection = database_connection.get_database_connection()

    try:
        connection.execute("""
            CREATE TABLE connection_test (
                id INTEGER PRIMARY KEY,
                value VARCHAR
            )
        """)
        connection.execute("""
            INSERT INTO connection_test VALUES (1, 'persisted')
        """)
    finally:
        connection.close()

    assert database_path.exists()


def test_separate_connections_access_same_database(tmp_path, monkeypatch):
    database_dir = tmp_path / "database"
    database_path = database_dir / "market_data.duckdb"

    monkeypatch.setattr(
        database_connection,
        "DATABASE_DIR",
        database_dir,
    )
    monkeypatch.setattr(
        database_connection,
        "DATABASE_PATH",
        database_path,
    )

    first_connection = database_connection.get_database_connection()

    try:
        first_connection.execute("""
            CREATE TABLE shared_test (
                id INTEGER PRIMARY KEY,
                value VARCHAR
            )
        """)
        first_connection.execute("""
            INSERT INTO shared_test VALUES (1, 'shared')
        """)
    finally:
        first_connection.close()

    second_connection = database_connection.get_database_connection()

    try:
        result = second_connection.execute(
            "SELECT value FROM shared_test WHERE id = 1"
        ).fetchone()

        assert result == ("shared",)
    finally:
        second_connection.close()


def test_connection_returns_duckdb_connection(tmp_path, monkeypatch):
    database_dir = tmp_path / "database"
    database_path = database_dir / "market_data.duckdb"

    monkeypatch.setattr(
        database_connection,
        "DATABASE_DIR",
        database_dir,
    )
    monkeypatch.setattr(
        database_connection,
        "DATABASE_PATH",
        database_path,
    )

    connection = database_connection.get_database_connection()

    try:
        assert isinstance(connection, duckdb.DuckDBPyConnection)
    finally:
        connection.close()