
import sqlite3
from contextlib import closing
from pathlib import Path


class QuotaStorage:

    def __init__(self, database_path: Path):
        self.database_path = Path(database_path)

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialise_database()

    def connect(self):
        return sqlite3.connect(
            self.database_path,
            timeout=30,
        )

    def _initialise_database(self):
        with closing(self.connect()) as connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS api_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_at REAL NOT NULL
                )
            """)

            connection.commit()
