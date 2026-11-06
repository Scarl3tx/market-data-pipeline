import json
import sqlite3

from pathlib import Path
from datetime import datetime, timezone

from alphavantage_trackers.paths import API_TRACKER_PATH

class AlphaVantageQueue:

    def __init__(self, db_path=None):
        self.db_path = db_path or API_TRACKER_PATH / "av_queue.sqlite3"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._initialise_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _initialise_db(self):

        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_jobs (
                    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    config TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT
                )
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS unique_active_jobs
                ON api_jobs (asset_type, symbol)
                WHERE status IN ('pending', 'processing')
            """)

    def add_jobs(self, asset_type: str, configs: dict):
        with self._connect() as conn:
            for symbol, config in configs.items():
                cursor = conn.execute("""
                    INSERT OR IGNORE INTO api_jobs (
                        asset_type,
                        symbol,
                        config,
                        created_at
                    )
                    VALUES (?, ?, ?, ?)
                """, (
                    asset_type,
                    symbol,
                    json.dumps(config),
                    datetime.now(timezone.utc).isoformat()
                ))

                if cursor.rowcount == 1:
                    print(f"[QUEUE] Added {asset_type}: {symbol}")
                else:
                    print(f"[QUEUE] Skipped duplicate {asset_type}: {symbol}")

    def get_next_job(self):

        with self._connect() as conn:

            conn.execute("BEGIN IMMEDIATE")

            row = conn.execute("""
                SELECT job_id, asset_type, symbol, config, attempts
                FROM api_jobs
                WHERE status = 'pending'
                ORDER BY job_id ASC
                LIMIT 1
            """).fetchone()

            if row is None:
                return None

            conn.execute("""
                UPDATE api_jobs
                SET status = 'processing',
                    attempts = attempts + 1
                WHERE job_id = ?
            """, (row[0],))

            return {
                "job_id": row[0],
                "asset_type": row[1],
                "symbol": row[2],
                "config": json.loads(row[3]),
                "attempts": row[4] + 1
            }

    def complete_job(self, job_id: int):

        with self._connect() as conn:
            cursor = conn.execute("""
                UPDATE api_jobs
                SET status = 'completed',
                    completed_at = ?
                WHERE job_id = ?
                AND status = 'processing'
            """, (
                datetime.now(timezone.utc).isoformat(),
                job_id
            ))

            return cursor.rowcount == 1

    def fail_job(
            
        self,
        job_id: int,
        error: str,
        max_attempts: int = 3
    ):

        with self._connect() as conn:

            row = conn.execute("""
                SELECT attempts
                FROM api_jobs
                WHERE job_id = ?
                AND status = 'processing'
            """, (job_id,)).fetchone()

            if row is None:
                return False

            attempts = row[0]

            new_status = (
                "failed" if attempts >= max_attempts else "pending"
            )

            conn.execute("""
                UPDATE api_jobs
                SET status = ?,
                    last_error = ?
                WHERE job_id = ?
            """, (
                new_status,
                error,
                job_id
            ))

            return True

    