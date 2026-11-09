
import os

import duckdb

from market_data.paths import DATABASE_DIR, DATABASE_PATH


def get_database_connection():
    configured_path = os.getenv("MARKET_DATA_DATABASE_PATH")

    database_path = (
        configured_path
        if configured_path
        else DATABASE_PATH
    )

    database_dir = os.path.dirname(database_path)

    if database_dir:
        os.makedirs(database_dir, exist_ok=True)

    return duckdb.connect(str(database_path))
