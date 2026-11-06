import sqlite3
from alphavantage_trackers.paths import API_TRACKER_PATH

db_path = API_TRACKER_PATH / "api_usage.sqlite3"

print(f"Database path: {db_path}")
print(f"Database exists: {db_path.exists()}")

with sqlite3.connect(db_path) as conn:
    tables = conn.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
    """).fetchall()

    print("\nTables found:")
    for table in tables:
        print(table[0])

    for (table_name,) in tables:
        print(f"\nContents of {table_name}:")
        rows = conn.execute(
            f'SELECT * FROM "{table_name}" LIMIT 30'
        ).fetchall()

        for row in rows:
            print(row)