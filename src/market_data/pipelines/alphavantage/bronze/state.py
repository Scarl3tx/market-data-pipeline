import json
from datetime import datetime, timedelta
from pathlib import Path


def load_news_cursor(state_path: Path) -> str | None:
    """Load the next Alpha Vantage news retrieval timestamp."""

    state_path = Path(state_path)

    if not state_path.exists():
        return None

    with open(state_path, "r", encoding="utf-8") as file:
        state = json.load(file)

    cursor = state.get("next_time_from")

    if cursor is None:
        return None

    datetime.strptime(cursor, "%Y%m%dT%H%M")

    return cursor


def save_news_cursor(
    latest_published: str,
    state_path: Path,
) -> str:
    """
    Save the next retrieval timestamp based on the latest
    successfully ingested article publication timestamp.
    """

    published = datetime.strptime(
        latest_published,
        "%Y%m%dT%H%M%S",
    )

    next_time = (
        published.replace(second=0, microsecond=0)
        + timedelta(minutes=1)
    )

    cursor = next_time.strftime("%Y%m%dT%H%M")

    state_path = Path(state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    with open(state_path, "w", encoding="utf-8") as file:
        json.dump(
            {"next_time_from": cursor},
            file,
            indent=4,
        )

    return cursor