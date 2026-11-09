import json
from pathlib import Path


def save_raw_news(data: dict, file_path: Path) -> bool:
    """
    Save the raw Alpha Vantage news response as JSON.

    Returns:
        True if saved.
        False if the file already exists.
    """
    file_path = Path(file_path)

    if file_path.exists():
        return False

    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    return True


def load_raw_news(file_path: Path) -> dict:
    """Load a previously saved raw Alpha Vantage news response."""
    file_path = Path(file_path)

    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)