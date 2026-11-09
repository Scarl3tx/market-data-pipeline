import json
from pathlib import Path

from datetime import datetime, timezone


BRONZE_PATH = Path("data/bronze/marketaux/news")


def main():
    files = sorted(BRONZE_PATH.rglob("*.json"))

    articles = {}
    page_count = 0
    errors = []

    for file_path in files:
        try:
            with file_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)

            page_count += 1

            response = payload.get("response", {})
            data = response.get("data", [])

            for article in data:
                article_id = (
                    article.get("uuid")
                    or article.get("id")
                )

                published_at = article.get("published_at")

                if not published_at:
                    errors.append(
                        f"Missing publication date: {file_path}"
                    )
                    continue

                key = article_id or (
                    published_at,
                    article.get("title")
                )

                articles[key] = {
                    "published_at": published_at,
                    "title": article.get("title"),
                }

        except Exception as exc:
            errors.append(f"{file_path}: {exc}")

    dates = [
        datetime.fromisoformat(
            article["published_at"].replace("Z", "+00:00")
        )
        for article in articles.values()
    ]

    print(f"Bronze files/pages: {page_count}")
    print(f"Unique articles: {len(articles)}")

    if dates:
        print(
            "Oldest publication:",
            min(dates).isoformat()
        )
        print(
            "Newest publication:",
            max(dates).isoformat()
        )

        print(
            "Actual coverage:",
            (max(dates) - min(dates)).days,
            "days"
        )

    print(f"Errors: {len(errors)}")

    for error in errors:
        print(error)


if __name__ == "__main__":
    main()