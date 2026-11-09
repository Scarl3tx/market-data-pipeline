
import pytest

from market_data.api.marketaux.exceptions import (
    MarketauxQuotaExceededError,
)
from market_data.pipelines.marketaux.bronze import downloader


class FakeClient:
    articles_per_request = 3

    def __init__(self, responses=None, errors=None):
        self.responses = responses or {}
        self.errors = errors or {}
        self.calls = []

    def get_news(self, symbols, page=1, **kwargs):
        symbol = symbols[0]

        self.calls.append({
            "symbol": symbol,
            "page": page,
            **kwargs,
        })

        if symbol in self.errors:
            raise self.errors[symbol]

        return self.responses.get(
            (symbol, page),
            {
                "meta": {
                    "returned": 0,
                    "found": 0,
                },
                "data": [],
            },
        )



class FakeState:
    def __init__(self):
        self.stocks = {}
        self.attempts = []
        self.saved_pages = []
        self.completed = []

    def initialize_stocks(self, stocks):
        for stock in stocks:
            symbol = stock["symbol"].upper()

            self.stocks.setdefault(
                symbol,
                {
                    "name": stock["name"],
                    "last_attempt_at": None,
                    "last_successful_check": None,
                    "oldest_filled_at": None,
                    "pending_since": None,
                    "pending_until": None,
                    "next_page": None,
                    "ingestion_in_progress": False,
                    "ingestion_type": None,
                },
            )

    def get_stocks_by_priority(self):
        return sorted(
            self.stocks.items(),
            key=lambda item: (
                not item[1]["ingestion_in_progress"],
                item[1]["oldest_filled_at"] is not None,
                item[1]["oldest_filled_at"] or "",
                item[1]["last_attempt_at"] or "",
                item[0],
            ),
        )

    def get_stock(self, symbol):
        return self.stocks[symbol].copy()

    def start_ingestion(
        self,
        symbol,
        initial_lookback_days=30,
        overlap_hours=24,
    ):
        stock = self.stocks[symbol]

        if stock["ingestion_in_progress"]:
            return

        stock["pending_since"] = (
            stock["last_successful_check"]
            or "2026-10-07T00:00:00+00:00"
        )
        stock["pending_until"] = (
            "2026-11-06T00:00:00+00:00"
        )
        stock["next_page"] = 1
        stock["ingestion_in_progress"] = True
        stock["ingestion_type"] = "recent"

    def start_backfill(self, symbol, window_days=30):
        stock = self.stocks[symbol]

        if stock["ingestion_in_progress"]:
            return

        if stock["oldest_filled_at"] is None:
            raise ValueError(
                f"No historical checkpoint for {symbol}"
            )

        from datetime import datetime, timedelta

        oldest = datetime.fromisoformat(
            stock["oldest_filled_at"]
        )

        stock["pending_until"] = oldest.isoformat()
        stock["pending_since"] = (
            oldest - timedelta(days=window_days)
        ).isoformat()
        stock["next_page"] = 1
        stock["ingestion_in_progress"] = True
        stock["ingestion_type"] = "backfill"

    def record_attempt(self, symbol):
        self.attempts.append(symbol)
        self.stocks[symbol]["last_attempt_at"] = (
            f"2026-11-06T10:00:{len(self.attempts):02d}+00:00"
        )

    def record_page_saved(self, symbol, page):
        self.saved_pages.append((symbol, page))
        self.stocks[symbol]["next_page"] = page + 1

    def complete_ingestion(self, symbol):
        self.completed.append(symbol)

        stock = self.stocks[symbol]
        ingestion_type = stock["ingestion_type"]

        if ingestion_type == "recent":
            stock["last_successful_check"] = (
                stock["pending_until"]
            )

        pending_since = stock["pending_since"]

        if stock["oldest_filled_at"] is None:
            stock["oldest_filled_at"] = pending_since
        elif pending_since < stock["oldest_filled_at"]:
            stock["oldest_filled_at"] = pending_since

        stock["pending_since"] = None
        stock["pending_until"] = None
        stock["next_page"] = None
        stock["ingestion_in_progress"] = False
        stock["ingestion_type"] = None

    def get_backfill_candidates(self):
        return [
            (symbol, stock.copy())
            for symbol, stock in self.get_stocks_by_priority()
            if (
                stock["oldest_filled_at"] is not None
                or (
                    stock["ingestion_in_progress"]
                    and stock["ingestion_type"] == "backfill"
                )
            )
        ]

@pytest.fixture
def stocks():
    return [
        {"symbol": "NVDA", "name": "NVIDIA"},
        {"symbol": "AAPL", "name": "Apple"},
    ]


def install_storage(monkeypatch, tmp_path):
    saved = []

    def fake_save(**kwargs):
        saved.append(kwargs)
        return tmp_path / (
            f"{kwargs['symbols'][0]}_{kwargs['page']}.json"
        )

    monkeypatch.setattr(
        downloader,
        "save_raw_news",
        fake_save,
    )

    return saved


def test_processes_enabled_stocks(monkeypatch, stocks, tmp_path):
    client = FakeClient()
    state = FakeState()
    saved = install_storage(monkeypatch, tmp_path)

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=stocks,
    )

    assert result["status"] == "success"
    assert len(result["successful"]) == 2
    assert len(client.calls) == 2
    assert len(saved) == 2
    assert state.completed == ["NVDA", "AAPL"]


def test_records_attempt_before_success(monkeypatch, tmp_path):
    client = FakeClient()
    state = FakeState()
    install_storage(monkeypatch, tmp_path)

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=[{"symbol": "NVDA", "name": "NVIDIA"}],
    )

    assert state.attempts == ["NVDA"]
    assert state.completed == ["NVDA"]
    assert result["status"] == "success"


def test_does_not_complete_when_storage_fails(
    monkeypatch, tmp_path
):
    client = FakeClient()
    state = FakeState()

    def fail_storage(**kwargs):
        raise OSError("Disk unavailable")

    monkeypatch.setattr(
        downloader,
        "save_raw_news",
        fail_storage,
    )

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=[{"symbol": "NVDA", "name": "NVIDIA"}],
    )

    assert result["status"] == "partial"
    assert state.attempts == ["NVDA"]
    assert state.saved_pages == []
    assert state.completed == []
    assert state.stocks["NVDA"]["next_page"] == 1
    assert state.stocks["NVDA"]["ingestion_in_progress"] is True


def test_continues_after_individual_stock_failure(
    monkeypatch, stocks, tmp_path
):
    client = FakeClient(
        errors={"NVDA": ValueError("Request failed")}
    )
    state = FakeState()
    install_storage(monkeypatch, tmp_path)

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=stocks,
    )

    assert result["status"] == "partial"
    assert [item["symbol"] for item in result["successful"]] == [
        "AAPL"
    ]
    assert [item["symbol"] for item in result["failed"]] == [
        "NVDA"
    ]


def test_skips_disabled_stocks(monkeypatch, tmp_path):
    client = FakeClient()
    state = FakeState()
    install_storage(monkeypatch, tmp_path)

    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=[
            {"symbol": "AAPL", "name": "Apple"}
        ],
    )

    assert [call["symbol"] for call in client.calls] == ["AAPL"]
    assert [item["symbol"] for item in result["successful"]] == [
        "AAPL"
    ]


def test_resumes_from_next_page(monkeypatch, tmp_path):
    client = FakeClient(
        responses={
            ("NVDA", 2): {
                "meta": {"returned": 1, "found": 4},
                "data": [{"uuid": "article-4"}],
            }
        }
    )
    state = FakeState()

    state.initialize_stocks([
        {"symbol": "NVDA", "name": "NVIDIA"}
    ])

    state.stocks["NVDA"]["ingestion_in_progress"] = True
    state.stocks["NVDA"]["ingestion_type"] = "recent"
    state.stocks["NVDA"]["pending_since"] = (
        "2026-10-07T00:00:00+00:00"
    )
    state.stocks["NVDA"]["pending_until"] = (
        "2026-11-06T00:00:00+00:00"
    )
    state.stocks["NVDA"]["next_page"] = 2

    install_storage(monkeypatch, tmp_path)

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=[{"symbol": "NVDA", "name": "NVIDIA"}],
    )

    assert client.calls[0]["page"] == 2
    assert result["status"] == "success"
    assert state.completed == ["NVDA"]


def test_stops_when_quota_is_exhausted(stocks):
    client = FakeClient(
        errors={
            "NVDA": MarketauxQuotaExceededError(
                "Daily quota exhausted"
            )
        }
    )
    state = FakeState()

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=stocks,
    )

    assert result["status"] == "partial"
    assert len(client.calls) == 1
    assert result["successful"] == []
    assert state.completed == []
    assert state.stocks["NVDA"]["ingestion_in_progress"] is True


def test_saves_multiple_pages(monkeypatch, tmp_path):
    client = FakeClient(
        responses={
            ("NVDA", 1): {
                "meta": {"returned": 3, "found": 4},
                "data": [
                    {"uuid": "article-1"},
                    {"uuid": "article-2"},
                    {"uuid": "article-3"},
                ],
            },
            ("NVDA", 2): {
                "meta": {"returned": 1, "found": 4},
                "data": [{"uuid": "article-4"}],
            },
        }
    )
    state = FakeState()
    saved = install_storage(monkeypatch, tmp_path)

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=[{"symbol": "NVDA", "name": "NVIDIA"}],
    )

    assert [call["page"] for call in client.calls] == [1, 2]
    assert [page for _, page in state.saved_pages] == [1, 2]
    assert len(saved) == 2
    assert result["status"] == "success"
    assert state.completed == ["NVDA"]


def test_processes_one_page_per_stock_per_round(
    monkeypatch, tmp_path
):
    client = FakeClient(
        responses={
            ("NVDA", 1): {
                "meta": {"returned": 3, "found": 6},
                "data": [{}, {}, {}],
            },
            ("AAPL", 1): {
                "meta": {"returned": 3, "found": 3},
                "data": [{}, {}, {}],
            },
            ("NVDA", 2): {
                "meta": {"returned": 3, "found": 6},
                "data": [{}, {}, {}],
            },
        }
    )
    state = FakeState()
    install_storage(monkeypatch, tmp_path)

    result = downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=[
            {"symbol": "NVDA", "name": "NVIDIA"},
            {"symbol": "AAPL", "name": "Apple"},
        ],
    )

    assert [
        (call["symbol"], call["page"])
        for call in client.calls
    ] == [
        ("NVDA", 1),
        ("AAPL", 1),
        ("NVDA", 2),
    ]

    assert result["status"] == "success"
    assert state.completed == ["AAPL", "NVDA"]


def test_passes_fixed_date_window_to_client(
    monkeypatch, tmp_path
):
    client = FakeClient()
    state = FakeState()
    install_storage(monkeypatch, tmp_path)

    downloader.run_marketaux_bronze(
        client=client,
        state=state,
        stocks=[{"symbol": "NVDA", "name": "NVIDIA"}],
    )

    assert client.calls[0]["published_after"] == (
        "2026-10-07T00:00:00"
    )
    assert client.calls[0]["published_before"] == (
        "2026-11-06T00:00:00"
    )

def test_checkpoint_read_failure_does_not_mask_original_error(
    monkeypatch,
    tmp_path,
):
    state = FakeState()
    original_get_stock = state.get_stock

    remaining_failures = 0

    def failing_get_stock(symbol):
        nonlocal remaining_failures

        if remaining_failures > 0:
            remaining_failures -= 1
            raise OSError("Checkpoint unavailable")

        return original_get_stock(symbol)

    state.get_stock = failing_get_stock

    class FailingClient(FakeClient):
        def get_news(self, symbols, page=1, **kwargs):
            nonlocal remaining_failures
            remaining_failures = 2
            raise ValueError("Original API failure")

    install_storage(monkeypatch, tmp_path)

    result = downloader.run_marketaux_bronze(
        client=FailingClient(),
        state=state,
        stocks=[{"symbol": "NVDA", "name": "NVIDIA"}],
    )

    assert result["status"] == "partial"
    assert len(result["failed"]) == 1
    assert result["failed"][0]["symbol"] == "NVDA"
    assert result["failed"][0]["error"] == "Original API failure"
    assert result["failed"][0]["checkpoint_resumable"] is False
    assert result["failed"][0]["retry_next_run"] is False