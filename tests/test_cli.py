
from market_data import main


def test_cli_success(monkeypatch):
    monkeypatch.setattr(
        main,
        "run_market_data_pipeline",
        lambda: {"status": "success"},
    )

    assert main.main() == 0


def test_cli_skipped(monkeypatch):
    monkeypatch.setattr(
        main,
        "run_market_data_pipeline",
        lambda: {"status": "skipped"},
    )

    assert main.main() == 0


def test_cli_partial_failure(monkeypatch):
    monkeypatch.setattr(
        main,
        "run_market_data_pipeline",
        lambda: {"status": "partial_failure"},
    )

    assert main.main() == 1


def test_cli_failure(monkeypatch):
    monkeypatch.setattr(
        main,
        "run_market_data_pipeline",
        lambda: {"status": "failed"},
    )

    assert main.main() == 2


def test_cli_unexpected_status(monkeypatch):
    monkeypatch.setattr(
        main,
        "run_market_data_pipeline",
        lambda: {"status": "unknown"},
    )

    assert main.main() == 2


def test_cli_exception(monkeypatch):
    def fail():
        raise RuntimeError("Unexpected pipeline error")

    monkeypatch.setattr(
        main,
        "run_market_data_pipeline",
        fail,
    )

    assert main.main() == 2
