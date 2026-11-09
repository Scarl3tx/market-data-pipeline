from market_data.api.twelvedata.exceptions import (
    TwelveDataDailyLimitError,
)

from market_data.pipelines.twelvedata import main as pipeline
def test_pipeline_passes_same_storage_to_all_stages(monkeypatch):
    from market_data.storage.local import LocalStorage
    from pathlib import Path
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as temp_dir:
        storage = LocalStorage(Path(temp_dir))
        received = {}

        def fake_bronze(**kwargs):
            received["bronze"] = kwargs["storage"]
            return [{"success": True}]

        def fake_silver(**kwargs):
            received["silver"] = kwargs["storage"]
            return [{"success": True}]

        def fake_gold(**kwargs):
            received["gold"] = kwargs["storage"]
            return [{"success": True}]

        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_bronze",
            fake_bronze,
        )
        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_silver",
            fake_silver,
        )
        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_gold",
            fake_gold,
        )

        result = pipeline.run_twelvedata_pipeline(storage=storage)

        assert received == {
            "bronze": storage,
            "silver": storage,
            "gold": storage,
        }

        assert result["stage_errors"] == {}
        assert result["daily_limit_reached"] is False


def test_storage_is_passed_to_downstream_stages_after_bronze_failure(
    monkeypatch,
):
    from market_data.storage.local import LocalStorage
    from pathlib import Path
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as temp_dir:
        storage = LocalStorage(Path(temp_dir))
        received = []

        def fake_bronze(**kwargs):
            raise RuntimeError("Bronze unavailable")

        def fake_silver(**kwargs):
            received.append(("silver", kwargs["storage"]))
            return []

        def fake_gold(**kwargs):
            received.append(("gold", kwargs["storage"]))
            return []

        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_bronze",
            fake_bronze,
        )
        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_silver",
            fake_silver,
        )
        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_gold",
            fake_gold,
        )

        result = pipeline.run_twelvedata_pipeline(storage=storage)

        assert result["stage_errors"]["bronze"] == "Bronze unavailable"
        assert received == [
            ("silver", storage),
            ("gold", storage),
        ]


def test_storage_is_passed_to_downstream_stages_after_daily_limit(
    monkeypatch,
):
    from market_data.storage.local import LocalStorage
    from pathlib import Path
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as temp_dir:
        storage = LocalStorage(Path(temp_dir))
        received = []

        def fake_bronze(**kwargs):
            raise TwelveDataDailyLimitError("Daily limit reached")

        def fake_silver(**kwargs):
            received.append(("silver", kwargs["storage"]))
            return []

        def fake_gold(**kwargs):
            received.append(("gold", kwargs["storage"]))
            return []

        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_bronze",
            fake_bronze,
        )
        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_silver",
            fake_silver,
        )
        monkeypatch.setattr(
            pipeline,
            "run_twelvedata_gold",
            fake_gold,
        )

        result = pipeline.run_twelvedata_pipeline(storage=storage)

        assert result["daily_limit_reached"] is True
        assert received == [
            ("silver", storage),
            ("gold", storage),
        ]
