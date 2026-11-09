import asyncio

from market_data.pipelines import main


def test_unknown_stage_result():
    assert main._get_stage_status(
        [{"unexpected": "value"}]
    ) == "failed"


def test_provider_with_no_recognised_stages_fails():
    status, stages = main._get_provider_status(
        {"metadata": {"status": "success"}}
    )

    assert status == "failed"
    assert stages == {}


def test_all_skipped_stages():
    status, stages = main._get_provider_status(
        {
            "bronze": [],
            "silver": [],
            "gold": [],
        }
    )

    assert status == "skipped"
    assert stages == {
        "bronze": "skipped",
        "silver": "skipped",
        "gold": "skipped",
    }


def test_each_run_has_a_unique_run_id(monkeypatch):
    async def fake_run_provider(name, pipeline, storage):
        return {
            "status": "success",
            "stages": {},
            "result": {},
            "error": None,
        }

    async def fake_refresh_combined_gold(storage):
        return {
            "status": "success",
            "rows": 1,
            "error": None,
        }

    monkeypatch.setattr(main, "get_storage", lambda: object())
    monkeypatch.setattr(main, "_run_provider", fake_run_provider)
    monkeypatch.setattr(
        main,
        "_refresh_combined_gold",
        fake_refresh_combined_gold,
    )
    monkeypatch.setattr(main, "save_pipeline_run", lambda results: None)

    first = asyncio.run(
        main._run_market_data_pipeline_unlocked_async()
    )
    second = asyncio.run(
        main._run_market_data_pipeline_unlocked_async()
    )

    assert first["run_id"]
    assert second["run_id"]
    assert first["run_id"] != second["run_id"]
