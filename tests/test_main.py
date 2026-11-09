import market_data.pipelines.main as main

def test_provider_with_stage_errors_and_successful_stages_is_partial():
    status, stages = main._get_provider_status(
        {
            "bronze": [],
            "silver": [{"symbol": "NVDA", "success": True}],
            "gold": [{"symbol": "NVDA", "success": True}],
            "daily_limit_reached": True,
            "stage_errors": {
                "bronze": "Daily limit reached",
            },
        }
    )

    assert status == "partial_failure"
    assert stages["bronze"] == "skipped"
    assert stages["silver"] == "success"
    assert stages["gold"] == "success"


def test_provider_with_stage_errors_and_no_successful_stages_fails():
    status, stages = main._get_provider_status(
        {
            "bronze": [],
            "silver": [],
            "gold": [],
            "daily_limit_reached": True,
            "stage_errors": {
                "bronze": "Daily limit reached",
            },
        }
    )

    assert status == "failed"
    assert set(stages.values()) == {"skipped"}


def test_provider_without_stage_errors_remains_successful():
    status, stages = main._get_provider_status(
        {
            "bronze": [{"symbol": "NVDA", "success": True}],
            "silver": [{"symbol": "NVDA", "success": True}],
            "gold": [{"symbol": "NVDA", "success": True}],
            "daily_limit_reached": False,
            "stage_errors": {},
        }
    )

    assert status == "success"
