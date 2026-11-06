
import json

import pytest

import src.alphavantage_trackers.pipelines.av_commodities.av_commodities_pipeline as commodities_pipeline


@pytest.fixture
def pipeline_files(tmp_path, monkeypatch):
    config_path = tmp_path / "configs"
    output_path = tmp_path / "commodities"

    config_path.mkdir()
    (output_path / "silver").mkdir(parents=True)

    monkeypatch.setattr(
        commodities_pipeline,
        "CONFIG_PATH",
        config_path
    )

    monkeypatch.setattr(
        commodities_pipeline,
        "AV_COMMODITIES_PATH",
        output_path
    )

    return {
        "config": config_path / "alphavantage_commodities_config.json",
        "schemas": config_path / "alphavantage_commodities_schemas.json",
        "output_schema": (
            output_path
            / "silver"
            / "silver_alphavantage_commodities_output_schema.json"
        )
    }


@pytest.fixture
def valid_pipeline_configs():
    return {
        "commodities": {
            "XAU": {
                "function": "GOLD_SILVER_SPOT",
                "schema_type": "commodity_gold"
            }
        }
    }


@pytest.fixture
def valid_schemas():
    return {
        "commodity_gold": {
            "asset_name": "str",
            "date": "datetime64[ns]",
            "close": "float64"
        }
    }


@pytest.fixture
def valid_output_schema():
    return {
        "asset_name": "str",
        "date": "datetime64[ns]",
        "year": "int64",
        "month": "int64",
        "day": "int64",
        "day_of_week": "int64",
        "close": "float64"
    }


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file)


# -------------------------------------------------------------------
# Successful execution
# -------------------------------------------------------------------

def test_run_commodities_pipeline_success(
    monkeypatch,
    pipeline_files,
    valid_pipeline_configs,
    valid_schemas,
    valid_output_schema
):
    write_json(pipeline_files["config"], valid_pipeline_configs)
    write_json(pipeline_files["schemas"], valid_schemas)
    write_json(pipeline_files["output_schema"], valid_output_schema)

    received_args = {}

    def mock_silver(**kwargs):
        received_args.update(kwargs)
        return True

    monkeypatch.setattr(
        commodities_pipeline,
        "run_alphavantage_commodities_silver",
        mock_silver
    )

    result = commodities_pipeline.run_commodities_pipeline()

    assert result is True
    assert received_args["configs"] == valid_pipeline_configs["commodities"]
    assert received_args["asset_schemas"] == valid_schemas
    assert received_args["silver_output_schema"] == valid_output_schema


# -------------------------------------------------------------------
# Configuration failures
# -------------------------------------------------------------------

def test_run_commodities_pipeline_missing_config(
    monkeypatch,
    pipeline_files
):
    result = commodities_pipeline.run_commodities_pipeline()

    assert result is False


def test_run_commodities_pipeline_invalid_config_json(
    monkeypatch,
    pipeline_files
):
    pipeline_files["config"].write_text(
        "{invalid json",
        encoding="utf-8"
    )

    result = commodities_pipeline.run_commodities_pipeline()

    assert result is False


def test_run_commodities_pipeline_empty_configs(
    monkeypatch,
    pipeline_files
):
    write_json(
        pipeline_files["config"],
        {"commodities": {}}
    )

    result = commodities_pipeline.run_commodities_pipeline()

    assert result is False


# -------------------------------------------------------------------
# Schema loading failures
# -------------------------------------------------------------------

def test_run_commodities_pipeline_missing_schema(
    pipeline_files,
    valid_pipeline_configs
):
    write_json(pipeline_files["config"], valid_pipeline_configs)

    result = commodities_pipeline.run_commodities_pipeline()

    assert result is False


def test_run_commodities_pipeline_empty_schema(
    pipeline_files,
    valid_pipeline_configs
):
    write_json(pipeline_files["config"], valid_pipeline_configs)
    write_json(pipeline_files["schemas"], {})

    result = commodities_pipeline.run_commodities_pipeline()

    assert result is False


# -------------------------------------------------------------------
# Silver execution result
# -------------------------------------------------------------------

def test_run_commodities_pipeline_silver_failure(
    monkeypatch,
    pipeline_files,
    valid_pipeline_configs,
    valid_schemas,
    valid_output_schema
):
    write_json(pipeline_files["config"], valid_pipeline_configs)
    write_json(pipeline_files["schemas"], valid_schemas)
    write_json(pipeline_files["output_schema"], valid_output_schema)

    monkeypatch.setattr(
        commodities_pipeline,
        "run_alphavantage_commodities_silver",
        lambda **kwargs: False
    )

    result = commodities_pipeline.run_commodities_pipeline()

    assert result is False
