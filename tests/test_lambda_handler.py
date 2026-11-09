
import json

import pytest

from market_data import lambda_handler as handler


class FakeSecretsManager:
    def __init__(self, secret_response):
        self.secret_response = secret_response
        self.requested_secret = None

    def get_secret_value(self, SecretId):
        self.requested_secret = SecretId
        return self.secret_response


def test_load_plain_text_api_key(monkeypatch):
    secrets = FakeSecretsManager({
        "SecretString": "test-api-key",
    })

    monkeypatch.setenv(
        "TWELVE_DATA_API_KEY_SECRET_ARN",
        "test-secret-arn",
    )

    monkeypatch.setattr(
        handler.boto3,
        "client",
        lambda service: secrets,
    )

    assert handler._load_api_key() == "test-api-key"
    assert secrets.requested_secret == "test-secret-arn"


@pytest.mark.parametrize(
    "secret",
    [
        {"api_key": "json-api-key"},
        {"TWELVE_DATA_API_KEY": "json-api-key"},
    ],
)
def test_load_json_api_key(monkeypatch, secret):
    secrets = FakeSecretsManager({
        "SecretString": json.dumps(secret),
    })

    monkeypatch.setenv(
        "TWELVE_DATA_API_KEY_SECRET_ARN",
        "test-secret-arn",
    )

    monkeypatch.setattr(
        handler.boto3,
        "client",
        lambda service: secrets,
    )

    assert handler._load_api_key() == "json-api-key"


def test_missing_secret_arn_raises_error(monkeypatch):
    monkeypatch.delenv(
        "TWELVE_DATA_API_KEY_SECRET_ARN",
        raising=False,
    )

    with pytest.raises(
        ValueError,
        match="TWELVE_DATA_API_KEY_SECRET_ARN",
    ):
        handler._load_api_key()


def test_empty_secret_raises_error(monkeypatch):
    secrets = FakeSecretsManager({
        "SecretString": "",
    })

    monkeypatch.setenv(
        "TWELVE_DATA_API_KEY_SECRET_ARN",
        "test-secret-arn",
    )

    monkeypatch.setattr(
        handler.boto3,
        "client",
        lambda service: secrets,
    )

    with pytest.raises(ValueError, match="empty API key"):
        handler._load_api_key()


def test_lambda_handler_runs_complete_pipeline(monkeypatch):
    calls = {}

    def fake_load_api_key():
        return "test-api-key"

    def fake_load_marketaux_api_token():
        return "test-marketaux-token"

    expected_results = {
        "run_id": "test-run-001",
        "status": "success",
        "twelvedata": {
            "status": "success",
            "stages": {
                "bronze": "success",
                "silver": "success",
                "gold": "success",
            },
            "error": None,
        },
        "marketaux": {
            "status": "success",
            "stages": {
                "bronze": "success",
                "silver": "success",
                "gold": "success",
            },
            "error": None,
        },
        "combined_gold": {
            "status": "success",
            "rows": 100,
            "error": None,
        },
    }

    def fake_pipeline():
        calls["api_key"] = handler.os.environ.get(
            "TWELVE_DATA_API_KEY"
        )
        calls["marketaux_token"] = handler.os.environ.get(
            "MARKETAUX_API_TOKEN"
        )
        calls["pipeline_called"] = True
        return expected_results

    monkeypatch.setattr(
        handler,
        "_load_api_key",
        fake_load_api_key,
    )

    monkeypatch.setattr(
        handler,
        "_load_marketaux_api_token",
        fake_load_marketaux_api_token,
    )

    monkeypatch.setattr(
        handler,
        "run_market_data_pipeline",
        fake_pipeline,
    )

    result = handler.lambda_handler({}, None)

    assert calls["api_key"] == "test-api-key"
    assert calls["marketaux_token"] == "test-marketaux-token"
    assert calls["pipeline_called"] is True
    assert result == expected_results
    assert result["twelvedata"]["status"] == "success"
    assert result["marketaux"]["status"] == "success"
    assert result["combined_gold"]["status"] == "success"

@pytest.mark.parametrize(
    "prefix",
    [
        "\ufeff",
        "\u00ef\u00bb\u00bf",
    ],
)
def test_load_api_key_removes_bom_prefix(monkeypatch, prefix):
    secrets = FakeSecretsManager({
        "SecretString": f"{prefix}test-api-key",
    })

    monkeypatch.setenv(
        "TWELVE_DATA_API_KEY_SECRET_ARN",
        "test-secret-arn",
    )

    monkeypatch.setattr(
        handler.boto3,
        "client",
        lambda service: secrets,
    )

    assert handler._load_api_key() == "test-api-key"

class FakeSNS:
    def __init__(self, error=None):
        self.error = error
        self.published_messages = []

    def publish(self, **kwargs):
        if self.error:
            raise self.error

        self.published_messages.append(kwargs)

        return {"MessageId": "test-message-001"}

def configure_fake_lambda(
    monkeypatch,
    sns,
    pipeline_results=None,
    pipeline_error=None,
):
    monkeypatch.setenv(
        "SNS_NOTIFICATION_TOPIC_ARN",
        "arn:aws:sns:ap-southeast-2:860897619002:market-data-pipeline-notifications",
    )

    monkeypatch.setattr(
        handler,
        "_load_api_key",
        lambda: "test-api-key",
    )

    monkeypatch.setattr(
        handler,
        "_load_marketaux_api_token",
        lambda: "test-marketaux-token",
    )

    monkeypatch.setattr(
        handler.boto3,
        "client",
        lambda service: sns if service == "sns" else None,
    )

    def fake_pipeline():
        if pipeline_error:
            raise pipeline_error

        return pipeline_results

    monkeypatch.setattr(
        handler,
        "run_market_data_pipeline",
        fake_pipeline,
    )


def test_lambda_handler_publishes_success_notification(monkeypatch):
    sns = FakeSNS()

    results = {
        "run_id": "success-001",
        "status": "success",
        "twelvedata": {"status": "success"},
        "marketaux": {"status": "success"},
        "combined_gold": {"status": "success"},
    }

    configure_fake_lambda(
        monkeypatch,
        sns,
        pipeline_results=results,
    )

    result = handler.lambda_handler({}, None)

    assert result == results
    assert len(sns.published_messages) == 1

    message = sns.published_messages[0]

    assert message["Subject"] == "Market Data Pipeline: SUCCESS"
    assert "success-001" in message["Message"]
    assert "Overall status: SUCCESS" in message["Message"]


def test_lambda_handler_publishes_partial_failure_notification(monkeypatch):
    sns = FakeSNS()

    results = {
        "run_id": "partial-001",
        "status": "partial_failure",
        "twelvedata": {"status": "success"},
        "marketaux": {
            "status": "partial_failure",
            "error": "Marketaux API token not found.",
        },
        "combined_gold": {"status": "success"},
    }

    configure_fake_lambda(
        monkeypatch,
        sns,
        pipeline_results=results,
    )

    result = handler.lambda_handler({}, None)

    assert result == results
    assert len(sns.published_messages) == 1

    message = sns.published_messages[0]

    assert message["Subject"] == "Market Data Pipeline: PARTIAL FAILURE"
    assert "Overall status: PARTIAL FAILURE" in message["Message"]
    assert "Marketaux API token not found." in message["Message"]


def test_lambda_handler_publishes_failure_notification(monkeypatch):
    sns = FakeSNS()

    configure_fake_lambda(
        monkeypatch,
        sns,
        pipeline_error=RuntimeError("Pipeline crashed"),
    )

    with pytest.raises(RuntimeError, match="Pipeline crashed"):
        handler.lambda_handler({}, None)

    assert len(sns.published_messages) == 1

    message = sns.published_messages[0]

    assert message["Subject"] == "Market Data Pipeline: FAILURE"
    assert "RuntimeError: Pipeline crashed" in message["Message"]


def test_sns_failure_does_not_mask_successful_pipeline(monkeypatch):
    sns = FakeSNS(error=RuntimeError("SNS unavailable"))

    results = {
        "run_id": "sns-error-001",
        "status": "success",
    }

    configure_fake_lambda(
        monkeypatch,
        sns,
        pipeline_results=results,
    )

    result = handler.lambda_handler({}, None)

    assert result == results

def test_missing_sns_configuration_does_not_break_pipeline(monkeypatch):
    monkeypatch.delenv(
        "SNS_NOTIFICATION_TOPIC_ARN",
        raising=False,
    )

    monkeypatch.setattr(
        handler,
        "_load_api_key",
        lambda: "test-api-key",
    )

    monkeypatch.setattr(
        handler,
        "_load_marketaux_api_token",
        lambda: "test-marketaux-token",
    )

    results = {
        "run_id": "no-sns-001",
        "status": "success",
    }

    monkeypatch.setattr(
        handler,
        "run_market_data_pipeline",
        lambda: results,
    )

    result = handler.lambda_handler({}, None)

    assert result == results

def test_skipped_pipeline_notification_is_not_failure():
    assert handler._get_notification_status(
        {"status": "skipped"}
    ) == "SKIPPED"