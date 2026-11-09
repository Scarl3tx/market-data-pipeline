
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from market_data.api.marketaux.quota_tracker import (
    MarketauxDynamoDBQuotaTracker,
)


def make_tracker(limit=100):
    client = Mock()

    tracker = MarketauxDynamoDBQuotaTracker(
        table_name="market-data-api-quotas",
        daily_limit=limit,
        client=client,
    )

    return tracker, client


def test_reserve_request_returns_updated_count():
    tracker, client = make_tracker()

    client.update_item.return_value = {
        "Attributes": {
            "request_count": {"N": "1"},
        }
    }

    assert tracker.reserve_request() == 1

    kwargs = client.update_item.call_args.kwargs

    assert kwargs["TableName"] == "market-data-api-quotas"
    assert kwargs["ReturnValues"] == "UPDATED_NEW"
    assert "request_count < :limit" in kwargs["ConditionExpression"]


def test_reserve_request_uses_atomic_conditional_update():
    tracker, client = make_tracker(limit=100)

    client.update_item.return_value = {
        "Attributes": {
            "request_count": {"N": "50"},
        }
    }

    tracker.reserve_request()

    kwargs = client.update_item.call_args.kwargs

    assert kwargs["UpdateExpression"] == (
        "SET request_count = "
        "if_not_exists(request_count, :zero) + :one"
    )

    assert kwargs["ExpressionAttributeValues"][":limit"] == {
        "N": "100"
    }


def test_reservation_rejects_exhausted_quota():
    tracker, client = make_tracker(limit=2)

    client.update_item.side_effect = ClientError(
        {
            "Error": {
                "Code": "ConditionalCheckFailedException",
                "Message": "Condition not satisfied",
            }
        },
        "UpdateItem",
    )

    with pytest.raises(RuntimeError, match="limit exhausted"):
        tracker.reserve_request()


def test_get_usage_returns_current_count():
    tracker, client = make_tracker(limit=100)

    client.get_item.return_value = {
        "Item": {
            "usage_date": {"S": tracker._today()},
            "request_count": {"N": "75"},
        }
    }

    assert tracker.get_usage() == {
        "used": 75,
        "limit": 100,
        "remaining": 25,
    }

    assert client.get_item.call_args.kwargs["ConsistentRead"] is True


def test_get_usage_returns_zero_for_new_day():
    tracker, client = make_tracker(limit=100)

    client.get_item.return_value = {}

    assert tracker.get_usage() == {
        "used": 0,
        "limit": 100,
        "remaining": 100,
    }


def test_non_quota_dynamodb_errors_are_propagated():
    tracker, client = make_tracker()

    error = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "Access denied",
            }
        },
        "UpdateItem",
    )

    client.update_item.side_effect = error

    with pytest.raises(ClientError):
        tracker.reserve_request()
