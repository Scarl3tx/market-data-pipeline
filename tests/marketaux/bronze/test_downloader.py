
import pytest

from market_data.pipelines.marketaux.bronze.downloader import (
    _is_last_page,
)


@pytest.mark.parametrize(
    "metadata,page,limit,expected",
    [
        ({"returned": 3, "found": 10}, 1, 3, False),
        ({"returned": 3, "found": 10}, 2, 3, False),
        ({"returned": 3, "found": 10}, 3, 3, False),
        ({"returned": 1, "found": 10}, 4, 3, True),
        ({"returned": 1, "found": 1}, 1, 3, True),
        ({"returned": 0, "found": 10}, 1, 3, True),
        ({"returned": 3, "found": 0}, 1, 3, True),
    ],
)
def test_identifies_last_page(metadata, page, limit, expected):
    assert _is_last_page(metadata, page, limit) is expected


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"returned": "3", "found": 10},
        {"returned": 3, "found": "10"},
        {"returned": -1, "found": 10},
    ],
)
def test_rejects_invalid_pagination_metadata(metadata):
    with pytest.raises(ValueError):
        _is_last_page(metadata, 1, 3)


def _is_last_page(metadata, page, limit):
    if (
        not isinstance(page, int)
        or isinstance(page, bool)
        or page < 1
    ):
        raise ValueError("Invalid pagination page.")

    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or limit < 1
    ):
        raise ValueError("Invalid pagination limit.")

    if not isinstance(metadata, dict):
        raise ValueError("Invalid Marketaux response metadata.")

    returned = metadata.get("returned")
    found = metadata.get("found")

    if (
        not isinstance(returned, int)
        or isinstance(returned, bool)
        or returned < 0
    ):
        raise ValueError(
            "Invalid Marketaux metadata: returned."
        )

    if (
        not isinstance(found, int)
        or isinstance(found, bool)
        or found < 0
    ):
        raise ValueError(
            "Invalid Marketaux metadata: found."
        )

    if returned > limit:
        raise ValueError(
            "Marketaux returned more articles than requested."
        )

    if returned == 0:
        return True

    return returned < limit or page * limit >= found


@pytest.mark.parametrize("page", [0, -1, 1.5, "1", True])
def test_rejects_invalid_page(page):
    with pytest.raises(ValueError):
        _is_last_page(
            {"returned": 3, "found": 10},
            page,
            3,
        )


@pytest.mark.parametrize("limit", [0, -1, 1.5, "3", True])
def test_rejects_invalid_limit(limit):
    with pytest.raises(ValueError):
        _is_last_page(
            {"returned": 3, "found": 10},
            1,
            limit,
        )


def test_rejects_returned_greater_than_limit():
    with pytest.raises(ValueError):
        _is_last_page(
            {"returned": 4, "found": 10},
            1,
            3,
        )


def test_rejects_non_dictionary_metadata():
    with pytest.raises(ValueError):
        _is_last_page([], 1, 3)
