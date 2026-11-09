
import pytest

from market_data.pipelines.marketaux.bronze.downloader import (
    _format_api_datetime,
)


def test_formats_utc_timestamp():
    result = _format_api_datetime(
        "2026-11-06T10:30:45+00:00"
    )

    assert result == "2026-11-06T10:30:45"


def test_converts_offset_timestamp_to_utc():
    result = _format_api_datetime(
        "2026-11-06T18:30:45+08:00"
    )

    assert result == "2026-11-06T10:30:45"


def test_assumes_utc_for_naive_timestamp():
    result = _format_api_datetime(
        "2026-11-06T10:30:45"
    )

    assert result == "2026-11-06T10:30:45"


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "not-a-date",
        "2026-99-99T10:30:45",
    ],
)
def test_rejects_invalid_timestamp(value):
    with pytest.raises(ValueError):
        _format_api_datetime(value)
