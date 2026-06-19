import pytest
import requests
from unittest.mock import MagicMock, patch

import pandas as pd


# ---------------------------------------------------------------------------
# _get — HTTP layer
# ---------------------------------------------------------------------------


def test_get_injects_access_token(ig, mocker):
    """access_token is always added to the request params."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"data": []}
    mock_get = mocker.patch("requests.get", return_value=mock_resp)

    ig._get("/some/endpoint", {"field": "value"})

    _, kwargs = mock_get.call_args
    assert kwargs["params"]["access_token"] == "fake_token"
    assert kwargs["params"]["field"] == "value"


def test_get_raises_on_http_error(ig, mocker):
    """HTTP 4xx/5xx raises requests.HTTPError."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.HTTPError("403")
    mock_resp.text = "Forbidden"
    mocker.patch("requests.get", return_value=mock_resp)

    with pytest.raises(requests.HTTPError):
        ig._get("/bad/endpoint", {})


def test_get_raises_on_api_error_payload(ig, mocker):
    """Graph API error payloads (200 OK with 'error' key) raise ValueError."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"error": {"code": 190, "message": "Invalid token"}}
    mocker.patch("requests.get", return_value=mock_resp)

    with pytest.raises(ValueError, match="Invalid token"):
        ig._get("/me", {})


def test_get_returns_payload_on_success(ig, mocker):
    """Successful response returns the parsed JSON dict."""
    payload = {"data": [{"id": "1"}], "paging": {}}
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = payload
    mocker.patch("requests.get", return_value=mock_resp)

    result = ig._get("/some/endpoint", {})
    assert result == payload


# ---------------------------------------------------------------------------
# _paginate — cursor-based pagination
# ---------------------------------------------------------------------------


def _make_get_side_effect(pages):
    """Return a side_effect callable that cycles through page payloads."""
    iterator = iter(pages)

    def side_effect(endpoint, params):
        return next(iterator)

    return side_effect


def test_paginate_single_page(ig, mocker):
    """Single page with no 'next' link returns all records."""
    page = {"data": [{"id": "1"}, {"id": "2"}], "paging": {}}
    mocker.patch.object(ig, "_get", side_effect=_make_get_side_effect([page]))

    result = ig._paginate("/acc/media", {})
    assert result == [{"id": "1"}, {"id": "2"}]


def test_paginate_multiple_pages(ig, mocker):
    """Pagination follows next cursor until exhausted."""
    pages = [
        {
            "data": [{"id": "1"}],
            "paging": {"next": "url", "cursors": {"after": "cur1"}},
        },
        {
            "data": [{"id": "2"}],
            "paging": {"next": "url", "cursors": {"after": "cur2"}},
        },
        {"data": [{"id": "3"}], "paging": {}},
    ]
    mocker.patch.object(ig, "_get", side_effect=_make_get_side_effect(pages))

    result = ig._paginate("/acc/media", {})
    assert [r["id"] for r in result] == ["1", "2", "3"]


def test_paginate_stops_on_empty_data(ig, mocker):
    """Pagination stops when an empty data page is returned."""
    pages = [
        {
            "data": [{"id": "1"}],
            "paging": {"next": "url", "cursors": {"after": "cur1"}},
        },
        {"data": [], "paging": {"next": "url", "cursors": {"after": "cur2"}}},
    ]
    mocker.patch.object(ig, "_get", side_effect=_make_get_side_effect(pages))

    result = ig._paginate("/acc/media", {})
    assert len(result) == 1


# ---------------------------------------------------------------------------
# _get_media — media fetch + type filtering
# ---------------------------------------------------------------------------


def test_get_media_filters_by_product_type(ig, mocker):
    """Only media matching media_product_type is returned."""
    all_media = [
        {"id": "1", "media_product_type": "FEED"},
        {"id": "2", "media_product_type": "REELS"},
        {"id": "3", "media_product_type": "FEED"},
    ]
    mocker.patch.object(ig, "_paginate", return_value=all_media)

    result = ig._get_media("acc123", "2024-01-01", "2024-01-31", "FEED")
    assert len(result) == 2
    assert all(m["media_product_type"] == "FEED" for m in result)


def test_get_media_returns_empty_when_no_match(ig, mocker):
    """Returns empty list when no media matches the requested type."""
    mocker.patch.object(ig, "_paginate", return_value=[{"id": "1", "media_product_type": "REELS"}])

    result = ig._get_media("acc123", "2024-01-01", "2024-01-31", "FEED")
    assert result == []


# ---------------------------------------------------------------------------
# _get_stories
# ---------------------------------------------------------------------------


def test_get_stories_uses_stories_endpoint(ig, mocker):
    """Stories are fetched from /{ig_account_id}/stories."""
    mock_paginate = mocker.patch.object(ig, "_paginate", return_value=[{"id": "s1"}])

    ig._get_stories("acc123")

    call_args = mock_paginate.call_args
    assert "/acc123/stories" in call_args[0][0]


def test_get_stories_returns_all_stories(ig, mocker):
    stories = [{"id": "s1"}, {"id": "s2"}]
    mocker.patch.object(ig, "_paginate", return_value=stories)

    result = ig._get_stories("acc123")
    assert result == stories


# ---------------------------------------------------------------------------
# _flatten_insights
# ---------------------------------------------------------------------------


def test_flatten_insights_extracts_lifetime_values(ig):
    data = [
        {"name": "impressions", "period": "lifetime", "values": [{"value": 500}]},
        {"name": "reach", "period": "lifetime", "values": [{"value": 300}]},
    ]
    result = ig._flatten_insights(data)
    assert result == {"impressions": 500, "reach": 300}


def test_flatten_insights_returns_none_for_empty_values(ig):
    data = [{"name": "impressions", "period": "lifetime", "values": []}]
    result = ig._flatten_insights(data)
    assert result["impressions"] is None


def test_flatten_insights_empty_data(ig):
    assert ig._flatten_insights([]) == {}


# ---------------------------------------------------------------------------
# _get_media_insights
# ---------------------------------------------------------------------------


def test_get_media_insights_returns_flat_dict(ig, mocker):
    """Successful insights call returns flattened metrics."""
    mocker.patch.object(
        ig,
        "_get",
        return_value={
            "data": [
                {"name": "likes", "period": "lifetime", "values": [{"value": 42}]}
            ]
        },
    )
    result = ig._get_media_insights("media123", ["likes"])
    assert result == {"likes": 42}


def test_get_media_insights_returns_empty_dict_on_failure(ig, mocker):
    """Errors during insights fetch return {} so processing continues."""
    mocker.patch.object(ig, "_get", side_effect=ValueError("unavailable"))

    result = ig._get_media_insights("media123", ["likes"])
    assert result == {}


def test_get_media_insights_returns_empty_dict_on_http_error(ig, mocker):
    mocker.patch.object(ig, "_get", side_effect=requests.HTTPError("500"))

    result = ig._get_media_insights("media123", ["impressions"])
    assert result == {}


# ---------------------------------------------------------------------------
# _build_dataframe
# ---------------------------------------------------------------------------


def test_build_dataframe_returns_empty_df_for_empty_media(ig, feed_metrics):
    df = ig._build_dataframe([], "acc123", feed_metrics)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_build_dataframe_one_row_per_media(ig, feed_metrics, mocker):
    media_list = [
        {
            "id": "m1",
            "media_type": "IMAGE",
            "media_product_type": "FEED",
            "caption": "hello",
            "timestamp": "2024-01-01T10:00:00+0000",
            "permalink": "https://ig.com/p/abc",
        },
        {
            "id": "m2",
            "media_type": "VIDEO",
            "media_product_type": "FEED",
            "caption": "world",
            "timestamp": "2024-01-02T10:00:00+0000",
            "permalink": "https://ig.com/p/def",
        },
    ]
    mocker.patch.object(ig, "_get_media_insights", return_value={"impressions": 100})

    df = ig._build_dataframe(media_list, "acc123", feed_metrics)

    assert len(df) == 2
    assert list(df["media_id"]) == ["m1", "m2"]
    assert "impressions" in df.columns


def test_build_dataframe_timestamp_is_datetime(ig, feed_metrics, mocker):
    media_list = [
        {
            "id": "m1",
            "media_type": "IMAGE",
            "media_product_type": "FEED",
            "caption": "",
            "timestamp": "2024-01-01T10:00:00+0000",
            "permalink": "",
        }
    ]
    mocker.patch.object(ig, "_get_media_insights", return_value={})

    df = ig._build_dataframe(media_list, "acc123", feed_metrics)
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])


def test_build_dataframe_insight_failure_still_produces_row(ig, feed_metrics, mocker):
    """A failed insights fetch doesn't drop the media row."""
    media_list = [
        {
            "id": "m1",
            "media_type": "IMAGE",
            "media_product_type": "FEED",
            "caption": "",
            "timestamp": "2024-01-01T10:00:00+0000",
            "permalink": "",
        }
    ]
    mocker.patch.object(ig, "_get_media_insights", return_value={})

    df = ig._build_dataframe(media_list, "acc123", feed_metrics)
    assert len(df) == 1
    assert df.iloc[0]["media_id"] == "m1"


# ---------------------------------------------------------------------------
# get_feed / get_reels / get_stories — public API
# ---------------------------------------------------------------------------


def test_get_feed_calls_get_media_with_feed_type(ig, feed_metrics, mocker):
    mock_get_media = mocker.patch.object(ig, "_get_media", return_value=[])
    mocker.patch.object(ig, "_build_dataframe", return_value=pd.DataFrame())

    ig.get_feed("acc123", feed_metrics, since="2024-01-01", until="2024-01-31")

    mock_get_media.assert_called_once_with("acc123", "2024-01-01", "2024-01-31", "FEED")


def test_get_reels_calls_get_media_with_reels_type(ig, feed_metrics, mocker):
    mock_get_media = mocker.patch.object(ig, "_get_media", return_value=[])
    mocker.patch.object(ig, "_build_dataframe", return_value=pd.DataFrame())

    ig.get_reels("acc123", feed_metrics, since="2024-01-01", until="2024-01-31")

    mock_get_media.assert_called_once_with("acc123", "2024-01-01", "2024-01-31", "REELS")


def test_get_stories_calls_get_stories(ig, story_metrics, mocker):
    mock_get_stories = mocker.patch.object(ig, "_get_stories", return_value=[])
    mocker.patch.object(ig, "_build_dataframe", return_value=pd.DataFrame())

    ig.get_stories("acc123", story_metrics)

    mock_get_stories.assert_called_once_with("acc123")


def test_get_feed_returns_dataframe(ig, feed_metrics, mocker):
    mocker.patch.object(ig, "_get_media", return_value=[])
    mocker.patch.object(ig, "_build_dataframe", return_value=pd.DataFrame({"x": [1]}))

    df = ig.get_feed("acc123", feed_metrics)
    assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# get_all
# ---------------------------------------------------------------------------


def test_get_all_returns_dict_with_three_keys(ig, feed_metrics, story_metrics, mocker):
    empty_df = pd.DataFrame()
    mocker.patch.object(ig, "get_feed", return_value=empty_df)
    mocker.patch.object(ig, "get_reels", return_value=empty_df)
    mocker.patch.object(ig, "get_stories", return_value=empty_df)

    result = ig.get_all(
        "acc123",
        feed_metrics=feed_metrics,
        reels_metrics=feed_metrics,
        story_metrics=story_metrics,
    )

    assert set(result.keys()) == {"feed", "reels", "stories"}
    assert all(isinstance(v, pd.DataFrame) for v in result.values())


# ---------------------------------------------------------------------------
# _resolve_dates
# ---------------------------------------------------------------------------


def test_resolve_dates_uses_provided_values(ig):
    since, until = ig._resolve_dates("2024-01-01", "2024-01-31")
    assert since == "2024-01-01"
    assert until == "2024-01-31"


def test_resolve_dates_defaults_when_none(ig):
    since, until = ig._resolve_dates(None, None)
    assert since is not None
    assert until is not None
    assert since < until
