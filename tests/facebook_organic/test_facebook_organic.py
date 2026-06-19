import pytest
import requests
from unittest.mock import MagicMock

import pandas as pd


PAGE_ID = "302064363144019"


# ---------------------------------------------------------------------------
# Instantiation — page_id is NOT a constructor argument
# ---------------------------------------------------------------------------


def test_instantiation_without_page_id(fb):
    """FacebookOrganic can be instantiated with only access_token."""
    assert fb.access_token == "fake_token"
    assert not hasattr(fb, "page_id")


def test_same_instance_works_for_multiple_page_ids(metrics, mocker):
    """One instance can serve multiple pages without re-instantiating."""
    from d2b_data.FacebookOrganic import FacebookOrganic

    fb = FacebookOrganic(access_token="fake_token")
    mocker.patch.object(fb, "get_posts", return_value=[])

    df1 = fb.get_report_dataframe("page_aaa", "2024-01-01", "2024-01-31", metrics)
    df2 = fb.get_report_dataframe("page_bbb", "2024-01-01", "2024-01-31", metrics)

    assert isinstance(df1, pd.DataFrame)
    assert isinstance(df2, pd.DataFrame)


# ---------------------------------------------------------------------------
# _get — HTTP layer
# ---------------------------------------------------------------------------


def test_get_injects_access_token(fb, mocker):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"data": []}
    mock_get = mocker.patch("requests.get", return_value=mock_resp)

    fb._get("/some/endpoint", {"field": "value"})

    _, kwargs = mock_get.call_args
    assert kwargs["params"]["access_token"] == "fake_token"
    assert kwargs["params"]["field"] == "value"


def test_get_raises_on_http_error(fb, mocker):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.HTTPError("403")
    mock_resp.text = "Forbidden"
    mocker.patch("requests.get", return_value=mock_resp)

    with pytest.raises(requests.HTTPError):
        fb._get("/bad/endpoint", {})


def test_get_raises_on_api_error_payload(fb, mocker):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"error": {"code": 190, "message": "Invalid token"}}
    mocker.patch("requests.get", return_value=mock_resp)

    with pytest.raises(ValueError, match="Invalid token"):
        fb._get("/me", {})


def test_get_returns_payload_on_success(fb, mocker):
    payload = {"data": [{"id": "1"}], "paging": {}}
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = payload
    mocker.patch("requests.get", return_value=mock_resp)

    result = fb._get("/some/endpoint", {})
    assert result == payload


# ---------------------------------------------------------------------------
# _paginate
# ---------------------------------------------------------------------------


def _pages_side_effect(pages):
    iterator = iter(pages)

    def side_effect(endpoint, params):
        return next(iterator)

    return side_effect


def test_paginate_single_page(fb, mocker):
    page = {"data": [{"id": "1"}, {"id": "2"}], "paging": {}}
    mocker.patch.object(fb, "_get", side_effect=_pages_side_effect([page]))

    result = fb._paginate(f"/{PAGE_ID}/posts", {})
    assert result == [{"id": "1"}, {"id": "2"}]


def test_paginate_multiple_pages(fb, mocker):
    pages = [
        {"data": [{"id": "1"}], "paging": {"next": "url", "cursors": {"after": "c1"}}},
        {"data": [{"id": "2"}], "paging": {"next": "url", "cursors": {"after": "c2"}}},
        {"data": [{"id": "3"}], "paging": {}},
    ]
    mocker.patch.object(fb, "_get", side_effect=_pages_side_effect(pages))

    result = fb._paginate(f"/{PAGE_ID}/posts", {})
    assert [r["id"] for r in result] == ["1", "2", "3"]


def test_paginate_stops_on_empty_data(fb, mocker):
    pages = [
        {"data": [{"id": "1"}], "paging": {"next": "url", "cursors": {"after": "c1"}}},
        {"data": [], "paging": {"next": "url", "cursors": {"after": "c2"}}},
    ]
    mocker.patch.object(fb, "_get", side_effect=_pages_side_effect(pages))

    result = fb._paginate(f"/{PAGE_ID}/posts", {})
    assert len(result) == 1


# ---------------------------------------------------------------------------
# _flatten_insights
# ---------------------------------------------------------------------------


def test_flatten_insights_basic(fb):
    data = [
        {"name": "post_impressions", "period": "lifetime", "values": [{"value": 1000}]},
        {"name": "post_clicks", "period": "lifetime", "values": [{"value": 50}]},
    ]
    result = fb._flatten_insights(data)
    assert result == {"post_impressions": 1000, "post_clicks": 50}


def test_flatten_insights_expands_reactions(fb):
    data = [
        {
            "name": "post_reactions_by_type_total",
            "period": "lifetime",
            "values": [{"value": {"like": 10, "love": 3, "haha": 1}}],
        }
    ]
    result = fb._flatten_insights(data)
    assert result == {"reactions_like": 10, "reactions_love": 3, "reactions_haha": 1}
    assert "post_reactions_by_type_total" not in result


def test_flatten_insights_returns_none_for_empty_values(fb):
    data = [{"name": "post_impressions", "period": "lifetime", "values": []}]
    result = fb._flatten_insights(data)
    assert result["post_impressions"] is None


def test_flatten_insights_empty_data(fb):
    assert fb._flatten_insights([]) == {}


# ---------------------------------------------------------------------------
# get_posts — normalization of shares / comments / reactions
# ---------------------------------------------------------------------------


def test_get_posts_normalizes_shares(fb, mocker):
    raw = [{"id": "p1", "shares": {"count": 7}}]
    mocker.patch.object(fb, "_paginate", return_value=raw)

    posts = fb.get_posts(PAGE_ID, "2024-01-01", "2024-01-31")
    assert posts[0]["shares"] == 7


def test_get_posts_sets_shares_to_zero_when_absent(fb, mocker):
    raw = [{"id": "p1"}]
    mocker.patch.object(fb, "_paginate", return_value=raw)

    posts = fb.get_posts(PAGE_ID, "2024-01-01", "2024-01-31")
    assert posts[0]["shares"] == 0


def test_get_posts_normalizes_comments(fb, mocker):
    raw = [{"id": "p1", "comments": {"summary": {"total_count": 4}}}]
    mocker.patch.object(fb, "_paginate", return_value=raw)

    posts = fb.get_posts(PAGE_ID, "2024-01-01", "2024-01-31")
    assert posts[0]["comments"] == 4


def test_get_posts_normalizes_reactions(fb, mocker):
    raw = [{"id": "p1", "reactions": {"summary": {"total_count": 12}}}]
    mocker.patch.object(fb, "_paginate", return_value=raw)

    posts = fb.get_posts(PAGE_ID, "2024-01-01", "2024-01-31")
    assert posts[0]["reactions"] == 12


def test_get_posts_uses_page_id_in_endpoint(fb, mocker):
    mock_paginate = mocker.patch.object(fb, "_paginate", return_value=[])

    fb.get_posts("my_page_id", "2024-01-01", "2024-01-31")

    endpoint = mock_paginate.call_args[0][0]
    assert "/my_page_id/posts" in endpoint


# ---------------------------------------------------------------------------
# get_post_insights
# ---------------------------------------------------------------------------


def test_get_post_insights_returns_flat_dict(fb, mocker):
    mocker.patch.object(
        fb,
        "_get",
        return_value={
            "data": [
                {"name": "post_impressions", "period": "lifetime", "values": [{"value": 999}]}
            ]
        },
    )
    result = fb.get_post_insights("p1", ["post_impressions"])
    assert result == {"post_impressions": 999}


def test_get_post_insights_returns_empty_dict_on_value_error(fb, mocker):
    mocker.patch.object(fb, "_get", side_effect=ValueError("unavailable"))
    assert fb.get_post_insights("p1", ["post_impressions"]) == {}


def test_get_post_insights_returns_empty_dict_on_http_error(fb, mocker):
    mocker.patch.object(fb, "_get", side_effect=requests.HTTPError("500"))
    assert fb.get_post_insights("p1", ["post_impressions"]) == {}


# ---------------------------------------------------------------------------
# get_report_dataframe
# ---------------------------------------------------------------------------


def test_get_report_dataframe_returns_empty_df_when_no_posts(fb, metrics, mocker):
    mocker.patch.object(fb, "get_posts", return_value=[])

    df = fb.get_report_dataframe(PAGE_ID, "2024-01-01", "2024-01-31", metrics)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_report_dataframe_one_row_per_post(fb, metrics, mocker):
    posts = [
        {
            "id": "p1",
            "message": "Hello",
            "created_time": "2024-01-10T12:00:00+0000",
            "like_count": 5,
            "comments": 2,
            "reactions": 7,
            "shares": 1,
        },
        {
            "id": "p2",
            "message": "World",
            "created_time": "2024-01-11T12:00:00+0000",
            "like_count": 3,
            "comments": 0,
            "reactions": 3,
            "shares": 0,
        },
    ]
    mocker.patch.object(fb, "get_posts", return_value=posts)
    mocker.patch.object(fb, "get_post_insights", return_value={"post_impressions": 100})

    df = fb.get_report_dataframe(PAGE_ID, "2024-01-01", "2024-01-31", metrics)

    assert len(df) == 2
    assert list(df["post_id"]) == ["p1", "p2"]
    assert "post_impressions" in df.columns


def test_get_report_dataframe_created_time_is_datetime(fb, metrics, mocker):
    posts = [
        {
            "id": "p1",
            "message": "",
            "created_time": "2024-01-10T12:00:00+0000",
            "like_count": 0,
            "comments": 0,
            "reactions": 0,
            "shares": 0,
        }
    ]
    mocker.patch.object(fb, "get_posts", return_value=posts)
    mocker.patch.object(fb, "get_post_insights", return_value={})

    df = fb.get_report_dataframe(PAGE_ID, "2024-01-01", "2024-01-31", metrics)
    assert pd.api.types.is_datetime64_any_dtype(df["created_time"])


def test_get_report_dataframe_accepts_yyyymmdd_format(fb, metrics, mocker):
    mocker.patch.object(fb, "get_posts", return_value=[])

    df = fb.get_report_dataframe(PAGE_ID, "20240101", "20240131", metrics)
    assert df.empty


def test_get_report_dataframe_raises_on_invalid_start_date(fb, metrics):
    with pytest.raises(ValueError, match="start_date"):
        fb.get_report_dataframe(PAGE_ID, "01-01-2024", "2024-01-31", metrics)


def test_get_report_dataframe_raises_on_missing_start_date(fb, metrics):
    with pytest.raises(ValueError, match="start date"):
        fb.get_report_dataframe(PAGE_ID, "", "2024-01-31", metrics)


def test_get_report_dataframe_raises_on_invalid_end_date(fb, metrics):
    with pytest.raises(ValueError, match="end_date"):
        fb.get_report_dataframe(PAGE_ID, "2024-01-01", "31-01-2024", metrics)


def test_get_report_dataframe_raises_on_missing_end_date(fb, metrics):
    with pytest.raises(ValueError, match="end date"):
        fb.get_report_dataframe(PAGE_ID, "2024-01-01", "", metrics)


def test_get_report_dataframe_includes_page_id_column(fb, metrics, mocker):
    posts = [
        {
            "id": "p1",
            "message": "",
            "created_time": "2024-01-10T12:00:00+0000",
            "like_count": 0,
            "comments": 0,
            "reactions": 0,
            "shares": 0,
        }
    ]
    mocker.patch.object(fb, "get_posts", return_value=posts)
    mocker.patch.object(fb, "get_post_insights", return_value={})

    df = fb.get_report_dataframe(PAGE_ID, "2024-01-01", "2024-01-31", metrics)
    assert df.iloc[0]["page_id"] == PAGE_ID


def test_get_report_dataframe_insight_failure_still_produces_row(fb, metrics, mocker):
    """A failed insights call doesn't drop the post row."""
    posts = [
        {
            "id": "p1",
            "message": "",
            "created_time": "2024-01-10T12:00:00+0000",
            "like_count": 0,
            "comments": 0,
            "reactions": 0,
            "shares": 0,
        }
    ]
    mocker.patch.object(fb, "get_posts", return_value=posts)
    mocker.patch.object(fb, "get_post_insights", return_value={})

    df = fb.get_report_dataframe(PAGE_ID, "2024-01-01", "2024-01-31", metrics)
    assert len(df) == 1
