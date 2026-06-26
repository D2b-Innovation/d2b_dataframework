from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests


# ------------------------------------------------------------------
# Init & Auth
# ------------------------------------------------------------------


def test_instance_with_valid_token(linkedin):
    """Token loaded from file sets headers correctly."""
    assert linkedin.token == "fake_token_123"
    assert linkedin.headers["Authorization"] == "Bearer fake_token_123"
    assert linkedin.headers["X-Restli-Protocol-Version"] == "2.0.0"


def test_instance_no_file_specified(linkedin_no_file):
    """Without token_path, token and headers remain None."""
    assert linkedin_no_file.token is None
    assert linkedin_no_file.headers is None


def test_instance_bad_file(linkedin_bad_file):
    """When file read fails, token stays None."""
    assert linkedin_bad_file.token is None
    assert linkedin_bad_file.headers is None


def test_load_token_missing_access_token_key(mocker):
    """Token file without 'access_token' key returns None."""
    fake_data = {"refresh_token": "something"}
    mocker.patch("builtins.open", mocker.mock_open())
    mocker.patch("json.load", return_value=fake_data)

    from d2b_data.linkedin_organic import LinkedinOrganic

    li = LinkedinOrganic(token_path="incomplete.json")
    assert li.token is None


def test_set_headers_without_token():
    """_set_headers does nothing when token is None."""
    from d2b_data.linkedin_organic import LinkedinOrganic

    li = LinkedinOrganic()
    li.token = None
    li._set_headers()
    assert li.headers is None


# ------------------------------------------------------------------
# _request_get
# ------------------------------------------------------------------


def test_request_get_no_headers_raises():
    """Calling _request_get without auth raises RuntimeError."""
    from d2b_data.linkedin_organic import LinkedinOrganic

    li = LinkedinOrganic()
    with pytest.raises(RuntimeError, match="Headers not set"):
        li._request_get("https://example.com")


def test_request_get_success(linkedin, mocker):
    """Successful GET returns parsed JSON."""
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {"elements": []}

    mocker.patch("requests.Session.send", return_value=fake_response)

    result = linkedin._request_get("https://api.linkedin.com/rest/test")
    assert result == {"elements": []}


def test_request_get_429_raises_quota_error(linkedin, mocker):
    """429 response raises QuotaExhaustedError immediately."""
    from d2b_data.linkedin_organic import QuotaExhaustedError

    fake_response = MagicMock()
    fake_response.status_code = 429

    mocker.patch("requests.Session.send", return_value=fake_response)

    with pytest.raises(QuotaExhaustedError, match="daily quota"):
        linkedin._request_get("https://api.linkedin.com/rest/test")


def test_request_get_transient_error_retries(linkedin, mocker):
    """Transient 500 errors are retried with backoff."""
    fail = MagicMock()
    fail.status_code = 500

    success = MagicMock()
    success.status_code = 200
    success.json.return_value = {"ok": True}

    mocker.patch("requests.Session.send", side_effect=[fail, success])
    mock_sleep = mocker.patch("d2b_data.linkedin_organic.time.sleep")

    result = linkedin._request_get("https://api.linkedin.com/rest/test")
    assert result == {"ok": True}
    assert mock_sleep.call_count == 1
    mock_sleep.assert_called_with(1)


def test_request_get_exhausts_retries(linkedin, mocker):
    """After max retries on 500, raise_for_status is called."""
    fail = MagicMock()
    fail.status_code = 500
    fail.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")

    mocker.patch("requests.Session.send", return_value=fail)
    mocker.patch("d2b_data.linkedin_organic.time.sleep")

    with pytest.raises(requests.exceptions.HTTPError):
        linkedin._request_get("https://api.linkedin.com/rest/test", max_retries=2)


def test_request_get_non_retryable_error(linkedin, mocker):
    """Non-transient errors (e.g. 403) raise immediately."""
    fail = MagicMock()
    fail.status_code = 403
    fail.raise_for_status.side_effect = requests.exceptions.HTTPError("403 Forbidden")

    mocker.patch("requests.Session.send", return_value=fail)

    with pytest.raises(requests.exceptions.HTTPError):
        linkedin._request_get("https://api.linkedin.com/rest/test")


# ------------------------------------------------------------------
# get_managed_pages
# ------------------------------------------------------------------


def test_get_managed_pages_success(linkedin, mocker):
    """Returns DataFrame with org_id, role, state columns."""
    raw = {
        "elements": [
            {
                "organization": "urn:li:organization:12345",
                "role": "ADMINISTRATOR",
                "state": "APPROVED",
            },
            {
                "organization": "urn:li:organization:67890",
                "role": "ANALYST",
                "state": "APPROVED",
            },
        ]
    }
    mocker.patch.object(linkedin, "_get_managed_pages_raw", return_value=raw)

    df = linkedin.get_managed_pages()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["org_id", "role", "state"]
    assert df.iloc[0]["org_id"] == "12345"


def test_get_managed_pages_with_role_filter(linkedin, mocker):
    """Role filter is passed to the raw method."""
    mock_raw = mocker.patch.object(
        linkedin, "_get_managed_pages_raw", return_value={"elements": []}
    )
    linkedin.get_managed_pages(role="ADMINISTRATOR")
    mock_raw.assert_called_once_with("ADMINISTRATOR")


def test_get_managed_pages_api_failure(linkedin, mocker):
    """Returns None when raw method fails."""
    mocker.patch.object(linkedin, "_get_managed_pages_raw", return_value=None)
    assert linkedin.get_managed_pages() is None


def test_get_managed_pages_empty_elements(linkedin, mocker):
    """Returns empty DataFrame when no elements."""
    mocker.patch.object(
        linkedin, "_get_managed_pages_raw", return_value={"elements": []}
    )
    df = linkedin.get_managed_pages()
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ------------------------------------------------------------------
# get_follower_stats
# ------------------------------------------------------------------


def test_get_follower_stats_success(linkedin, mocker):
    """Returns pivoted DataFrame with follower counts."""
    raw = {
        "elements": [
            {
                "organizationalEntity": "urn:li:organization:12345",
                "followerCountsByFunction": [
                    {
                        "function": "ENGINEERING",
                        "followerCounts": {
                            "organicFollowerCount": 100,
                            "paidFollowerCount": 20,
                        },
                    },
                    {
                        "function": "MARKETING",
                        "followerCounts": {
                            "organicFollowerCount": 50,
                            "paidFollowerCount": 10,
                        },
                    },
                ],
            }
        ]
    }
    mocker.patch.object(linkedin, "_get_follower_stats_raw", return_value=raw)

    df = linkedin.get_follower_stats("12345")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "organic_followers" in df.columns
    assert "paid_followers" in df.columns


def test_get_follower_stats_api_failure(linkedin, mocker):
    """Returns None when raw method fails."""
    mocker.patch.object(linkedin, "_get_follower_stats_raw", return_value=None)
    assert linkedin.get_follower_stats("12345") is None


def test_get_follower_stats_empty_elements(linkedin, mocker):
    """Returns empty DataFrame when no elements."""
    mocker.patch.object(
        linkedin, "_get_follower_stats_raw", return_value={"elements": []}
    )
    df = linkedin.get_follower_stats("12345")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ------------------------------------------------------------------
# get_posts
# ------------------------------------------------------------------


def test_get_posts_success(linkedin, mocker):
    """Returns DataFrame with post metadata."""
    posts = [
        {
            "id": "urn:li:share:111",
            "createdAt": 1704067200000,  # 2024-01-01 00:00:00 UTC
            "commentary": "Hello world",
            "content": {"article": {"source": "https://example.com"}},
            "visibility": "PUBLIC",
            "lifecycleState": "PUBLISHED",
        },
        {
            "id": "urn:li:share:222",
            "createdAt": 1704153600000,  # 2024-01-02 00:00:00 UTC
            "commentary": "Second post",
            "content": {"media": {"id": "media:1"}},
            "visibility": "PUBLIC",
            "lifecycleState": "PUBLISHED",
        },
    ]
    mocker.patch.object(linkedin, "_fetch_paginated_posts", return_value=posts)

    df = linkedin.get_posts("12345", "2024-01-01", "2024-01-31")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]["content_type"] == "ARTICLE"
    assert df.iloc[1]["content_type"] == "MEDIA"


def test_get_posts_content_types(linkedin, mocker):
    """Content type detection covers all variants."""
    posts = [
        {"id": "a", "createdAt": 1704067200000, "content": {"multiImage": {}}},
        {"id": "b", "createdAt": 1704067200000, "content": {}},
        {"id": "c", "createdAt": 1704067200000},
    ]
    mocker.patch.object(linkedin, "_fetch_paginated_posts", return_value=posts)

    df = linkedin.get_posts("12345", "2024-01-01", "2024-01-31")
    assert df.iloc[0]["content_type"] == "MULTI_IMAGE"
    assert df.iloc[1]["content_type"] == "NONE"
    assert df.iloc[2]["content_type"] == "NONE"


def test_get_posts_date_parse_failure(linkedin, mocker):
    """Returns None when _fetch_paginated_posts returns None (bad dates)."""
    mocker.patch.object(linkedin, "_fetch_paginated_posts", return_value=None)

    result = linkedin.get_posts("12345", "invalid", "date")
    assert result is None


def test_get_posts_empty(linkedin, mocker):
    """Returns empty DataFrame when no posts in range."""
    mocker.patch.object(linkedin, "_fetch_paginated_posts", return_value=[])

    df = linkedin.get_posts("12345", "2024-01-01", "2024-01-31")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ------------------------------------------------------------------
# _fetch_paginated_posts
# ------------------------------------------------------------------


def test_fetch_paginated_posts_filters_by_date(linkedin, mocker):
    """Only posts within date range are collected."""
    page = {
        "elements": [
            {"createdAt": 1706745600000},  # 2024-02-01 — in range
            {"createdAt": 1704067200000},  # 2024-01-01 — before range, stop
        ]
    }
    mocker.patch.object(linkedin, "_request_get", return_value=page)

    result = linkedin._fetch_paginated_posts("123", "2024-01-15", "2024-02-15")
    assert len(result) == 1


def test_fetch_paginated_posts_skips_future_posts(linkedin, mocker):
    """Posts newer than end_date are skipped, paging continues."""
    page1 = {
        "elements": [
            {"createdAt": 1735689600000},  # 2025-01-01 — too new
        ]
    }
    page2 = {"elements": []}
    mocker.patch.object(linkedin, "_request_get", side_effect=[page1, page2])

    result = linkedin._fetch_paginated_posts("123", "2024-01-01", "2024-12-31")
    assert len(result) == 0


def test_fetch_paginated_posts_invalid_dates(linkedin):
    """Bad date format returns None."""
    result = linkedin._fetch_paginated_posts("123", "not-a-date", "2024-01-01")
    assert result is None


def test_fetch_paginated_posts_api_error_stops(linkedin, mocker):
    """API error during pagination stops and returns collected so far."""
    mocker.patch.object(
        linkedin,
        "_request_get",
        side_effect=requests.exceptions.HTTPError("500"),
    )
    result = linkedin._fetch_paginated_posts("123", "2024-01-01", "2024-01-31")
    assert result == []


# ------------------------------------------------------------------
# get_engagement
# ------------------------------------------------------------------


def test_get_engagement_success(linkedin, mocker):
    """Returns DataFrame with likes, comments, shares."""
    raw = {
        "urn:li:share:111": {
            "likesSummary": {"totalLikes": 10},
            "commentsSummary": {"totalFirstLevelComments": 3},
            "sharesSummary": {"totalShares": 1},
        }
    }
    mocker.patch.object(linkedin, "_get_engagement_raw", return_value=raw)

    df = linkedin.get_engagement(["urn:li:share:111"])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["likes"] == 10
    assert df.iloc[0]["comments"] == 3
    assert df.iloc[0]["shares"] == 1


def test_get_engagement_empty_urns(linkedin, mocker):
    """Empty URN list returns empty DataFrame."""
    mocker.patch.object(linkedin, "_get_engagement_raw", return_value={})

    df = linkedin.get_engagement([])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_engagement_api_failure(linkedin, mocker):
    """Returns None when raw method fails."""
    mocker.patch.object(linkedin, "_get_engagement_raw", return_value=None)
    assert linkedin.get_engagement(["urn:li:share:111"]) is None


def test_get_engagement_quota_exhausted(linkedin, mocker):
    """QuotaExhaustedError propagates from raw method."""
    from d2b_data.linkedin_organic import QuotaExhaustedError

    mocker.patch.object(
        linkedin,
        "_get_engagement_raw",
        side_effect=QuotaExhaustedError("quota hit"),
    )

    with pytest.raises(QuotaExhaustedError):
        linkedin.get_engagement(["urn:li:share:111"])


# ------------------------------------------------------------------
# _get_engagement_raw
# ------------------------------------------------------------------


def test_get_engagement_raw_individual_calls(linkedin, mocker):
    """Makes one API call per URN."""
    success = {"likesSummary": {"totalLikes": 5}}
    mocker.patch.object(linkedin, "_request_get", return_value=success)

    result = linkedin._get_engagement_raw(
        ["urn:li:share:1", "urn:li:share:2", "urn:li:share:3"]
    )
    assert len(result) == 3
    assert linkedin._request_get.call_count == 3


def test_get_engagement_raw_partial_failure(linkedin, mocker):
    """Continues on non-quota errors, skipping failed URNs."""
    success = {"likesSummary": {"totalLikes": 5}}
    mocker.patch.object(
        linkedin,
        "_request_get",
        side_effect=[
            success,
            requests.exceptions.HTTPError("404"),
            success,
        ],
    )

    result = linkedin._get_engagement_raw(
        ["urn:li:share:1", "urn:li:share:2", "urn:li:share:3"]
    )
    assert len(result) == 2


def test_get_engagement_raw_quota_stops_immediately(linkedin, mocker):
    """QuotaExhaustedError stops iteration."""
    from d2b_data.linkedin_organic import QuotaExhaustedError

    success = {"likesSummary": {"totalLikes": 5}}
    mocker.patch.object(
        linkedin,
        "_request_get",
        side_effect=[success, QuotaExhaustedError("quota")],
    )

    with pytest.raises(QuotaExhaustedError):
        linkedin._get_engagement_raw(["urn:li:share:1", "urn:li:share:2"])


# ------------------------------------------------------------------
# get_posts_with_engagement
# ------------------------------------------------------------------


def test_posts_with_engagement_merged(linkedin, mocker):
    """Posts and engagement are merged on post_urn."""
    df_posts = pd.DataFrame(
        {
            "post_urn": ["urn:1", "urn:2"],
            "created_at": ["2024-01-01", "2024-01-02"],
            "commentary": ["a", "b"],
            "content_type": ["ARTICLE", "MEDIA"],
            "visibility": ["PUBLIC", "PUBLIC"],
            "lifecycle_state": ["PUBLISHED", "PUBLISHED"],
        }
    )
    df_eng = pd.DataFrame(
        {
            "post_urn": ["urn:1", "urn:2"],
            "likes": [10, 20],
            "comments": [1, 2],
            "shares": [0, 1],
        }
    )
    mocker.patch.object(linkedin, "get_posts", return_value=df_posts)
    mocker.patch.object(linkedin, "get_engagement", return_value=df_eng)

    result = linkedin.get_posts_with_engagement("123", "2024-01-01", "2024-01-31")
    assert len(result) == 2
    assert "likes" in result.columns
    assert result.iloc[0]["likes"] == 10


def test_posts_with_engagement_quota_fallback(linkedin, mocker):
    """Quota error returns posts without engagement columns."""
    from d2b_data.linkedin_organic import QuotaExhaustedError

    df_posts = pd.DataFrame(
        {
            "post_urn": ["urn:1"],
            "created_at": ["2024-01-01"],
            "commentary": ["test"],
            "content_type": ["ARTICLE"],
            "visibility": ["PUBLIC"],
            "lifecycle_state": ["PUBLISHED"],
        }
    )
    mocker.patch.object(linkedin, "get_posts", return_value=df_posts)
    mocker.patch.object(
        linkedin,
        "get_engagement",
        side_effect=QuotaExhaustedError("quota"),
    )

    result = linkedin.get_posts_with_engagement("123", "2024-01-01", "2024-01-31")
    assert len(result) == 1
    assert "likes" not in result.columns


def test_posts_with_engagement_no_posts(linkedin, mocker):
    """Returns None when get_posts returns None."""
    mocker.patch.object(linkedin, "get_posts", return_value=None)
    assert linkedin.get_posts_with_engagement("123", "2024-01-01", "2024-01-31") is None


def test_posts_with_engagement_empty_posts(linkedin, mocker):
    """Returns empty DataFrame when no posts found."""
    mocker.patch.object(linkedin, "get_posts", return_value=pd.DataFrame())
    result = linkedin.get_posts_with_engagement("123", "2024-01-01", "2024-01-31")
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_posts_with_engagement_no_engagement_data(linkedin, mocker):
    """When engagement returns empty, returns posts only."""
    df_posts = pd.DataFrame(
        {
            "post_urn": ["urn:1"],
            "created_at": ["2024-01-01"],
            "commentary": ["test"],
            "content_type": ["ARTICLE"],
            "visibility": ["PUBLIC"],
            "lifecycle_state": ["PUBLISHED"],
        }
    )
    mocker.patch.object(linkedin, "get_posts", return_value=df_posts)
    mocker.patch.object(linkedin, "get_engagement", return_value=pd.DataFrame())

    result = linkedin.get_posts_with_engagement("123", "2024-01-01", "2024-01-31")
    assert len(result) == 1
    assert "likes" not in result.columns


# ------------------------------------------------------------------
# get_posts_with_engagement_safe (cached)
# ------------------------------------------------------------------


def test_safe_engagement_uses_cache(linkedin, mocker, tmp_path):
    """Cached URNs are not re-fetched from API."""
    import json

    cache_file = tmp_path / "cache.json"
    cache_data = {
        "urn:1": {
            "likesSummary": {"totalLikes": 5},
            "commentsSummary": {"totalFirstLevelComments": 1},
            "sharesSummary": {"totalShares": 0},
        }
    }
    cache_file.write_text(json.dumps(cache_data))

    df_posts = pd.DataFrame(
        {
            "post_urn": ["urn:1"],
            "created_at": ["2024-01-01"],
            "commentary": ["test"],
            "content_type": ["ARTICLE"],
            "visibility": ["PUBLIC"],
            "lifecycle_state": ["PUBLISHED"],
        }
    )
    mocker.patch.object(linkedin, "get_posts", return_value=df_posts)
    mock_raw = mocker.patch.object(linkedin, "_get_engagement_raw")

    result = linkedin.get_posts_with_engagement_safe(
        "123", "2024-01-01", "2024-01-31", cache_path=str(cache_file)
    )

    mock_raw.assert_not_called()
    assert result.iloc[0]["likes"] == 5


def test_safe_engagement_fetches_missing(linkedin, mocker, tmp_path):
    """URNs not in cache are fetched from API."""
    import json

    cache_file = tmp_path / "cache.json"
    cache_file.write_text("{}")

    df_posts = pd.DataFrame(
        {
            "post_urn": ["urn:1"],
            "created_at": ["2024-01-01"],
            "commentary": ["test"],
            "content_type": ["ARTICLE"],
            "visibility": ["PUBLIC"],
            "lifecycle_state": ["PUBLISHED"],
        }
    )
    raw_result = {
        "urn:1": {
            "likesSummary": {"totalLikes": 10},
            "commentsSummary": {"totalFirstLevelComments": 2},
            "sharesSummary": {"totalShares": 1},
        }
    }
    mocker.patch.object(linkedin, "get_posts", return_value=df_posts)
    mocker.patch.object(linkedin, "_get_engagement_raw", return_value=raw_result)

    result = linkedin.get_posts_with_engagement_safe(
        "123", "2024-01-01", "2024-01-31", cache_path=str(cache_file)
    )

    assert result.iloc[0]["likes"] == 10
    saved = json.loads(cache_file.read_text())
    assert "urn:1" in saved


def test_safe_engagement_quota_partial(linkedin, mocker, tmp_path):
    """Quota error mid-fetch returns partial results from cache."""
    from d2b_data.linkedin_organic import QuotaExhaustedError
    import json

    cache_file = tmp_path / "cache.json"
    cache_data = {
        "urn:1": {
            "likesSummary": {"totalLikes": 5},
            "commentsSummary": {"totalFirstLevelComments": 0},
            "sharesSummary": {"totalShares": 0},
        }
    }
    cache_file.write_text(json.dumps(cache_data))

    df_posts = pd.DataFrame(
        {
            "post_urn": ["urn:1", "urn:2"],
            "created_at": ["2024-01-01", "2024-01-02"],
            "commentary": ["a", "b"],
            "content_type": ["ARTICLE", "MEDIA"],
            "visibility": ["PUBLIC", "PUBLIC"],
            "lifecycle_state": ["PUBLISHED", "PUBLISHED"],
        }
    )
    mocker.patch.object(linkedin, "get_posts", return_value=df_posts)
    mocker.patch.object(
        linkedin,
        "_get_engagement_raw",
        side_effect=QuotaExhaustedError("quota"),
    )

    result = linkedin.get_posts_with_engagement_safe(
        "123", "2024-01-01", "2024-01-31", cache_path=str(cache_file)
    )

    assert len(result) == 2
    assert result.iloc[0]["likes"] == 5
    assert result.iloc[1]["likes"] == 0  # fillna


def test_safe_engagement_no_posts(linkedin, mocker, tmp_path):
    """Returns None when get_posts returns None."""
    mocker.patch.object(linkedin, "get_posts", return_value=None)
    result = linkedin.get_posts_with_engagement_safe(
        "123", "2024-01-01", "2024-01-31", cache_path=str(tmp_path / "c.json")
    )
    assert result is None
