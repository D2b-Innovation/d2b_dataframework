from unittest.mock import MagicMock

import pytest


def test_instance_is_created_with_valid_token(tiktok):
    """When file exists and token is valid, header Access-Token must be setted"""

    assert tiktok.headers["Access-Token"] == "fake_token_123"


def test_instance_is_created_with_no_token_found(tiktok_no_file):
    """Instance with no token found in root"""

    assert tiktok_no_file.token is None
    assert tiktok_no_file.headers.get("Access-Token") is None


def test_validate_connection_no_token(tiktok_no_file):
    """Instance with no token found"""

    assert tiktok_no_file.validate_connection() is False


def test_validate_connection_is_valid(tiktok, mocker):
    """Validates that the connection returns a valid response"""

    mocker.patch(
        "d2b_data.Tiktok_marketing.TikTokMarketing._token_test_connection",
        return_value=True,
    )

    assert tiktok.validate_connection() is True


def test_validate_connection_is_not_valid(tiktok, mocker):
    """When token is loaded but connection fails, should return False"""

    mocker.patch(
        "d2b_data.Tiktok_marketing.TikTokMarketing._token_test_connection",
        return_value=False,
    )

    assert tiktok.validate_connection() is False


def test_load_token_from_file_valid(tiktok_no_token, mocker):
    """Tests that token is correctly loaded"""
    fake_data = {
        "app_id": "fake_app_id",
        "secret": "fake_secret",
        "access_token": "fake_token_123",
    }
    mocker.patch("builtins.open", mocker.mock_open())
    mocker.patch("json.load", return_value=fake_data)

    result = tiktok_no_token._load_token_from_file()

    assert result == fake_data
    assert tiktok_no_token.token == "fake_token_123"


def test_load_token_from_file_incomplete_data(tiktok_no_token, mocker):
    "When token file is missing required fields, should return None"

    fake_data = {"app_id": "fake_app_id"}
    mocker.patch("builtins.open", mocker.mock_open())
    mocker.patch("json.load", return_value=fake_data)

    result = tiktok_no_token._load_token_from_file()

    assert result is None


def test_load_token_from_file_invalid_json(tiktok_no_token, mocker):
    """When token file contains invalid JSON, should return None"""

    mocker.patch("builtins.open", side_effect=Exception("File read error"))

    result = tiktok_no_token._load_token_from_file()

    assert result is None


def test_get_report_raw_success(tiktok, mocker):
    """Verifies the first answer is correct with no retries"""

    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {"code": 0, "data": {"list": []}}

    mocker.patch("d2b_data.Tiktok_marketing.requests.get", return_value=fake_response)
    result = tiktok._get_report_raw(params={})

    assert result["code"] == 0


def test_get_report_raw_429_backoff(tiktok, mocker):
    """Testing for the 429 error handling and exponential backoff"""

    fake_response = MagicMock()
    fake_response.status_code = 429
    mocker.patch(
        "d2b_data.Tiktok_marketing.requests.get",
        side_effect=[
            fake_response,
            fake_response,
            fake_response,
            fake_response,
            fake_response,
        ],
    )
    mock_sleep = mocker.patch("time.sleep")
    result = tiktok._get_report_raw(params={})

    assert mock_sleep.call_count == 5
    assert result is None


def test_get_report_error_code_tiktok(tiktok, mocker):
    """Get report handles correctly the error code from TikTok API"""

    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {"code": 40001, "message": "Some error"}
    mocker.patch("d2b_data.Tiktok_marketing.requests.get", return_value=fake_response)

    result = tiktok._get_report_raw(params={})

    assert result is None


def test_get_report_raw_exception(tiktok, mocker):
    """Tests the correct implementation on error handling during extraction"""
    mocker.patch(
        "d2b_data.Tiktok_marketing.requests.get", side_effect=Exception("Error Message")
    )

    result = tiktok._get_report_raw(params={})

    assert result is None


def test_get_report_dataframe_no_date_dimension_365_plus_days_error(tiktok):
    """Checks that user can't query more than 365 days without stat_time_day mode"""

    start_date = "2025-01-01"
    end_date = "2026-02-01"
    dimensions = ["ad_id"]
    metrics = ["spend", "impressions", "clicks"]
    advertiser_id = "user/12345"

    with pytest.raises(ValueError):
        tiktok.get_report_dataframe(
            advertiser_id, start_date, end_date, dimensions=dimensions, metrics=metrics
        )


def test_get_report_dataframe_no_stat_time_day_pagination(tiktok, mocker):
    """When response has multiple pages, should concatenate all records into one DataFrame"""
    fake_page_1 = {
        "code": 0,
        "data": {
            "list": [{"dimensions": {"ad_id": "1"}, "metrics": {"spend": "10"}}],
            "page_info": {"total_page": 2},
        },
    }
    fake_page_2 = {
        "code": 0,
        "data": {
            "list": [{"dimensions": {"ad_id": "2"}, "metrics": {"spend": "20"}}],
            "page_info": {"total_page": 2},
        },
    }

    mocker.patch.object(
        tiktok, "_get_report_raw", side_effect=[fake_page_1, fake_page_2]
    )

    result = tiktok.get_report_dataframe(
        "123", "2025-01-01", "2025-02-01", dimensions=["ad_id"], metrics=["spend"]
    )

    assert len(result) == 2


def test_get_report_dataframe_no_stat_time_day_none_response_raises_error(
    tiktok, mocker
):
    """When _get_report_raw returns None, should raise RuntimeError"""
    mocker.patch.object(tiktok, "_get_report_raw", return_value=None)

    with pytest.raises(RuntimeError):
        tiktok.get_report_dataframe(
            "123", "2025-01-01", "2025-02-01", dimensions=["ad_id"], metrics=["spend"]
        )


def test_get_report_dataframe_no_stat_time_day_no_data_returns_empty_dataframe(
    tiktok, mocker
):
    """When response has no list, should return empty DataFrame"""
    mocker.patch.object(tiktok, "_get_report_raw", return_value={"code": 0, "data": {}})

    result = tiktok.get_report_dataframe(
        "123", "2025-01-01", "2025-02-01", dimensions=["ad_id"], metrics=["spend"]
    )

    assert result.empty


def test_get_report_dataframe_no_stat_time_day_returns_dataframe(tiktok, mocker):
    """When response has data, should return a properly structured DataFrame"""
    fake_response = {
        "code": 0,
        "data": {
            "list": [{"dimensions": {"ad_id": "1"}, "metrics": {"spend": "10"}}],
            "page_info": {"total_page": 1},
        },
    }

    mocker.patch.object(tiktok, "_get_report_raw", return_value=fake_response)

    result = tiktok.get_report_dataframe(
        "123", "2025-01-01", "2025-02-01", dimensions=["ad_id"], metrics=["spend"]
    )

    assert not result.empty
    assert "ad_id" in result.columns
    assert "spend" in result.columns


def test_get_report_dataframe_stat_time_day_returns_dataframe(tiktok, mocker):
    """When stat_time_day is in dimensions, should return sorted DataFrame"""
    fake_response = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"ad_id": "2", "stat_time_day": "2025-01-02"},
                    "metrics": {"spend": "20"},
                },
                {
                    "dimensions": {"ad_id": "1", "stat_time_day": "2025-01-01"},
                    "metrics": {"spend": "10"},
                },
            ],
            "page_info": {"total_page": 1},
        },
    }

    mocker.patch.object(tiktok, "_get_report_raw", return_value=fake_response)

    result = tiktok.get_report_dataframe(
        "123",
        "2025-01-01",
        "2025-01-30",
        dimensions=["ad_id", "stat_time_day"],
        metrics=["spend"],
    )

    assert not result.empty
    assert "ad_id" in result.columns
    assert "stat_time_day" in result.columns
    assert result.iloc[0]["ad_id"] == "1"


def test_get_report_dataframe_stat_time_day_none_response_raises_error(tiktok, mocker):
    """When _get_report_raw returns None in stat_time_day mode, should raise RuntimeError"""
    mocker.patch.object(tiktok, "_get_report_raw", return_value=None)

    with pytest.raises(RuntimeError):
        tiktok.get_report_dataframe(
            "123",
            "2025-01-01",
            "2025-01-30",
            dimensions=["ad_id", "stat_time_day"],
            metrics=["spend"],
        )


def test_get_report_dataframe_stat_time_day_no_data_returns_empty_dataframe(
    tiktok, mocker
):
    """When response has no list in stat_time_day mode, should return empty DataFrame"""
    mocker.patch.object(tiktok, "_get_report_raw", return_value={"code": 0, "data": {}})

    result = tiktok.get_report_dataframe(
        "123",
        "2025-01-01",
        "2025-01-30",
        dimensions=["ad_id", "stat_time_day"],
        metrics=["spend"],
    )

    assert result.empty


def test_get_report_dataframe_stat_time_day_chunks_dates(tiktok, mocker):
    """When date range exceeds 30 days, should make multiple requests"""
    fake_response = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"ad_id": "1", "stat_time_day": "2025-01-01"},
                    "metrics": {"spend": "10"},
                }
            ],
            "page_info": {"total_page": 1},
        },
    }

    mock_raw = mocker.patch.object(
        tiktok, "_get_report_raw", return_value=fake_response
    )

    tiktok.get_report_dataframe(
        "123",
        "2025-01-01",
        "2025-03-01",
        dimensions=["ad_id", "stat_time_day"],
        metrics=["spend"],
    )

    assert mock_raw.call_count > 1


def test_get_report_json_no_dates_direct_call(tiktok, mocker):
    """When no start_date or end_date in params, should make a direct request"""
    fake_response = {
        "code": 0,
        "data": {"list": [{"ad_id": "1"}], "page_info": {"total_page": 1}},
    }
    mock_raw = mocker.patch.object(
        tiktok, "_get_report_raw", return_value=fake_response
    )

    result = tiktok.get_report_json(params={"advertiser_id": "123"})

    assert mock_raw.call_count == 1
    assert result == fake_response


def test_get_report_json_with_dates_chunks_requests(tiktok, mocker):
    """When date range exceeds 30 days, should make multiple chunked requests"""
    fake_response = {
        "code": 0,
        "data": {"list": [{"ad_id": "1"}], "page_info": {"total_page": 1}},
    }
    mock_raw = mocker.patch.object(
        tiktok, "_get_report_raw", return_value=fake_response
    )

    tiktok.get_report_json(
        params={
            "advertiser_id": "123",
            "start_date": "2025-01-01",
            "end_date": "2025-03-01",
        }
    )

    assert mock_raw.call_count > 1


def test_get_report_json_no_data_returns_empty_dict(tiktok, mocker):
    """When no data found in any chunk, should return empty dict"""
    mocker.patch.object(tiktok, "_get_report_raw", return_value={"code": 0, "data": {}})

    result = tiktok.get_report_json(
        params={
            "advertiser_id": "123",
            "start_date": "2025-01-01",
            "end_date": "2025-01-30",
        }
    )

    assert result == {}


def test_get_report_json_with_data_returns_consolidated_dict(tiktok, mocker):
    """When data found across chunks, should return consolidated dict with all records"""
    fake_response = {
        "code": 0,
        "data": {
            "list": [{"ad_id": "1"}, {"ad_id": "2"}],
            "page_info": {"total_page": 1},
        },
    }
    mocker.patch.object(tiktok, "_get_report_raw", return_value=fake_response)

    result = tiktok.get_report_json(
        params={
            "advertiser_id": "123",
            "start_date": "2025-01-01",
            "end_date": "2025-01-30",
        }
    )

    assert "data" in result
    assert "list" in result["data"]
    assert len(result["data"]["list"]) > 0
