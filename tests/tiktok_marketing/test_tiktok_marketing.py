from d2b_data.Tiktok_marketing import TikTokMarketing
from unittest.mock import MagicMock
from googleapiclient.errors import HttpError
import pytest

def test_instance_is_created_with_valid_token(tiktok):
    """When file exists and token is valid, header Access-Token must be setted"""
    
    assert tiktok.headers['Access-Token'] == "fake_token_123"


def test_instance_is_created_with_no_token(tiktok_no_file):
    """Instance with no token found in root"""

    assert tiktok_no_file.token is None
    assert tiktok_no_file.headers.get('Access-Token') is None

def test_validate_connection_no_token(tiktok_no_file):
    """Instance with no token found"""

    assert tiktok_no_file.validate_connection() is False

def test_validate_connection_is_valid(tiktok, mocker):
    """Validates that the connection returns a valid response"""

    mocker.patch("d2b_data.Tiktok_marketing.TikTokMarketing._token_test_connection", return_value=True)

    assert tiktok.validate_connection() is True

def test_validate_connection_is_not_valid(tiktok, mocker):
    """When token is loaded but connection fails, should return False"""

    mocker.patch("d2b_data.Tiktok_marketing.TikTokMarketing._token_test_connection", return_value=False)

    assert tiktok.validate_connection() is False

def test_load_token_from_file_valid(tiktok_no_token, mocker):
    """Tests that token is correctly loaded"""
    fake_data = {
        "app_id": "fake_app_id",
        "secret": "fake_secret",
        "access_token": "fake_token_123"
    }
    mocker.patch("builtins.open", mocker.mock_open())
    mocker.patch("json.load", return_value=fake_data)

    result = tiktok_no_token._load_token_from_file()

    assert result == fake_data
    assert tiktok_no_token.token == "fake_token_123"

def test_load_token_from_file_incomplete_data(tiktok_no_token, mocker):
    "When token file is missing required fields, should return None"

    fake_data = {
        "app_id": "fake_app_id"
    }
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

    assert result['code'] == 0

def test_get_report_raw_429_backoff(tiktok, mocker):
    """Testing for the 429 error handling and exponential backoff"""
    
    fake_response = MagicMock()
    fake_response.status_code = 429
    mocker.patch("d2b_data.Tiktok_marketing.requests.get", side_effect=[fake_response, 
                                                                       fake_response, 
                                                                       fake_response,
                                                                       fake_response,
                                                                       fake_response])
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
    """Errors everywhere"""
    mocker.patch("d2b_data.Tiktok_marketing.requests.get", side_effect=Exception("Error Message"))

    result = tiktok._get_report_raw(params={})

    assert result is None