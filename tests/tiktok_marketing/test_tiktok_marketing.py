from d2b_data.Tiktok_marketing import TikTokMarketing
from unittest.mock import MagicMock
from googleapiclient.errors import HttpError
import pytest

def test_instance_is_created_with_valid_token(tiktok):
    """When file exists and token is valid, header Access-Token must be setted"""
    
    assert tiktok.headers['Access-Token'] == "fake_token_123"

def test_instance_is_created_with_invalid_token(tiktok_false):
    """When file exists and token is not valid, token must be None"""
    
    assert tiktok_false.token is None

def test_instance_is_created_with_no_token(tiktok_no_file):
    """Instance with no token found in root"""

    assert tiktok_no_file.token is None
    assert tiktok_no_file.headers.get('Access-Token') is None