import pytest
from unittest.mock import MagicMock

@pytest.fixture
def tiktok(mocker):
    def fake_load_token(self):
        self.token = "fake_token_123"
        self.app_id = "fake_app_id"
        self.secret = "fake_secret"

    mocker.patch("os.path.isfile", return_value=True)
    mocker.patch("d2b_data.Tiktok_marketing.TikTokMarketing._load_token_from_file", fake_load_token)

    from d2b_data.Tiktok_marketing import TikTokMarketing
    return TikTokMarketing(token_path="fake_token.json")

@pytest.fixture
def tiktok_false(mocker):
    def fake_load_token(self):
        self.token = "fake_token_123"
        self.app_id = "fake_app_id"
        self.secret = "fake_secret"

    mocker.patch("os.path.isfile", return_value=True)
    mocker.patch("d2b_data.Tiktok_marketing.TikTokMarketing._load_token_from_file", fake_load_token)

    from d2b_data.Tiktok_marketing import TikTokMarketing
    return TikTokMarketing(token_path="fake_token.json")

@pytest.fixture
def tiktok_no_file(mocker):
    mocker.patch("os.path.isfile", return_value=False)

    from d2b_data.Tiktok_marketing import TikTokMarketing
    return TikTokMarketing()

@pytest.fixture
def tiktok_no_token(mocker):
    mocker.patch("os.path.isfile", return_value=False)

    from d2b_data.Tiktok_marketing import TikTokMarketing
    return TikTokMarketing(token_path="fake_token.json")