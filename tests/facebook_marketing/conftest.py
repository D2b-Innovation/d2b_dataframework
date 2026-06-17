import sys
import pytest
from unittest.mock import MagicMock, patch

# google-cloud-storage is not required for Facebook_Marketing logic — mock it
# at the sys.modules level before the module is imported so the import doesn't fail.
_gcs_mock = MagicMock()
sys.modules.setdefault("google.cloud.storage", _gcs_mock)
sys.modules.setdefault("google.cloud", MagicMock(storage=_gcs_mock))


@pytest.fixture
def fb(mocker):
    mocker.patch("facebook_business.api.FacebookAdsApi.init")
    from d2b_data.Facebook_Marketing import Facebook_Marketing
    return Facebook_Marketing(
        app_id="fake_app_id",
        app_secret="fake_secret",
        access_token="fake_token",
        id_account="123456789",
    )


@pytest.fixture
def base_params():
    return {
        "level": "campaign",
        "fields": ["impressions", "clicks", "spend"],
        "breakdowns": [],
        "time_range": {"since": "2024-01-01", "until": "2024-01-31"},
    }
