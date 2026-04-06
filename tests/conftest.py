import pytest
from unittest.mock import MagicMock

@pytest.fixture
def ga4(mocker):
    # Interceptamos Google_Token_MNG antes de que __init__ lo llame
    mock_token_mng = MagicMock()
    mock_token_mng.get_service.return_value = MagicMock()

    mocker.patch(
        "d2b_data.Google_GA4.d2b_data.Google_Token_MNG.Google_Token_MNG",
        return_value=mock_token_mng
    )

    from d2b_data.Google_GA4 import Google_GA4

    return Google_GA4(
        client_secret="fake_secret.json",
        token_json="fake_token.json",
        debug=False
    )