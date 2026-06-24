import pytest
from unittest.mock import MagicMock


@pytest.fixture
def yt(mocker):
    mocker.patch(
        "d2b_data.youtube_public.build",
        return_value=MagicMock(),
    )
    from d2b_data.youtube_public import YouTubePublic

    return YouTubePublic(api_key="fake_api_key")
