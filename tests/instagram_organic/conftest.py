import pytest

from d2b_data.InstagramOrganic import InstagramOrganic


@pytest.fixture
def ig():
    return InstagramOrganic(access_token="fake_token")


@pytest.fixture
def feed_metrics():
    return ["impressions", "reach", "likes"]


@pytest.fixture
def story_metrics():
    return ["impressions", "reach"]
