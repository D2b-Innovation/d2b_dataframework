import pytest

from d2b_data.FacebookOrganic import FacebookOrganic


@pytest.fixture
def fb():
    return FacebookOrganic(access_token="fake_token")


@pytest.fixture
def metrics():
    return ["post_impressions", "post_engaged_users", "post_clicks"]
