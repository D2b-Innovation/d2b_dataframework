import pytest


@pytest.fixture
def linkedin(mocker):
    """LinkedinOrganic with a valid token loaded from file."""
    fake_data = {"access_token": "fake_token_123"}
    mocker.patch("builtins.open", mocker.mock_open())
    mocker.patch("json.load", return_value=fake_data)

    from d2b_data.linkedin_organic import LinkedinOrganic

    return LinkedinOrganic(token_path="fake_token.json")


@pytest.fixture
def linkedin_no_file():
    """LinkedinOrganic with no token file specified."""
    from d2b_data.linkedin_organic import LinkedinOrganic

    return LinkedinOrganic()


@pytest.fixture
def linkedin_bad_file(mocker):
    """LinkedinOrganic where token file exists but is invalid."""
    mocker.patch("builtins.open", side_effect=Exception("File not found"))

    from d2b_data.linkedin_organic import LinkedinOrganic

    return LinkedinOrganic(token_path="bad_token.json")
