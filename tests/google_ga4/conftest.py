import pytest
from unittest.mock import MagicMock

@pytest.fixture
def ga4(mocker):
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

@pytest.fixture
def df_fake():
    import pandas as pd
    return pd.DataFrame({
        "date": ["2024-01-01"],
        "city": ["Santiago"],
        "sessions": ["150"]
    })

@pytest.fixture
def raw_response_with_sampling():
    return {
        "reports": [{
            "dimensionHeaders": [{"name": "date"}],
            "metricHeaders": [{"name": "sessions"}],
            "rows": [
                {
                    "dimensionValues": [{"value": "2024-01-01"}],
                    "metricValues": [{"value": "150"}]
                }
            ],
            "metadata": {
                "samplingMetadatas": [
                    {"samplesReadCounts": "500", "samplingSpaceSizes": "1000"}
                ]
            }
        }]
    }

@pytest.fixture
def df_fake_with_sampling():
    import pandas as pd
    return pd.DataFrame({
        "date": ["2024-01-01"],
        "sessions": ["150"],
        "samplesReadCounts": [500],
        "samplingSpaceSizes": [1000],
        "sampling_percentage": [50.0],
        "sampled": [True],
        "dataLossFromOtherRow": [False]
    })