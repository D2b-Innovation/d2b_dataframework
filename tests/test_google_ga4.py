from d2b_data.Google_GA4 import Google_GA4
from unittest.mock import MagicMock
from googleapiclient.errors import HttpError
import pytest

def test_instance_is_created_correctly(ga4):
    """Verifies that the object is created correctly"""
    assert ga4.auto_paginate == True
    assert ga4.debug_status == False
    assert ga4.intraday_limit == 30 * 100000

def test_extract_sampling_info_no_metadata(ga4):
    """Verifies the correct reading of an object that doesn't have metadata"""
    report_data = {}

    # 2. Llama al método
    result = ga4._extract_sampling_info(report_data)

    # 3. Verifica lo que esperas
    assert result['sampled'] == False
    assert result['sampling_percentage'] == 100.0
    assert result['dataLossFromOtherRow'] == False

def test_extract_sampling_info_with_metadata(ga4):
    """Verifies that the object comes with metadata"""

    report_data = {
    "metadata": {
        "samplingMetadatas": [
            {"samplesReadCount": "500", "samplingSpaceSize": "1000"}
        ]
     }
    }
    
    result = ga4._extract_sampling_info(report_data) 

    assert result['sampled'] == True
    assert result['samplesReadCounts'] == 500
    assert result['samplingSpaceSizes'] == 1000
    assert result['sampling_percentage'] == 50.0

def test_to_dataframe_full_data(ga4):
    """Verifies correct implementation when there's full data returned"""
    raw_response = {
    "reports": [{
        "dimensionHeaders": [{"name": "date"}, {"name": "city"}],
        "metricHeaders": [{"name": "sessions"}],
        "rows": [
            {
                "dimensionValues": [{"value": "2024-01-01"}, {"value": "Santiago"}],
                "metricValues": [{"value": "150"}]
            } 
          ]
        }]
    }

    result = ga4._to_df(raw_response)
    assert list(result.columns) == ['date', 'city', 'sessions']
    assert result.iloc[0]['date'] == "2024-01-01"
    assert result.iloc[0]['city'] == "Santiago"
    assert result.iloc[0]['sessions'] == "150"
    
def test_to_dataframe_no_data(ga4):
    """Verifies correct implementation when there's no data returned"""
    raw_response = {
    "reports": []
    }

    result = ga4._to_df(raw_response)

    assert result.empty == True

def test_to_dataframe_no_rows(ga4):
    """Verifies implementation when the DataFrame returns with no rows"""

    raw_response = {
    "reports": [{
        "dimensionHeaders": [{"name": "date"}, {"name": "city"}],
        "metricHeaders": [{"name": "sessions"}],
        "rows": []
        }]
    }

    result = ga4._to_df(raw_response)
    assert len(result) == 0

def test_get_report_raw_no_errors(ga4):
    """Verifies the first answer is correct with no retries"""
    raw_response = {"reports": [{
        "dimensionHeaders": [{"name": "date"}, {"name": "city"}],
        "metricHeaders": [{"name": "sessions"}],
        "rows": [
            {
                "dimensionValues": [{"value": "2024-01-01"}, {"value": "Santiago"}],
                "metricValues": [{"value": "150"}]
            } 
          ]
        }]
    }

    mock_execute = MagicMock(return_value=raw_response)
    mock_batch = MagicMock(execute=mock_execute)
    mock_properties = MagicMock(
        return_value=MagicMock(
            batchRunReports=MagicMock(return_value=mock_batch)
        )
    )
    
    ga4.service.properties = mock_properties

    result = ga4._get_report_raw("properties/123", {"requests": [{}]})
    assert result == raw_response

def test_get_report_429_backoff(ga4, mocker):
    """Testing for the 429 error handling and exponential backoff"""
    
    fake_resp = MagicMock()
    fake_resp.status = 429
    http_error = HttpError(resp=fake_resp, content=b"Too Many Requests")
    mock_execute = MagicMock(side_effect=http_error)
    mock_batch = MagicMock(execute=mock_execute)
    mock_properties = MagicMock(
        return_value=MagicMock(
            batchRunReports=MagicMock(return_value=mock_batch)
        )
    )
    ga4.service.properties = mock_properties

    mock_sleep = mocker.patch("time.sleep")

    with pytest.raises(HttpError):
        ga4._get_report_raw("properties/123", {"requests": [{}]})

    assert mock_sleep.call_count == 5



