from d2b_data.Google_GA4 import Google_GA4

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

