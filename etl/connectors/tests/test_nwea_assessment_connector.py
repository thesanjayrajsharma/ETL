"""
Unit tests for NWEA Assessment Connector
"""
import pytest
from unittest.mock import patch, MagicMock
from etl.connectors.nwea import NWEAAssessmentConnector

import pandas as pd

# TO DO:  update tests to match actual API response -- don't work right now

# Fixture for a successful API response
@pytest.fixture
def mock_successful_response():
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "testResults": [
            {"testResultBid": 1, 
             "modifiedDateTime": "2024-01-01T00:00:00", 
            },
            {"testResultBid": 2, 
             "modifiedDateTime": "2024-02-01T00:00:00", 
            }
        ],
        "pagination": {"hasNextPage": False}
    }
    mock_response.status_code = 200
    return mock_response

# Fixture for a failed API response
@pytest.fixture
def mock_failed_response():
    mock_response = MagicMock()
    mock_response.status_code = 500  # Simulate a server error
    return mock_response

# Fixture for a successful authentication response
@pytest.fixture
def mock_auth_response():
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "test_token"}
    return mock_response

@patch('src.connectors.nwea.requests.post')
@patch('src.connectors.nwea.requests.get')
def test_pull_data_success(mock_get, mock_post, mock_successful_response, mock_auth_response):
    mock_post.return_value = mock_auth_response
    mock_get.return_value = mock_successful_response

    connector = NWEAAssessmentConnector(
        url="http://example.api.com/data",
        params={},
        bid="test_bid",
        max_date = pd.to_datetime("2023-09-23")
    )

    connector.pull_data()

    # TO DO:  add assertions to test different edge cases
    assert len(connector.api_results_) == 2
    assert type(connector.api_results_) == list
    assert type(connector.pulled_data_) == pd.DataFrame

@patch('src.connectors.nwea.requests.post')
@patch('src.connectors.nwea.requests.get')
def test_pull_data_failure(mock_get, mock_post, mock_failed_response, mock_auth_response):
    mock_post.return_value = mock_auth_response
    mock_get.return_value = mock_failed_response

    connector = NWEAAssessmentConnector(
        url="http://example.api.com/data",
        params={},
        bid="test_bid"
    )

    with pytest.raises(ValueError):
        connector.pull_data()