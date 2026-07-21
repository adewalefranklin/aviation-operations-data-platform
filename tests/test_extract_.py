from unittest.mock import Mock, patch

import pytest
import requests

from src.project_pipeline.exceptions import ExtractError
from src.project_pipeline.extract import OpenSkyExtractor


@pytest.fixture
def mock_config():
    """Provide the minimum configuration required by OpenSkyExtractor."""

    config = Mock()
    config.opensky_base_url = "https://opensky.example.com/api"

    return config


def test_extract_states_success(mock_config):
    """Return decoded OpenSky data when the API request succeeds."""

    expected_data = {
        "time": 1750000000,
        "states": [
            [
                "abc123",
                "TEST123",
                "Germany",
            ]
        ],
    }

    mock_response = Mock()
    mock_response.json.return_value = expected_data
    mock_response.raise_for_status.return_value = None

    with patch(
        "src.project_pipeline.extract.OpenSkyAuthenticator.get_access_token",
        return_value="test-access-token",
    ) as mock_get_token, patch(
        "src.project_pipeline.extract.requests.get",
        return_value=mock_response,
    ) as mock_requests_get:
        extractor = OpenSkyExtractor(mock_config)

        result = extractor.extract_states()

    assert result == expected_data

    mock_get_token.assert_called_once_with()

    mock_requests_get.assert_called_once_with(
        url="https://opensky.example.com/api/states/all",
        headers={
            "Authorization": "Bearer test-access-token",
        },
        timeout=30,
    )

    mock_response.raise_for_status.assert_called_once_with()
    mock_response.json.assert_called_once_with()


def test_extract_states_raises_extract_error_when_request_fails(mock_config):
    """Convert requests exceptions into the project's ExtractError."""

    request_error = requests.RequestException("OpenSky unavailable")

    with patch(
        "src.project_pipeline.extract.OpenSkyAuthenticator.get_access_token",
        return_value="test-access-token",
    ), patch(
        "src.project_pipeline.extract.requests.get",
        side_effect=request_error,
    ):
        extractor = OpenSkyExtractor(mock_config)

        with pytest.raises(
            ExtractError,
            match="Failed to extract aircraft states from OpenSky API",
        ):
            extractor.extract_states()


def test_extract_states_raises_extract_error_for_http_error(mock_config):
    """Raise ExtractError when OpenSky returns an unsuccessful HTTP status."""

    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("401 Client Error")

    with patch(
        "src.project_pipeline.extract.OpenSkyAuthenticator.get_access_token",
        return_value="invalid-token",
    ), patch(
        "src.project_pipeline.extract.requests.get",
        return_value=mock_response,
    ):
        extractor = OpenSkyExtractor(mock_config)

        with pytest.raises(ExtractError):
            extractor.extract_states()
