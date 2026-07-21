import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from src.project_pipeline.exceptions import LoadError
from src.project_pipeline.load import S3JsonLoader


@pytest.fixture
def mock_config():
    """Provide the minimum configuration required by S3JsonLoader."""

    config = Mock()
    config.aws_region = "eu-central-1"
    config.aws_bucket_name = "aviation-platform-test"
    config.s3_raw_prefix = "raw/opensky/states"

    return config


@pytest.fixture
def sample_payload():
    """Provide a representative raw OpenSky payload."""

    return {
        "time": 1750000000,
        "states": [
            [
                "abc123",
                "TEST123",
                "Germany",
            ]
        ],
    }


def test_load_to_s3_success(mock_config, sample_payload):
    """Upload JSON using the expected time-partitioned S3 key."""

    fixed_time = datetime(
        2026,
        7,
        21,
        10,
        30,
        45,
        tzinfo=timezone.utc,
    )

    mock_s3_client = Mock()

    with patch(
        "src.project_pipeline.load.boto3.client",
        return_value=mock_s3_client,
    ) as mock_boto_client, patch("src.project_pipeline.load.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_time

        loader = S3JsonLoader(mock_config)

        result = loader.load_to_s3(sample_payload)

    expected_key = (
        "raw/opensky/states/"
        "year=2026/"
        "month=07/"
        "day=21/"
        "hour=10/"
        "20260721T103045Z.json"
    )

    assert result == expected_key

    mock_boto_client.assert_called_once_with(
        "s3",
        region_name="eu-central-1",
    )

    mock_s3_client.put_object.assert_called_once_with(
        Bucket="aviation-platform-test",
        Key=expected_key,
        Body=json.dumps(sample_payload),
        ContentType="application/json",
    )


def test_load_to_s3_raises_load_error_on_client_error(
    mock_config,
    sample_payload,
):
    """Convert AWS ClientError into the project's LoadError."""

    aws_error = ClientError(
        error_response={
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access denied",
            }
        },
        operation_name="PutObject",
    )

    mock_s3_client = Mock()
    mock_s3_client.put_object.side_effect = aws_error

    with patch(
        "src.project_pipeline.load.boto3.client",
        return_value=mock_s3_client,
    ):
        loader = S3JsonLoader(mock_config)

        with pytest.raises(
            LoadError,
            match="Failed to load raw OpenSky data to S3",
        ):
            loader.load_to_s3(sample_payload)
