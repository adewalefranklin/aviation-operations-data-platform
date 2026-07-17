import json
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.project_pipeline.config import Config
from src.project_pipeline.exceptions import LoadError
from src.project_pipeline.logger import get_logger


class S3JsonLoader:
    """Loads raw OpenSky API payloads into Amazon S3 as JSON."""

    def __init__(self, config: Config):
        self.logger = get_logger(__name__)
        self.config = config

        self.s3_client = boto3.client(
            "s3",
            region_name=self.config.aws_region,
        )

        self.bucket_name = self.config.aws_bucket_name
        self.s3_raw_prefix = self.config.s3_raw_prefix

    def load_to_s3(self, data: dict) -> str:
        """Load the raw OpenSky API payload into Amazon S3."""

        self.logger.info("Starting load of raw OpenSky data to S3...")

        now = datetime.now(timezone.utc)

        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")
        timestamp = now.strftime("%Y%m%dT%H%M%SZ")

        s3_key = (
            f"{self.s3_raw_prefix}/"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"hour={hour}/"
            f"{timestamp}.json"
        )

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType="application/json",
            )

            self.logger.info(
                "Successfully loaded raw OpenSky data to S3 at %s.",
                s3_key,
            )

            return s3_key

        except (BotoCoreError, ClientError) as error:
            self.logger.error(
                "Failed to load raw OpenSky data to S3: %s",
                error,
            )

            raise LoadError("Failed to load raw OpenSky data to S3.") from error
