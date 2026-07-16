import os

from dotenv import load_dotenv

from src.project_pipeline.exceptions import ConfigError

load_dotenv()


class Config:
    def __init__(self):
        self.opensky_client_id = os.getenv("OPENSKY_CLIENT_ID")
        self.opensky_client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
        self.opensky_token_url = os.getenv("OPENSKY_TOKEN_URL")
        self.opensky_base_url = os.getenv("OPENSKY_BASE_URL")
        self.s3_raw_prefix = os.getenv("S3_RAW_PREFIX", "raw/opensky")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_REGION")
        self.aws_bucket_name = os.getenv("AWS_BUCKET_NAME")

        if not self.opensky_client_id or not self.opensky_client_secret:
            raise ConfigError(
                "OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET must be set in the environment variables."
            )
        if not self.opensky_base_url:
            raise ConfigError(
                "OPENSKY_BASE_URL must be set in the environment variables."
            )
        if not self.opensky_token_url:
            raise ConfigError(
                "OPENSKY_TOKEN_URL must be set in the environment variables."
            )
        if not self.s3_raw_prefix:
            raise ConfigError("S3_RAW_PREFIX must be set in the environment variables.")
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise ConfigError(
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment variables."
            )
        if not self.aws_region:
            raise ConfigError("AWS_REGION must be set in the environment variables.")
        if not self.aws_bucket_name:
            raise ConfigError("AWS_BUCKET_NAME must be set in the environment variables.")
        
