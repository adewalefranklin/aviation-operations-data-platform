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
