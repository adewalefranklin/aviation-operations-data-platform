import requests

from src.project_pipeline.config import Config
from src.project_pipeline.exceptions import AuthenticationError
from src.project_pipeline.logger import get_logger


logger = get_logger(__name__)


class OpenSkyAuthenticator:
    """Handles OAuth2 authentication with the OpenSky API."""

    def __init__(self, config: Config):
        self.config = config

    def get_access_token(self) -> str:
            logger.info("Requesting OpenSky access token...")

            payload = {
                "grant_type": "client_credentials",
                "client_id": self.config.opensky_client_id,
                "client_secret": self.config.opensky_client_secret,
            }

            try:
                response = requests.post(
                    url=self.config.opensky_token_url,
                    data=payload,
                    timeout=30,
                )

                response.raise_for_status()

                token_data = response.json()
                access_token = token_data.get("access_token")

                if not access_token:
                    raise AuthenticationError(
                        "OpenSky authentication response did not contain an access token."
                    )

                logger.info("OpenSky access token obtained successfully.")

                return access_token

            except requests.RequestException as error:
                logger.error("Failed to authenticate with OpenSky: %s", error)

                raise AuthenticationError(
                    "Failed to obtain an OpenSky access token."
                ) from error