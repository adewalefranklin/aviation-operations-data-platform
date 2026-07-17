import requests

from src.project_pipeline.authentication import OpenSkyAuthenticator
from src.project_pipeline.config import Config
from src.project_pipeline.exceptions import ExtractError
from src.project_pipeline.logger import get_logger


class OpenSkyExtractor:
    """Handles data extraction from the OpenSky API."""

    def __init__(self, config: Config):
        self.config = config
        self.authenticator = OpenSkyAuthenticator(config)
        self.logger = get_logger(__name__)

    def extract_states(self) -> dict:
        """Extract the current aircraft state vectors from OpenSky."""

        self.logger.info("Starting data extraction from OpenSky API...")

        access_token = self.authenticator.get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        url = f"{self.config.opensky_base_url}/states/all"

        try:
            response = requests.get(
                url=url,
                headers=headers,
                timeout=30,
            )

            response.raise_for_status()

            data = response.json()

            self.logger.info("OpenSky aircraft-state extraction successful.")

            return data

        except requests.RequestException as e:
            self.logger.error(
                "Failed to extract aircraft states from OpenSky: %s",
                e,
            )

            raise ExtractError(
                "Failed to extract aircraft states from OpenSky API."
            ) from e
