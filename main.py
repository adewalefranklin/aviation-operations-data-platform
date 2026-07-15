from src.project_pipeline.authentication import OpenSkyAuthenticator
from src.project_pipeline.config import Config
from src.project_pipeline.exceptions import PipelineError
from src.project_pipeline.logger import get_logger


logger = get_logger(__name__)


def main() -> None:
    try:
        config = Config()
        authenticator = OpenSkyAuthenticator(config)

        access_token = authenticator.get_access_token()

        logger.info(
            "Authentication test successful. Token received with length: %s",
            len(access_token),
        )

    except PipelineError as error:
        logger.error("Application failed: %s", error)


if __name__ == "__main__":
    main()