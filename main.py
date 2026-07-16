from src.project_pipeline.config import Config
from src.project_pipeline.exceptions import PipelineError
from src.project_pipeline.logger import get_logger
from src.project_pipeline.pipeline import OpenSkyPipeline


logger = get_logger(__name__)


def main() -> None:
    """Application entry point."""

    try:
        config = Config()

        pipeline = OpenSkyPipeline(config)

        s3_key = pipeline.run()

        logger.info(
            "Application completed successfully. File uploaded to %s.",
            s3_key,
        )

    except PipelineError as error:
        logger.error(
            "Application failed: %s",
            error,
        )


if __name__ == "__main__":
    main()
    