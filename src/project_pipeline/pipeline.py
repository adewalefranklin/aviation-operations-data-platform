from src.project_pipeline.config import Config
from src.project_pipeline.exceptions import PipelineError
from src.project_pipeline.extract import OpenSkyExtractor
from src.project_pipeline.load import S3JsonLoader
from src.project_pipeline.logger import get_logger


class OpenSkyPipeline:
    """Orchestrates extraction and raw loading of OpenSky aircraft states."""

    def __init__(self, config: Config):
        self.config = config
        self.logger = get_logger(__name__)
        self.extractor = OpenSkyExtractor(config)
        self.loader = S3JsonLoader(config)

    def run(self) -> str:
        """Run the OpenSky ELT ingestion pipeline."""

        try:
            raw_data = self.extractor.extract_states()

            s3_key = self.loader.load_to_s3(raw_data)

            self.logger.info(
                "OpenSky ELT pipeline completed successfully. "
                "Data loaded to S3 at %s.",
                s3_key,
            )

            return s3_key

        except PipelineError as error:
            self.logger.error(
                "OpenSky ELT pipeline failed: %s",
                error,
            )
            raise