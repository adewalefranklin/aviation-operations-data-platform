from datetime import datetime, timezone

from src.project_pipeline.exceptions import TransformError
from src.project_pipeline.logger import get_logger

logger = get_logger(__name__)

EXPECTED_STATE_FIELDS = 17


class OpenSkyTransformer:
    """Performs light transformation of OpenSky aircraft state vectors."""

    def transform_states(self, payload: dict) -> dict:
        """
        Transform OpenSky aircraft state vectors into readable dictionaries.
        """

        logger.info("Starting transformation of OpenSky aircraft states...")

        source_response_time = payload.get("time")
        states = payload.get("states")

        if states is None:
            raise TransformError("OpenSky payload does not contain a 'states' field.")

        ingestion_timestamp = datetime.now(timezone.utc).isoformat()

        transformed_states = []

        for state in states:

            if len(state) != EXPECTED_STATE_FIELDS:
                logger.warning(
                    "Skipping invalid state vector. Expected %s fields but received %s.",
                    EXPECTED_STATE_FIELDS,
                    len(state),
                )
                continue

            record = {
                "icao24": state[0],
                "callsign": state[1],
                "origin_country": state[2],
                "time_position": state[3],
                "last_contact": state[4],
                "longitude": state[5],
                "latitude": state[6],
                "barometric_altitude": state[7],
                "on_ground": state[8],
                "velocity": state[9],
                "true_track": state[10],
                "vertical_rate": state[11],
                "sensors": state[12],
                "geometric_altitude": state[13],
                "squawk": state[14],
                "spi": state[15],
                "position_source": state[16],
            }

            # Remove trailing spaces from the callsign.
            if record["callsign"]:
                record["callsign"] = record["callsign"].strip()

            # Enrich the record with pipeline metadata.

            record["source"] = "opensky"
            record["source_response_time"] = source_response_time
            record["ingestion_timestamp"] = ingestion_timestamp

            transformed_states.append(record)

        transformed_payload = {
            "source": "opensky",
            "source_response_time": source_response_time,
            "ingestion_timestamp": ingestion_timestamp,
            "record_count": len(transformed_states),
            "states": transformed_states,
        }

        logger.info(
            "Transformation completed successfully. %s aircraft records transformed.",
            len(transformed_states),
        )

        return transformed_payload
