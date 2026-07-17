import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, IntegerType

# Application logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger("OpenSkyStatesToParquet")


# S3 paths

RAW_PATH = "s3://aviation-operations-data-platform/" "raw/opensky/states/"

PROCESSED_PATH = "s3://aviation-operations-data-platform/" "processed/opensky/states/"

REJECTED_PATH = (
    "s3://aviation-operations-data-platform/" "data-quality/rejected/opensky/states/"
)


# Read raw OpenSky payloads


def read_raw_data(
    spark: SparkSession,
    raw_path: str,
) -> DataFrame:
    """
    Read multiline OpenSky JSON payloads from the S3 raw layer.
    """

    logger.info("Reading raw OpenSky data from %s", raw_path)

    raw_df = spark.read.option("multiLine", "true").json(raw_path)

    logger.info("Raw OpenSky data loaded successfully")

    return raw_df


# Explode the states array


def explode_states(
    raw_df: DataFrame,
) -> DataFrame:
    """
    Convert each item inside the OpenSky states array into
    a separate Spark row.
    """

    logger.info("Exploding the OpenSky states array")

    exploded_df = raw_df.select(
        F.col("time").cast("long").alias("source_response_time"),
        F.input_file_name().alias("source_file"),
        F.posexplode_outer(F.col("states")).alias(
            "state_position",
            "state",
        ),
    )

    logger.info("States array exploded successfully")

    return exploded_df


# Separate valid and rejected records


def separate_valid_and_rejected(
    exploded_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    A valid OpenSky state vector must contain exactly 17 fields.
    Records with missing or malformed state arrays are rejected.
    """

    logger.info("Validating OpenSky state-vector lengths")

    validated_df = exploded_df.withColumn(
        "field_count",
        F.size(F.col("state")),
    )

    valid_df = validated_df.filter(F.col("field_count") == 17)

    rejected_df = (
        validated_df.filter(F.col("state").isNull() | (F.col("field_count") != 17))
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("state").isNull(),
                F.lit("STATE_ARRAY_IS_NULL"),
            ).otherwise(
                F.concat(
                    F.lit("INVALID_FIELD_COUNT_EXPECTED_17_ACTUAL_"),
                    F.coalesce(
                        F.col("field_count").cast("string"),
                        F.lit("NULL"),
                    ),
                )
            ),
        )
        .withColumn(
            "rejection_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "ingestion_date",
            F.to_date(F.col("rejection_timestamp")),
        )
    )

    logger.info("State-vector validation completed")

    return valid_df, rejected_df


# Transform valid OpenSky records


def transform_valid_records(
    valid_df: DataFrame,
) -> DataFrame:
    """
    Map the 17 positional values in each OpenSky state vector
    to named analytical columns.
    """

    logger.info("Mapping OpenSky state positions to named columns")

    transformed_df = valid_df.select(
        # State-vector position 0
        F.col("state")[0].cast("string").alias("icao24"),
        # Position 1
        F.trim(F.col("state")[1].cast("string")).alias("callsign"),
        # Position 2
        F.col("state")[2].cast("string").alias("origin_country"),
        # Position 3
        F.col("state")[3].cast("long").alias("time_position"),
        # Position 4
        F.col("state")[4].cast("long").alias("last_contact"),
        # Position 5
        F.col("state")[5].cast("double").alias("longitude"),
        # Position 6
        F.col("state")[6].cast("double").alias("latitude"),
        # Position 7
        F.col("state")[7].cast("double").alias("barometric_altitude_m"),
        # Position 8
        F.col("state")[8].cast("boolean").alias("on_ground"),
        # Position 9
        F.col("state")[9].cast("double").alias("velocity_m_s"),
        # Position 10
        F.col("state")[10].cast("double").alias("true_track_degrees"),
        # Position 11
        F.col("state")[11].cast("double").alias("vertical_rate_m_s"),
        # Position 12
        F.from_json(
            F.col("state")[12].cast("string"),
            ArrayType(IntegerType()),
        ).alias("sensors"),
        # Position 13
        F.col("state")[13].cast("double").alias("geometric_altitude_m"),
        # Position 14
        F.col("state")[14].cast("string").alias("squawk"),
        # Position 15
        F.col("state")[15].cast("boolean").alias("special_purpose_indicator"),
        # Position 16
        F.col("state")[16].cast("integer").alias("position_source"),
        # Metadata
        F.col("source_response_time"),
        F.col("source_file"),
        F.col("state_position"),
        F.lit("opensky").alias("source"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )

    transformed_df = (
        transformed_df
        # Prefer the aircraft position timestamp.
        # Fall back to last_contact and then API response time.
        .withColumn(
            "event_timestamp_epoch",
            F.coalesce(
                F.col("time_position"),
                F.col("last_contact"),
                F.col("source_response_time"),
            ),
        )
        # Convert Unix epoch seconds to a readable timestamp.
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.from_unixtime(F.col("event_timestamp_epoch"))),
        )
        # Partition column for the processed S3 layer.
        .withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp")),
        )
        # Useful aviation-friendly derived values.
        .withColumn(
            "velocity_kmh",
            F.round(
                F.col("velocity_m_s") * F.lit(3.6),
                2,
            ),
        )
        .withColumn(
            "barometric_altitude_ft",
            F.round(
                F.col("barometric_altitude_m") * F.lit(3.28084),
                2,
            ),
        )
        .withColumn(
            "geometric_altitude_ft",
            F.round(
                F.col("geometric_altitude_m") * F.lit(3.28084),
                2,
            ),
        )
    )

    logger.info("Valid OpenSky records transformed successfully")

    return transformed_df


# Write processed Parquet data


def write_processed_data(
    processed_df: DataFrame,
    processed_path: str,
) -> None:
    """
    Write valid records as Parquet partitioned by event_date.
    """

    logger.info(
        "Writing processed Parquet data to %s",
        processed_path,
    )

    (
        processed_df.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(processed_path)
    )

    logger.info("Processed Parquet data written successfully")


# Write rejected records


def write_rejected_data(
    rejected_df: DataFrame,
    rejected_path: str,
) -> None:
    """
    Write malformed records for later inspection.
    """

    logger.info(
        "Writing rejected records to %s",
        rejected_path,
    )

    (
        rejected_df.write.mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(rejected_path)
    )

    logger.info("Rejected records written successfully")


# Main Spark application


def main() -> None:
    logger.info("Starting OpenSky Spark transformation job")

    spark = SparkSession.builder.appName("OpenSkyStatesToParquet").getOrCreate()

    try:
        spark.conf.set(
            "spark.sql.session.timeZone",
            "UTC",
        )

        # Overwrite only the partitions included in the current job.
        spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )

        logger.info("SparkSession created successfully")
        logger.info("Spark session timezone set to UTC")

        raw_df = read_raw_data(
            spark=spark,
            raw_path=RAW_PATH,
        )

        logger.info("Raw schema:")
        raw_df.printSchema()

        exploded_df = explode_states(raw_df)

        valid_df, rejected_df = separate_valid_and_rejected(exploded_df)

        # Cache because the DataFrames are used for both counting
        # and writing.
        valid_df = valid_df.cache()
        rejected_df = rejected_df.cache()

        valid_count = valid_df.count()
        rejected_count = rejected_df.count()

        logger.info(
            "Validation summary - valid records: %s",
            valid_count,
        )

        logger.info(
            "Validation summary - rejected records: %s",
            rejected_count,
        )

        processed_df = transform_valid_records(valid_df)

        logger.info("Processed schema:")
        processed_df.printSchema()

        processed_df.select(
            "icao24",
            "callsign",
            "origin_country",
            "longitude",
            "latitude",
            "velocity_kmh",
            "event_timestamp",
            "event_date",
        ).show(
            20,
            truncate=False,
        )

        if valid_count > 0:
            write_processed_data(
                processed_df=processed_df,
                processed_path=PROCESSED_PATH,
            )
        else:
            logger.warning(
                "No valid records found. " "Processed output will not be written."
            )

        if rejected_count > 0:
            write_rejected_data(
                rejected_df=rejected_df,
                rejected_path=REJECTED_PATH,
            )
        else:
            logger.info(
                "No rejected records found. " "Rejected output will not be written."
            )

        logger.info("OpenSky Spark transformation completed successfully")

    except Exception:
        logger.exception("OpenSky Spark transformation failed")
        raise

    finally:
        logger.info("Stopping SparkSession")
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
