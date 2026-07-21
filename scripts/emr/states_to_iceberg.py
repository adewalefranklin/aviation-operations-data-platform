import argparse
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType

# Configuration

DEFAULT_RAW_PATH = "s3://aviation-operations-data-platform/" "raw/opensky/states/"

DEFAULT_REJECTED_PATH = (
    "s3://aviation-operations-data-platform/" "data-quality/rejected/opensky/states/"
)

ICEBERG_WAREHOUSE_PATH = "s3://aviation-operations-data-platform/" "iceberg/"

ICEBERG_CATALOG = "glue_catalog"
ICEBERG_DATABASE = "aviation_operations"
ICEBERG_TABLE = "opensky_flight_states_iceberg"

ICEBERG_TABLE_NAME = f"{ICEBERG_CATALOG}." f"{ICEBERG_DATABASE}." f"{ICEBERG_TABLE}"

INCOMING_VIEW_NAME = "incoming_opensky_states"


# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


# Runtime arguments


def parse_arguments() -> argparse.Namespace:
    """
    Parse optional EMR step arguments.

    Without arguments, the job processes the complete raw prefix.

    For incremental processing, pass a specific raw object or
    ingestion partition using --raw-path.
    """

    parser = argparse.ArgumentParser(
        description=("Transform OpenSky raw JSON into an Apache Iceberg table.")
    )

    parser.add_argument(
        "--raw-path",
        default=DEFAULT_RAW_PATH,
        help=("S3 raw prefix, partition, or individual JSON object " "to process."),
    )

    parser.add_argument(
        "--rejected-path",
        default=DEFAULT_REJECTED_PATH,
        help="S3 destination for rejected records.",
    )

    return parser.parse_args()


# Spark session


def create_spark_session() -> SparkSession:
    """
    Create a Spark session configured for Apache Iceberg and
    the AWS Glue Data Catalog.
    """

    logger.info("Creating Spark session with Apache Iceberg support")

    spark = (
        SparkSession.builder.appName("OpenSkyStatesToIceberg")
        .config(
            "spark.sql.extensions",
            ("org.apache.iceberg.spark.extensions." "IcebergSparkSessionExtensions"),
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            ICEBERG_WAREHOUSE_PATH,
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    logger.info("Spark session created successfully")

    return spark


# Iceberg catalog verification


def verify_iceberg_catalog(
    spark: SparkSession,
) -> None:
    """
    Force Spark to load the Iceberg catalog and confirm that the
    required Glue database exists.
    """

    logger.info(
        "Verifying Iceberg catalog %s and database %s",
        ICEBERG_CATALOG,
        ICEBERG_DATABASE,
    )

    namespaces_df = spark.sql(f"SHOW NAMESPACES IN {ICEBERG_CATALOG}")

    namespace_exists = (
        namespaces_df.filter(F.col("namespace") == ICEBERG_DATABASE).limit(1).count()
        > 0
    )

    if not namespace_exists:
        raise RuntimeError(
            "Required Glue database was not found: " f"{ICEBERG_DATABASE}"
        )

    logger.info("Iceberg catalog verified successfully")


# Read raw OpenSky data


def read_raw_data(
    spark: SparkSession,
    raw_path: str,
) -> DataFrame:
    """
    Read multiline OpenSky JSON payloads from Amazon S3.

    raw_path may reference:

    1. The complete raw dataset.
    2. A year/month/day/hour partition.
    3. One specific JSON ingestion object.
    """

    logger.info(
        "Reading raw OpenSky data from %s",
        raw_path,
    )

    raw_df = spark.read.option("multiLine", "true").json(raw_path)

    logger.info("Raw OpenSky data loaded successfully")

    return raw_df


# Explode OpenSky states


def explode_states(
    raw_df: DataFrame,
) -> DataFrame:
    """
    Convert every OpenSky state vector into an individual Spark row.

    posexplode_outer preserves the original array position and allows
    null state arrays to reach the rejection logic.
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

    logger.info("OpenSky states array exploded successfully")

    return exploded_df


# Validate records


def separate_valid_and_rejected(
    exploded_df: DataFrame,
    processing_run_id: str,
    processing_timestamp: datetime,
) -> tuple[DataFrame, DataFrame]:
    """
    Separate valid OpenSky records from malformed records.

    A valid OpenSky state vector must contain exactly 17 fields.
    """

    logger.info("Validating OpenSky state-vector lengths")

    validated_df = exploded_df.withColumn(
        "field_count",
        F.size(F.col("state")),
    )

    valid_df = validated_df.filter(
        F.col("state").isNotNull() & (F.col("field_count") == 17)
    )

    rejected_df = (
        validated_df.filter(F.col("state").isNull() | (F.col("field_count") != 17))
        .withColumn(
            "rejection_reason",
            F.when(
                F.col("state").isNull(),
                F.lit("STATE_ARRAY_IS_NULL"),
            ).otherwise(
                F.concat(
                    F.lit("INVALID_FIELD_COUNT_" "EXPECTED_17_ACTUAL_"),
                    F.coalesce(
                        F.col("field_count").cast("string"),
                        F.lit("NULL"),
                    ),
                )
            ),
        )
        .withColumn(
            "processing_run_id",
            F.lit(processing_run_id),
        )
        .withColumn(
            "rejection_timestamp",
            F.lit(processing_timestamp).cast("timestamp"),
        )
        .withColumn(
            "ingestion_date",
            F.to_date(F.col("rejection_timestamp")),
        )
    )

    logger.info("OpenSky state-vector validation completed")

    return valid_df, rejected_df


# Transform valid records


def transform_valid_records(
    valid_df: DataFrame,
    processing_run_id: str,
    processing_timestamp: datetime,
) -> DataFrame:
    """
    Map OpenSky's 17 positional values into named analytical columns.

    record_key:
        Stable identifier used for Iceberg MERGE matching.

    payload_hash:
        Hash used to determine whether a matching record has changed.

    processing_run_id and ingestion_timestamp are excluded from the
    payload hash because they change for every pipeline execution.
    """

    logger.info("Mapping OpenSky state positions to analytical columns")

    transformed_df = valid_df.select(
        F.col("state")[0].cast("string").alias("icao24"),
        F.trim(F.col("state")[1].cast("string")).alias("callsign"),
        F.col("state")[2].cast("string").alias("origin_country"),
        F.col("state")[3].cast("long").alias("time_position"),
        F.col("state")[4].cast("long").alias("last_contact"),
        F.col("state")[5].cast("double").alias("longitude"),
        F.col("state")[6].cast("double").alias("latitude"),
        F.col("state")[7].cast("double").alias("barometric_altitude_m"),
        F.col("state")[8].cast("boolean").alias("on_ground"),
        F.col("state")[9].cast("double").alias("velocity_m_s"),
        F.col("state")[10].cast("double").alias("true_track_degrees"),
        F.col("state")[11].cast("double").alias("vertical_rate_m_s"),
        F.from_json(
            F.col("state")[12].cast("string"),
            ArrayType(IntegerType()),
        ).alias("sensors"),
        F.col("state")[13].cast("double").alias("geometric_altitude_m"),
        F.col("state")[14].cast("string").alias("squawk"),
        F.col("state")[15].cast("boolean").alias("special_purpose_indicator"),
        F.col("state")[16].cast("integer").alias("position_source"),
        F.col("source_response_time"),
        F.col("source_file"),
        F.col("state_position"),
        F.lit("opensky").alias("source"),
        F.lit(processing_run_id).alias("processing_run_id"),
        F.lit(processing_timestamp).cast("timestamp").alias("ingestion_timestamp"),
    )

    transformed_df = (
        transformed_df.withColumn(
            "event_timestamp_epoch",
            F.coalesce(
                F.col("time_position"),
                F.col("last_contact"),
                F.col("source_response_time"),
            ),
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.from_unixtime(F.col("event_timestamp_epoch"))),
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp")),
        )
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
        .withColumn(
            "record_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(
                        F.col("icao24"),
                        F.lit("NULL"),
                    ),
                    F.coalesce(
                        F.col("last_contact").cast("string"),
                        F.lit("NULL"),
                    ),
                    F.coalesce(
                        F.col("source_response_time").cast("string"),
                        F.lit("NULL"),
                    ),
                    F.coalesce(
                        F.col("state_position").cast("string"),
                        F.lit("NULL"),
                    ),
                ),
                256,
            ),
        )
    )

    payload_columns = [
        "icao24",
        "callsign",
        "origin_country",
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "barometric_altitude_m",
        "on_ground",
        "velocity_m_s",
        "true_track_degrees",
        "vertical_rate_m_s",
        "sensors",
        "geometric_altitude_m",
        "squawk",
        "special_purpose_indicator",
        "position_source",
        "source_response_time",
        "state_position",
        "source",
        "event_timestamp_epoch",
        "event_timestamp",
        "event_date",
        "velocity_kmh",
        "barometric_altitude_ft",
        "geometric_altitude_ft",
    ]

    transformed_df = transformed_df.withColumn(
        "payload_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(
                        F.col(column_name).cast("string"),
                        F.lit("NULL"),
                    )
                    for column_name in payload_columns
                ],
            ),
            256,
        ),
    )

    logger.info("Valid OpenSky records transformed successfully")

    return transformed_df


# Deduplicate and materialize merge source


def prepare_merge_source(
    processed_df: DataFrame,
) -> DataFrame:
    """
    Deduplicate the current batch and materialize the result.

    Materializing with localCheckpoint cuts the transformation lineage
    before Iceberg MERGE analysis. This prevents expressions such as
    input_file_name from remaining in the MERGE source logical plan.

    Iceberg requires the MERGE source to be deterministic.
    """

    logger.info("Deduplicating current batch by record_key")

    deduplicated_df = processed_df.dropDuplicates(["record_key"])

    logger.info("Materializing deterministic Iceberg MERGE source")

    materialized_df = deduplicated_df.localCheckpoint(eager=True)

    logger.info("Current batch deduplicated and materialized successfully")

    return materialized_df


# Iceberg table existence


def iceberg_table_exists(
    spark: SparkSession,
) -> bool:
    """
    Check whether the Glue-backed Iceberg table already exists.
    """

    logger.info(
        "Checking whether Iceberg table exists: %s",
        ICEBERG_TABLE_NAME,
    )

    tables_df = spark.sql(f"SHOW TABLES IN " f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}")

    table_exists = (
        tables_df.filter(F.col("tableName") == ICEBERG_TABLE).limit(1).count() > 0
    )

    logger.info(
        "Iceberg table exists: %s",
        table_exists,
    )

    return table_exists


# Create Iceberg table


def create_iceberg_table(
    processed_df: DataFrame,
) -> None:
    """
    Create the Iceberg version 2 table during the first run.

    days(event_timestamp) is an Iceberg hidden partition transform.
    """

    logger.info(
        "Creating Iceberg table %s",
        ICEBERG_TABLE_NAME,
    )

    (
        processed_df.writeTo(ICEBERG_TABLE_NAME)
        .using("iceberg")
        .partitionedBy(F.days("event_timestamp"))
        .tableProperty(
            "format-version",
            "2",
        )
        .tableProperty(
            "write.format.default",
            "parquet",
        )
        .tableProperty(
            "write.parquet.compression-codec",
            "zstd",
        )
        .create()
    )

    logger.info(
        "Iceberg table created successfully: %s",
        ICEBERG_TABLE_NAME,
    )


# Merge into existing Iceberg table


def merge_into_iceberg_table(
    spark: SparkSession,
    processed_df: DataFrame,
) -> None:
    """
    Merge the current deterministic batch into the Iceberg table.

    Matching key and identical payload:
        No action.

    Matching key and changed payload:
        Update the target record.

    New key:
        Insert the source record.
    """

    logger.info(
        "Merging current batch into %s",
        ICEBERG_TABLE_NAME,
    )

    processed_df.createOrReplaceTempView(INCOMING_VIEW_NAME)

    merge_sql = f"""
        MERGE INTO {ICEBERG_TABLE_NAME} AS target
        USING {INCOMING_VIEW_NAME} AS source
        ON target.record_key = source.record_key

        WHEN MATCHED
             AND NOT (
                 target.payload_hash
                 <=> source.payload_hash
             )
        THEN UPDATE SET
            target.icao24 =
                source.icao24,
            target.callsign =
                source.callsign,
            target.origin_country =
                source.origin_country,
            target.time_position =
                source.time_position,
            target.last_contact =
                source.last_contact,
            target.longitude =
                source.longitude,
            target.latitude =
                source.latitude,
            target.barometric_altitude_m =
                source.barometric_altitude_m,
            target.on_ground =
                source.on_ground,
            target.velocity_m_s =
                source.velocity_m_s,
            target.true_track_degrees =
                source.true_track_degrees,
            target.vertical_rate_m_s =
                source.vertical_rate_m_s,
            target.sensors =
                source.sensors,
            target.geometric_altitude_m =
                source.geometric_altitude_m,
            target.squawk =
                source.squawk,
            target.special_purpose_indicator =
                source.special_purpose_indicator,
            target.position_source =
                source.position_source,
            target.source_response_time =
                source.source_response_time,
            target.source_file =
                source.source_file,
            target.state_position =
                source.state_position,
            target.source =
                source.source,
            target.processing_run_id =
                source.processing_run_id,
            target.ingestion_timestamp =
                source.ingestion_timestamp,
            target.event_timestamp_epoch =
                source.event_timestamp_epoch,
            target.event_timestamp =
                source.event_timestamp,
            target.event_date =
                source.event_date,
            target.velocity_kmh =
                source.velocity_kmh,
            target.barometric_altitude_ft =
                source.barometric_altitude_ft,
            target.geometric_altitude_ft =
                source.geometric_altitude_ft,
            target.payload_hash =
                source.payload_hash

        WHEN NOT MATCHED
        THEN INSERT (
            icao24,
            callsign,
            origin_country,
            time_position,
            last_contact,
            longitude,
            latitude,
            barometric_altitude_m,
            on_ground,
            velocity_m_s,
            true_track_degrees,
            vertical_rate_m_s,
            sensors,
            geometric_altitude_m,
            squawk,
            special_purpose_indicator,
            position_source,
            source_response_time,
            source_file,
            state_position,
            source,
            processing_run_id,
            ingestion_timestamp,
            event_timestamp_epoch,
            event_timestamp,
            event_date,
            velocity_kmh,
            barometric_altitude_ft,
            geometric_altitude_ft,
            record_key,
            payload_hash
        )
        VALUES (
            source.icao24,
            source.callsign,
            source.origin_country,
            source.time_position,
            source.last_contact,
            source.longitude,
            source.latitude,
            source.barometric_altitude_m,
            source.on_ground,
            source.velocity_m_s,
            source.true_track_degrees,
            source.vertical_rate_m_s,
            source.sensors,
            source.geometric_altitude_m,
            source.squawk,
            source.special_purpose_indicator,
            source.position_source,
            source.source_response_time,
            source.source_file,
            source.state_position,
            source.source,
            source.processing_run_id,
            source.ingestion_timestamp,
            source.event_timestamp_epoch,
            source.event_timestamp,
            source.event_date,
            source.velocity_kmh,
            source.barometric_altitude_ft,
            source.geometric_altitude_ft,
            source.record_key,
            source.payload_hash
        )
    """

    spark.sql(merge_sql)

    logger.info("Iceberg MERGE completed successfully")


# Write Iceberg table


def write_iceberg_table(
    spark: SparkSession,
    processed_df: DataFrame,
) -> None:
    """
    Create the table on the first run and use MERGE on later runs.
    """

    if iceberg_table_exists(spark):
        merge_into_iceberg_table(
            spark=spark,
            processed_df=processed_df,
        )
    else:
        create_iceberg_table(
            processed_df=processed_df,
        )


# Write rejected records


def write_rejected_data(
    rejected_df: DataFrame,
    rejected_path: str,
) -> int:
    """
    Append rejected records as partitioned Parquet audit events.
    """

    logger.info("Checking for rejected OpenSky records")

    rejected_count = rejected_df.count()

    if rejected_count == 0:
        logger.info("No rejected records found; " "skipping rejected-data write")

        return 0

    logger.info(
        "Writing %s rejected records to %s",
        rejected_count,
        rejected_path,
    )

    (
        rejected_df.write.mode("append")
        .partitionBy("ingestion_date")
        .parquet(rejected_path)
    )

    logger.info("Rejected records written successfully")

    return rejected_count


# Pipeline execution


def main() -> None:
    """
    Execute the OpenSky raw-to-Iceberg processing pipeline.

    The target table is idempotent:

    - identical reruns do not insert duplicates;
    - changed matching records are updated;
    - previously unseen records are inserted.
    """

    args = parse_arguments()

    processing_run_id = str(uuid.uuid4())

    processing_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    logger.info(
        "Starting OpenSky Iceberg pipeline; " "run_id=%s raw_path=%s",
        processing_run_id,
        args.raw_path,
    )

    spark = create_spark_session()

    processed_df: DataFrame | None = None

    try:
        logger.info("Pipeline stage: verify Iceberg catalog")

        verify_iceberg_catalog(
            spark=spark,
        )

        logger.info("Pipeline stage: read raw OpenSky data")

        raw_df = read_raw_data(
            spark=spark,
            raw_path=args.raw_path,
        )

        logger.info("Pipeline stage: explode state vectors")

        exploded_df = explode_states(
            raw_df=raw_df,
        )

        logger.info("Pipeline stage: validate state vectors")

        valid_df, rejected_df = separate_valid_and_rejected(
            exploded_df=exploded_df,
            processing_run_id=processing_run_id,
            processing_timestamp=processing_timestamp,
        )

        logger.info("Pipeline stage: transform valid records")

        transformed_df = transform_valid_records(
            valid_df=valid_df,
            processing_run_id=processing_run_id,
            processing_timestamp=processing_timestamp,
        )

        logger.info("Pipeline stage: prepare deterministic merge source")

        processed_df = prepare_merge_source(
            processed_df=transformed_df,
        )

        valid_record_count = processed_df.count()

        if valid_record_count == 0:
            raise RuntimeError(
                "No valid OpenSky records were available " "for the Iceberg table"
            )

        logger.info(
            "Prepared %s unique valid records for Iceberg",
            valid_record_count,
        )

        logger.info("Pipeline stage: write Iceberg table")

        write_iceberg_table(
            spark=spark,
            processed_df=processed_df,
        )

        logger.info("Pipeline stage: write rejected records")

        rejected_record_count = write_rejected_data(
            rejected_df=rejected_df,
            rejected_path=args.rejected_path,
        )

        logger.info(
            "OpenSky Iceberg pipeline completed successfully; "
            "run_id=%s valid_records=%s rejected_records=%s",
            processing_run_id,
            valid_record_count,
            rejected_record_count,
        )

    except Exception:
        logger.exception(
            "OpenSky Iceberg pipeline failed; " "run_id=%s",
            processing_run_id,
        )

        raise

    finally:
        if processed_df is not None:
            processed_df.unpersist(blocking=False)

        spark.stop()

        logger.info(
            "Spark session stopped; run_id=%s",
            processing_run_id,
        )


if __name__ == "__main__":
    main()
