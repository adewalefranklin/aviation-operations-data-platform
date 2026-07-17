from pyspark.sql import SparkSession

import pyspark.sql.functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    IntegerType,
    ArrayType,
)


def main() -> None:
    spark = SparkSession.builder.appName("OpenSkyStatesToParquet").getOrCreate()

    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    raw_path = "s3://aviation-operations-data-platform/" "raw/opensky/states/"

    processed_path = (
        "s3://aviation-operations-data-platform/" "processed/opensky/states/"
    )

    rejected_path = (
        "s3://aviation-operations-data-platform/"
        "data-quality/rejected/opensky/states/"
    )

    raw_df = spark.read.option("multiLine", "true").json(raw_path)

    raw_df.printSchema()

    raw_df.select(
        F.col("time"),
        F.size(F.col("states")).alias("state_count"),
    ).show(
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()
