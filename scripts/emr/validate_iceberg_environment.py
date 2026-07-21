from pyspark.sql import SparkSession

ICEBERG_TABLE_NAME = "glue_catalog." "aviation_operations." "iceberg_test"


spark = (
    SparkSession.builder.appName("IcebergTableTest")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions." "IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://aviation-operations-data-platform/iceberg/",
    )
    .config(
        "spark.sql.session.timeZone",
        "UTC",
    )
    .getOrCreate()
)


try:
    print("Spark session created successfully")

    spark.sql("SHOW NAMESPACES IN glue_catalog").show(truncate=False)

    test_df = spark.createDataFrame(
        [
            (
                1,
                "TEST123",
                "Germany",
            ),
            (
                2,
                "TEST456",
                "France",
            ),
        ],
        [
            "id",
            "callsign",
            "origin_country",
        ],
    )

    (
        test_df.writeTo(ICEBERG_TABLE_NAME)
        .using("iceberg")
        .tableProperty(
            "format-version",
            "2",
        )
        .create()
    )

    print(f"Iceberg table created successfully: " f"{ICEBERG_TABLE_NAME}")

    spark.table(ICEBERG_TABLE_NAME).show(truncate=False)

finally:
    spark.stop()
    print("Spark session stopped")
