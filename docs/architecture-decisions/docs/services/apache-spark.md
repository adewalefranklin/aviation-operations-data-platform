# Apache Spark

---

# Overview

Apache Spark is an open-source distributed data processing framework designed for large-scale analytics, ETL (Extract, Transform, Load), machine learning, and streaming workloads.

Unlike traditional Python scripts that execute on a single machine, Spark distributes computations across multiple machines (nodes), allowing very large datasets to be processed efficiently and fault-tolerantly.

Within the Aviation Operations Data Platform, Apache Spark is responsible for transforming raw aviation data stored in Amazon S3 into optimized analytical datasets stored as partitioned Parquet files.

---

# Why Apache Spark?

The OpenSky Network API returns nested JSON payloads that may eventually contain thousands or millions of aircraft state vectors collected continuously over time.

Processing this data using only Python would become increasingly slow as the volume grows.

Apache Spark provides:

- Distributed processing
- Parallel execution
- Fault tolerance
- In-memory computation
- Optimized analytical transformations
- Native support for Parquet
- Native integration with Amazon S3
- Scalability from gigabytes to petabytes

Spark separates compute from storage, allowing Amazon S3 to remain the permanent storage layer while EMR provides temporary compute resources.

---

# Why Not Plain Python?

Plain Python works well for:

- Data extraction
- API requests
- Small transformations
- Automation scripts

However, it executes on a single machine.

As aviation data grows, a single process becomes a bottleneck.

Apache Spark distributes processing across multiple executors, allowing multiple partitions of data to be processed simultaneously.

---

# Spark Within Our Architecture

```
                OpenSky API
                      │
                      ▼
          Python Extraction Layer
                      │
                      ▼
            Amazon S3 Raw Layer
                      │
                      ▼
          Amazon EMR Cluster
                      │
                      ▼
             Apache Spark
                      │
                      ▼
      Data Validation & Transformation
                      │
                      ▼
      Snappy Parquet (Processed Layer)
                      │
                      ▼
        AWS Glue / Athena / Snowflake
```

Spark is the transformation engine of the platform.

---

# Spark Architecture

Apache Spark consists of several components.

```
                Spark Application
                       │
                       ▼
                  Spark Driver
                       │
         ┌─────────────┴─────────────┐
         ▼                           ▼
     Executor 1                 Executor 2
         │                           │
      Partition A               Partition B
```

## Driver

The Driver is the central coordinator.

Responsibilities include:

- Creating the SparkSession
- Building the execution plan
- Scheduling tasks
- Coordinating executors
- Returning results

Our Driver runs on the EMR Primary node.

---

## Executors

Executors perform the actual computations.

Responsibilities include:

- Reading data
- Executing transformations
- Writing output
- Returning task results

Executors run on the Core nodes.

---

## YARN

YARN is Hadoop's resource manager.

Responsibilities include:

- Allocating cluster resources
- Launching Spark containers
- Monitoring execution
- Restarting failed containers when possible

Amazon EMR automatically configures YARN.

---

# SparkSession

Every Spark application begins by creating a SparkSession.

```python
spark = (
    SparkSession.builder
    .appName("OpenSkyStatesToParquet")
    .getOrCreate()
)
```

SparkSession represents the entry point into Spark.

It provides access to:

- Reading data
- Writing data
- SQL execution
- DataFrame creation
- Spark configuration

---

# Reading JSON from Amazon S3

The raw OpenSky payloads are stored inside the Raw Data Lake.

Spark reads them directly from S3.

```python
raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(RAW_PATH)
)
```

The `multiLine` option allows Spark to correctly parse formatted JSON documents spanning multiple lines.

---

# DataFrames

Spark stores data inside DataFrames.

A DataFrame is a distributed table similar to:

- SQL tables
- Pandas DataFrames

Unlike Pandas, Spark DataFrames are distributed across multiple machines.

Each row represents one record.

Each column has a defined data type.

---

# Transformations vs Actions

Spark operations are divided into two categories.

## Transformations

Transformations describe what should happen.

Examples:

- select()
- filter()
- withColumn()
- explode()
- alias()
- cast()

Transformations are lazy.

---

## Actions

Actions trigger execution.

Examples:

- show()
- count()
- write()
- collect()

Only when an action is called does Spark actually execute the entire plan.

---

# Lazy Evaluation

Spark does not execute each transformation immediately.

Instead it builds a logical execution plan.

Example:

```python
df.select(...)
  .filter(...)
  .withColumn(...)
```

Nothing runs yet.

Execution begins only after:

```python
df.write.parquet(...)
```

or

```python
df.show()
```

This allows Spark to optimize the entire pipeline before execution.

---

# Exploding the OpenSky State Array

The OpenSky API returns one large array containing many aircraft.

```
states
├── aircraft 1
├── aircraft 2
├── aircraft 3
└── ...
```

Spark converts each aircraft into its own row.

```python
F.posexplode_outer("states")
```

This returns:

- state_position
- state

Each aircraft can now be processed independently.

---

# Data Validation

Each OpenSky state vector must contain exactly 17 fields.

Spark validates every record.

```
field_count == 17
```

Records are divided into:

Valid records

↓

Processed Layer

Rejected records

↓

Data Quality Layer

---

# Mapping Positional Values

The OpenSky API returns arrays.

Example:

```
[
icao24,
callsign,
origin_country,
...
]
```

Spark maps every position to a named column.

Example:

```python
F.col("state")[0].alias("icao24")
```

becomes

```
icao24
```

instead of

```
state[0]
```

This creates an analytical schema suitable for downstream consumers.

---

# Data Type Conversion

Each positional value is converted into its proper type.

Examples include:

- String
- Double
- Boolean
- Integer
- Timestamp

Explicit casting improves data quality and enables efficient analytical queries.

---

# Derived Columns

Additional analytical columns are generated.

Examples include:

- velocity_kmh
- event_timestamp
- event_date
- barometric_altitude_ft
- geometric_altitude_ft

These columns simplify downstream analysis while preserving the original values.

---

# Partitioning

Processed data is partitioned by:

```
event_date
```

Spark automatically creates folders such as:

```
processed/

└── event_date=2026-07-16/
```

Partitioning improves query performance because analytical engines only scan the required partitions.

---

# Parquet

Spark writes the processed data as Apache Parquet.

```python
.write.parquet(...)
```

Parquet is:

- Columnar
- Compressed
- Efficient for analytics
- Supported by Athena
- Supported by Snowflake
- Supported by Glue

---

# Snappy Compression

Spark uses Snappy compression.

Output files end with:

```
.snappy.parquet
```

Advantages include:

- Fast compression
- Fast decompression
- Reduced storage
- Lower Athena query costs

---

# Logging

Python logging was added to the Spark application.

Example:

```python
logger.info("Reading raw OpenSky data")
```

Logging provides visibility into:

- Application startup
- Reading data
- Validation
- Transformation
- Writing output
- Errors
- Completion

These logs are available through:

- EMR Step Logs
- YARN Logs
- Amazon S3 EMR Logs
- Amazon CloudWatch

---

# Writing Output

Valid records are written to:

```
processed/
```

Rejected records are written to:

```
data-quality/rejected/
```

Both datasets are written in Parquet format.

---

# Why Apache Spark Was Selected

Apache Spark was selected because it provides:

- Distributed execution
- Fault tolerance
- High performance
- Native S3 integration
- Scalable ETL
- Excellent Parquet support
- Tight integration with Amazon EMR

It allows the processing layer to remain independent of the storage layer while supporting future growth of the Aviation Operations Data Platform.

---

# Lessons Learned

During implementation the following concepts were mastered:

- SparkSession
- DataFrames
- Reading JSON from S3
- Lazy Evaluation
- Transformations
- Actions
- Exploding nested arrays
- Data validation
- Schema mapping
- Data type conversion
- Derived analytical columns
- Partitioned Parquet
- Snappy compression
- Python logging
- Distributed execution on Amazon EMR
- YARN job execution
- EMR Step submission
- Writing analytical datasets to Amazon S3

---

# Future Enhancements

Future improvements include:

- Apache Iceberg tables
- Spark SQL
- Window functions
- Broadcast joins
- Delta-style merge operations
- Data compaction
- Performance tuning
- Glue Data Catalog integration
- Athena querying
- Airflow orchestration