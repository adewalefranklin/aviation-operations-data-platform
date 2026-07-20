# Use Apache Iceberg as the Lakehouse Table Format

## Status

Accepted

## Date

2026-07-20

---

# Context

The Aviation Operations Data Platform requires a modern table format capable of managing analytical datasets stored in Amazon S3 while supporting reliable updates, schema evolution, and ACID transactions.

The solution must support:

- Apache Spark integration
- Amazon S3 storage
- AWS Glue Data Catalog integration
- Amazon Athena querying
- ACID transactions
- schema evolution
- hidden partitioning
- time travel
- snapshot management
- idempotent data ingestion
- efficient analytical queries

The table format should separate metadata management from the underlying data files while remaining compatible with the AWS analytics ecosystem.

---

# Options Considered

## Option 1 — Apache Iceberg ✅

Apache Iceberg is an open table format designed for large-scale analytical data lakes.

### Advantages

- ACID transactions
- Native Apache Spark support
- Native Amazon Athena support
- Native AWS Glue Data Catalog integration
- Hidden partitioning
- Schema evolution
- Snapshot management
- Time travel
- MERGE, UPDATE and DELETE support
- Handles large datasets efficiently
- Suitable for enterprise lakehouse architectures

### Disadvantages

- Additional metadata management
- More complex than plain Parquet
- Requires Iceberg runtime libraries
- Slight learning curve compared to traditional data lakes

---

## Option 2 — Standard Parquet Files

Store processed data directly as partitioned Parquet files.

### Why Not

- No ACID transactions
- No MERGE support
- No snapshot history
- Manual partition management
- Difficult schema evolution
- Duplicate handling must be implemented manually

---

## Option 3 — Delta Lake

Lakehouse table format developed by Databricks.

### Why Not

- Primarily optimized for the Databricks ecosystem
- Less tightly integrated with native AWS analytics services
- Apache Iceberg aligns better with the project's AWS-first architecture

---

## Option 4 — Apache Hudi

Incremental data lake framework supporting upserts and CDC.

### Why Not

- Greater operational complexity
- Focused on incremental ingestion workloads
- Apache Iceberg provides a simpler architecture for analytical workloads

---

# Decision

Apache Iceberg will serve as the analytical table format for processed aviation data.

Apache Spark running on Amazon EMR transforms raw OpenSky flight data into an Iceberg table stored in Amazon S3.

The AWS Glue Data Catalog manages the table metadata while Amazon Athena queries the data directly without requiring crawlers or manual partition management.

The processing workflow is:

```
OpenSky API
        │
        ▼
Python Extraction
        │
        ▼
Amazon S3 Raw Layer
        │
        ▼
Amazon EMR
        │
        ▼
Apache Spark
        │
        ▼
Apache Iceberg
        │
        ▼
AWS Glue Data Catalog
        │
        ▼
Amazon Athena
```

Storage remains separated from compute while Iceberg manages metadata and table operations.

---

# Implementation

The following implementation decisions were made.

## Iceberg Catalog

The AWS Glue Data Catalog was configured as the Iceberg catalog.

```
glue_catalog
```

This allows Spark and Athena to access the same table metadata.

---

## Warehouse Location

Iceberg metadata and data files are stored in Amazon S3.

```
s3://aviation-operations-data-platform/iceberg/
```

---

## Database

```
aviation_operations
```

---

## Iceberg Table

```
opensky_flight_states_iceberg
```

---

## Spark Configuration

The Spark session was configured with:

- Iceberg Spark Extensions
- Glue Catalog
- GlueCatalog implementation
- Amazon S3 FileIO
- Iceberg warehouse location

The Iceberg runtime library is loaded during Spark job submission.

---

## Hidden Partitioning

Instead of manually creating folder structures, Iceberg automatically partitions the table using:

```
days(event_timestamp)
```

Users query the table normally while Iceberg handles partition pruning internally.

---

## MERGE Operations

Instead of appending data on every execution, the pipeline performs an Iceberg MERGE operation.

Matching records are updated only when their contents change.

New records are inserted automatically.

---

## Idempotent Processing

To prevent duplicate data, the pipeline generates:

```
record_key
```

A unique identifier for every logical flight record.

Each record also includes:

```
payload_hash
```

Used to detect whether the record contents have changed.

Additional audit information includes:

```
processing_run_id
```

and

```
ingestion_timestamp
```

allowing every pipeline execution to be traced.

---

# Validation

The following functionality was successfully verified:

- Iceberg runtime loaded successfully
- Spark connected to AWS Glue Catalog
- Iceberg table created automatically
- Table registered in AWS Glue
- Queries executed successfully in Amazon Athena
- Hidden partitioning configured
- MERGE operations executed successfully
- Duplicate records prevented
- Audit columns populated correctly

---

# Why Apache Iceberg?

Apache Iceberg was selected because it provides enterprise-grade table management for cloud data lakes while remaining tightly integrated with the AWS analytics ecosystem.

Compared to traditional Parquet datasets, Iceberg simplifies schema evolution, partition management and incremental data processing while enabling reliable ACID transactions and idempotent ingestion.

This aligns with the project's objective of building a production-ready lakehouse architecture using AWS-native services.

---

# Consequences

## Positive

- ACID transactions
- Reliable MERGE operations
- Schema evolution
- Hidden partitioning
- Snapshot management
- Time travel support
- Native Glue Catalog integration
- Native Athena integration
- Supports idempotent processing
- Production-ready lakehouse architecture

## Negative

- Additional metadata files
- Requires Iceberg runtime libraries
- Slightly higher implementation complexity
- More concepts to understand than plain Parquet

---

# Future Considerations

Future enhancements include:

- Snapshot rollback
- Time travel queries
- Incremental reads
- Schema evolution demonstrations
- Partition evolution
- Iceberg maintenance procedures
- Snapshot expiration
- Orphan file cleanup
- Airflow orchestration
- Terraform deployment automation