# AWS Glue

## Status

Accepted

## Date

2026-07-17

---

# Context

After Apache Spark running on Amazon EMR transforms the raw OpenSky API JSON files into partitioned Parquet datasets, the processed data resides in Amazon S3.

Although the Parquet files already contain their schema internally, analytical services such as Amazon Athena require a centralized metadata repository that describes:

- table names
- column names
- data types
- partitions
- storage location

Without this metadata layer, every analytical service would need to rediscover the schema independently.

The platform therefore requires a centralized metadata catalog that remains independent from both storage and compute.

---

# Options Considered

## Option 1 — AWS Glue Data Catalog + Glue Crawler ✅

The crawler automatically scans the processed Parquet files stored in Amazon S3 and creates metadata definitions inside the AWS Glue Data Catalog.

### Advantages

- Automatically discovers schemas
- Automatically discovers partitions
- Eliminates manual schema creation
- Native integration with Athena
- Native integration with EMR
- Native integration with Lake Formation
- Supports evolving datasets
- Centralized metadata repository
- Scales automatically as new partitions are added

### Disadvantages

- Additional AWS service to manage
- Crawlers consume small amounts of compute while scanning data
- Table naming conventions should be planned carefully

---

## Option 2 — Manual Glue Tables

Create Glue tables manually by defining every column.

### Advantages

- Full control
- No crawler execution

### Why Not

- Time consuming
- Easy to introduce schema errors
- Difficult to maintain as schemas evolve
- Poor scalability

---

## Option 3 — Athena CREATE EXTERNAL TABLE

Create metadata directly using SQL.

### Advantages

- Familiar SQL syntax

### Why Not

- Manual maintenance
- Schema updates require manual intervention
- Less suitable for evolving data lake architectures

---

## Option 4 — Query Files Directly

Allow every analytics engine to interpret the Parquet files independently.

### Why Not

- No centralized metadata
- Duplicate schema discovery
- Poor governance
- Limited interoperability between AWS analytics services

---

# Decision

AWS Glue Data Catalog will serve as the centralized metadata repository for the Aviation Operations Data Platform.

A Glue Crawler will automatically scan the processed Parquet datasets produced by Apache Spark and register them as tables inside the Glue Data Catalog.

For this project:

Database

```
aviation_operations
```

Crawler

```
opensky-states-crawler
```

Generated Table

```
opensky_states
```

Storage Location

```
s3://aviation-operations-data-platform/processed/opensky/states/
```

The crawler automatically discovers:

- schema
- column names
- data types
- partition columns
- partition values

No manual schema creation is required.

---

# Architecture

```
Amazon S3 (Processed Parquet)
            │
            ▼
     Glue Crawler
            │
 Discovers Metadata
            │
            ▼
 AWS Glue Data Catalog
            │
            ▼
      Amazon Athena
```

---

# Consequences

## Positive

- Centralized metadata management
- Automatic schema discovery
- Automatic partition discovery
- Eliminates manual table creation
- Native integration across AWS analytics services
- Easy expansion as additional datasets are added

## Negative

- Crawler execution introduces a small operational cost
- Incorrect folder structures may generate unexpected table names
- Table naming conventions should be standardized early

---

# Lessons Learned

During implementation we observed:

- Spark writes both the data and the schema into Parquet files.
- Glue Crawlers read the Parquet metadata instead of inspecting every individual value.
- The crawler automatically detected the partition:

```
event_date
```

and registered

```
event_date=2026-07-16
```

inside the Glue Data Catalog.

The crawler generated the table from the processed dataset without requiring any manual schema definition.

---

# Future Considerations

Future enhancements may include:

- Apache Iceberg tables
- Glue ETL Jobs
- Glue Data Quality
- Lake Formation integration
- Automated crawler execution through Apache Airflow
- Schema evolution management