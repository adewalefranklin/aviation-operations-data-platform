# Amazon Athena

**Status:** Implemented  
**Project:** Aviation Operations Data Platform  
**Service:** Amazon Athena  
**Architecture Layer:** Serverless Query Engine

---

# Overview

Amazon Athena is the serverless SQL query engine used within the Aviation Operations Data Platform to analyze processed flight data stored in Amazon S3.

Rather than loading data into a traditional database, Athena queries the processed Parquet files directly from the data lake using metadata stored in the AWS Glue Data Catalog.

This enables interactive analytics without managing database servers or warehouse clusters.

---

# Problem Statement

After processing raw OpenSky flight data with Apache Spark, we required a way to:

- Validate transformed datasets
- Explore flight information interactively
- Perform SQL analytics
- Avoid provisioning another database
- Minimize infrastructure costs

The processed datasets already existed inside Amazon S3 as partitioned Parquet files.

Creating another database simply for querying would duplicate storage and increase operational overhead.

---

# Options Considered

## Option 1 — Amazon Redshift

Advantages

- Excellent analytical performance
- Persistent warehouse
- Columnar storage
- BI friendly

Disadvantages

- Requires running a cluster
- Continuous compute cost
- Additional data loading step
- More infrastructure management

---

## Option 2 — Query Raw JSON Directly

Advantages

- Very easy

Disadvantages

- Expensive
- Large scans
- Poor performance
- No optimization
- Difficult schema management

---

## Option 3 — Amazon Athena (Chosen)

Advantages

- Completely serverless
- No cluster management
- SQL interface
- Queries S3 directly
- Integrates with Glue Catalog
- Pay only when queries execute

This matched the project's goal of building a scalable and cost-efficient data lake.

---

# Architecture Position

```
                     OpenSky API
                          │
                          ▼
                Python Data Ingestion
                          │
                          ▼
                  Amazon S3 (RAW JSON)
                          │
                          ▼
                 Apache Spark on EMR
                          │
                          ▼
             Partitioned Parquet Files
                          │
                          ▼
                AWS Glue Data Catalog
                          │
                          ▼
                  Amazon Athena
                          │
                          ▼
              SQL Analytics & Validation
```

Athena is the final consumer of the processed analytical data.

---

# Why Athena Was Selected

Athena allows SQL queries directly against files stored in S3.

Instead of copying data into another system, Athena reads:

- location
- schema
- partitions

from the Glue Catalog.

Therefore:

Storage remains in S3.

Only compute is temporary.

---

# Glue Catalog Integration

Athena itself does not discover schemas.

Instead it relies on the AWS Glue Data Catalog.

The Glue Crawler automatically created:

- database
- table
- schema
- partition metadata

Athena simply uses this metadata during query execution.

Flow:

```
S3 Parquet
      │
      ▼
Glue Catalog
      │
      ▼
Athena SQL
```

---

# Query Execution

Example:

```sql
SELECT
    icao24,
    callsign,
    velocity_m_s
FROM opensky_states
WHERE event_date='2026-07-16';
```

Athena:

1. Reads metadata from Glue
2. Locates only the matching partition
3. Reads only the requested columns
4. Returns the result

---

# Why Parquet Was Important

The Spark job converts JSON into Parquet.

Benefits:

- Columnar storage
- Compression
- Faster scanning
- Lower Athena cost

Instead of reading every field from JSON:

```
{
  dozens of attributes...
}
```

Athena only reads:

```
icao24

callsign

velocity_m_s
```

This dramatically reduces scanned bytes.

---

# Why Partitioning Was Important

Spark writes:

```
processed/

    event_date=2026-07-16/

    event_date=2026-07-17/

    event_date=2026-07-18/
```

When querying:

```sql
WHERE event_date='2026-07-16'
```

Athena ignores every other folder.

Without partitioning:

```
Scan all folders
```

With partitioning:

```
Scan one folder only
```

This improves both speed and cost.

---

# Athena Pricing

Athena pricing is based on:

Amount of data scanned

NOT

- number of rows
- execution time
- result size

Therefore:

Good:

```sql
SELECT
    icao24,
    velocity_m_s
FROM opensky_states
WHERE event_date='2026-07-16';
```

Bad:

```sql
SELECT *
FROM opensky_states;
```

especially on very large datasets.

---

# Lessons Learned

## Lesson 1

Always avoid:

```sql
SELECT *
```

unless exploring a dataset.

---

## Lesson 2

Parquet significantly reduces Athena costs.

---

## Lesson 3

Partitioning reduces scanned data.

---

## Lesson 4

Glue Catalog simplifies metadata management.

---

## Lesson 5

Athena should primarily query processed analytical datasets rather than raw ingestion data.

---

# Comparison with Previous Financial Project

Financial Platform

```
Raw Data

↓

Athena / Redshift
```

Result

- higher scans
- warehouse costs
- more manual work

---

Aviation Platform

```
Raw JSON

↓

Spark

↓

Partitioned Parquet

↓

Glue

↓

Athena
```

Benefits

- cheaper
- cleaner
- scalable
- easier maintenance
- reusable datasets

This architecture represents a significant improvement over the earlier implementation.

---

# Scalability

Athena scales automatically.

No servers need provisioning.

As data grows:

```
100 MB

↓

10 GB

↓

500 GB

↓

Several TB
```

Athena continues querying the same S3 data lake.

Only query cost changes based on scanned data.

---

# Maintainability

Athena remains simple because each service has a single responsibility.

Python

Extracts data

Spark

Transforms data

S3

Stores data

Glue

Maintains metadata

Athena

Queries data

This separation greatly simplifies debugging and future enhancements.

---

# Future Improvements

Future production enhancements may include:

- Apache Iceberg
- Partition Projection
- Lake Formation
- Fine-grained permissions
- Materialized Views
- BI integrations
- Automated query monitoring

---

# Key Takeaways

✔ Serverless SQL engine

✔ Queries S3 directly

✔ Uses Glue Catalog metadata

✔ No warehouse management

✔ Works best with Parquet

✔ Partition-aware

✔ Cost based on data scanned

✔ Excellent for ad hoc analytics

✔ Highly scalable

✔ Production-ready architecture

---

# Final Decision

Amazon Athena was selected as the analytical query engine because it provides a serverless, scalable, and cost-efficient method of querying processed Parquet datasets stored in Amazon S3.

Combined with Apache Spark and the AWS Glue Data Catalog, Athena completes the modern AWS data lake architecture without requiring an always-running analytical warehouse.

This design aligns with cloud-native data engineering best practices by separating storage, processing, metadata management, and analytics into independent, scalable components.