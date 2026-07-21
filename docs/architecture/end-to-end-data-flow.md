# End-to-End Data Flow

## Overview

The Aviation Operations Data Platform follows an **Extract, Load, Transform (ELT)** architecture where raw OpenSky flight data is first preserved in Amazon S3 before being processed into a curated Apache Iceberg table.

Apache Airflow orchestrates the complete workflow while Amazon EMR provides distributed Apache Spark processing. Metadata is managed through the AWS Glue Data Catalog and the processed data can be queried directly using Amazon Athena.

---

# Complete Pipeline

```text
                   OpenSky Network API
                            │
                            ▼
                Python Ingestion Pipeline
                            │
                            ▼
                 Amazon S3 Raw Landing Zone


===========================================================
             Apache Airflow Workflow Orchestration
===========================================================

                            │
                            ▼
                  Validate Raw S3 Data
                            │
                            ▼
                  Create EMR Cluster
                            │
                            ▼
              Apache Spark on Amazon EMR
                            │
                            ▼
             Data Validation & Transformation
                            │
                            ▼
                     Apache Iceberg Table
                            │
                            ▼
                  AWS Glue Data Catalog
                            │
                            ▼
                      Amazon Athena
                            │
                            ▼
                     Analytics / BI
```

---

# Step 1 – Data Extraction

The pipeline begins by authenticating against the OpenSky Network API using OAuth2 Client Credentials.

The ingestion application retrieves the latest aircraft state vectors and converts the API response into structured JSON.

**Output**

- OpenSky JSON response
- Aircraft state vectors
- Response timestamp

---

# Step 2 – Raw Data Landing

The extracted JSON payload is stored unchanged inside Amazon S3.

The raw layer preserves the original source data to support:

- Reprocessing
- Auditing
- Traceability
- Recovery

The data is partitioned using the ingestion timestamp.

Example:

```text
raw/
└── opensky/
    └── states/
        └── year=2026/
            └── month=07/
                └── day=21/
                    └── hour=10/
```

---

# Step 3 – Workflow Orchestration

Apache Airflow orchestrates the complete platform.

Rather than processing data itself, Airflow coordinates the execution of each stage.

Pipeline tasks include:

- Run OpenSky ingestion
- Validate raw S3 data
- Create Amazon EMR cluster
- Wait for cluster readiness
- Submit Spark processing job
- Wait for Spark completion
- Validate Apache Iceberg table
- Terminate the EMR cluster

This ensures processing occurs in the correct order and that compute resources are automatically cleaned up.

---

# Step 4 – Distributed Processing

Amazon EMR provisions a temporary cluster that executes Apache Spark.

Spark performs the following operations:

- Read raw JSON from Amazon S3
- Explode aircraft state arrays
- Validate record structure
- Reject malformed records
- Transform positional arrays into analytical columns
- Generate record keys
- Generate payload hashes
- Prepare data for Apache Iceberg

---

# Step 5 – Apache Iceberg

The processed dataset is written into an Apache Iceberg table.

Rather than appending duplicate records, the platform performs an idempotent `MERGE INTO` operation.

The merge logic follows three rules:

- Matching record and identical payload → No action
- Matching record and changed payload → Update
- New record → Insert

This allows the pipeline to be safely rerun without creating duplicate records.

---

# Step 6 – Metadata Management

The Iceberg table is registered inside the AWS Glue Data Catalog.

The Glue Catalog stores:

- Database metadata
- Table definitions
- Schema information
- Iceberg metadata location

This allows multiple AWS services to discover and query the dataset.

---

# Step 7 – Query Layer

Amazon Athena queries the Apache Iceberg table directly from Amazon S3.

No data movement into a warehouse is required.

Typical use cases include:

- Data validation
- Ad hoc SQL queries
- Operational reporting
- Downstream analytics

---

# End-to-End Workflow Summary

| Stage | Technology | Purpose |
|--------|------------|----------|
| Authentication | OpenSky OAuth2 | Secure API access |
| Extraction | Python | Retrieve aircraft state data |
| Raw Storage | Amazon S3 | Preserve immutable raw data |
| Orchestration | Apache Airflow | Coordinate workflow execution |
| Distributed Processing | Amazon EMR + Apache Spark | Validate and transform data |
| Curated Storage | Apache Iceberg | Store processed analytical data |
| Metadata | AWS Glue Data Catalog | Register tables and schemas |
| Query Layer | Amazon Athena | Query Iceberg tables using SQL |

---

# Design Principles

The platform was designed around several key engineering principles:

- Preserve immutable raw data
- Separate storage from compute
- Use distributed processing for scalability
- Automate workflow orchestration
- Minimise cloud cost using transient compute
- Ensure idempotent processing
- Centralise metadata management
- Enable serverless querying

---

# Result

The completed platform demonstrates a modern cloud-native data engineering architecture capable of ingesting, processing, cataloguing, and analysing aviation telemetry using AWS managed services and open data lake technologies.