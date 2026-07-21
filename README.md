# Aviation Operations Data Platform

An end-to-end cloud-native aviation data platform built on AWS that ingests live aircraft state data from the OpenSky Network API, stores immutable raw data in Amazon S3, transforms it using Apache Spark on Amazon EMR, stores curated datasets in Apache Iceberg with the AWS Glue Data Catalog, and orchestrates the entire workflow using Apache Airflow.

---

# Project Overview

This project demonstrates how a modern data engineering platform can ingest, process, catalogue, and manage aviation telemetry using a scalable ELT architecture.

The platform follows cloud data engineering best practices:

- Preserve immutable raw data
- Perform transformations with distributed Spark processing
- Store analytical datasets in Apache Iceberg
- Register metadata using AWS Glue Data Catalog
- Orchestrate workflows with Apache Airflow
- Automatically provision and terminate compute resources to minimise cloud cost

---

# Pipeline Architecture

```
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

# Technology Stack

| Category | Technologies |
|-----------|-------------|
| Programming | Python 3 |
| Cloud Platform | AWS |
| Data Lake | Amazon S3 |
| Distributed Processing | Amazon EMR |
| Processing Engine | Apache Spark |
| Table Format | Apache Iceberg |
| Metadata | AWS Glue Data Catalog |
| Orchestration | Apache Airflow |
| Query Engine | Amazon Athena |
| Authentication | OAuth2 |
| Version Control | Git & GitHub |
| Containerisation | Docker |

---

# Project Structure

```
aviation-operations-data-platform/
│
├── airflow/
│   └── aviation_operations_pipeline.py
│
├── docs/
│   ├── architecture-decisions/
│   └── screenshots/
│
├── scripts/
│   ├── emr/
│   │   └── states_to_iceberg.py
│   └── ingestion/
│
├── src/
│   ├── authentication/
│   ├── extract/
│   ├── load/
│   ├── pipeline/
│   └── config/
│
├── tests/
│
├── Dockerfile
├── requirements.txt
└── README.md
```

---

# Data Pipeline

## Step 1 — Authentication

The platform authenticates against the OpenSky Network API using OAuth2 Client Credentials.

---

## Step 2 — Data Extraction

Aircraft state vectors are extracted from the OpenSky API.

Each API response contains:

- Response timestamp
- Aircraft state vectors
- Position
- Speed
- Altitude
- Heading
- Flight identifiers

---

## Step 3 — Raw Data Storage

The original API response is stored unchanged inside Amazon S3.

Example:

```
raw/
└── opensky/
    └── states/
        └── year=2026/
            └── month=07/
                └── day=21/
                    └── hour=07/
```

The platform follows an ELT architecture by preserving the original source data.

---

## Step 4 — Airflow Orchestration

Apache Airflow orchestrates the complete workflow.

Pipeline:

```
Start

↓

Run OpenSky Ingestion

↓

Validate Raw S3 Data

↓

Create EMR Cluster

↓

Wait for EMR

↓

Submit Spark Job

↓

Wait for Spark Completion

↓

Validate Iceberg Table

↓

Terminate EMR Cluster

↓

End
```

---

## Step 5 — Spark Processing

Amazon EMR executes Apache Spark to:

- Read raw JSON
- Explode aircraft state arrays
- Validate records
- Reject malformed records
- Transform positional arrays into analytical columns
- Generate business keys
- Calculate payload hashes
- Prepare analytical datasets

---

## Step 6 — Data Quality

The platform validates:

- State vector length
- Missing values
- Invalid aircraft records

Rejected records are written to a dedicated S3 location for auditing.

---

## Step 7 — Apache Iceberg

Spark writes the curated dataset into Apache Iceberg.

The platform supports:

- ACID transactions
- Schema evolution
- Time travel
- Hidden partitioning
- Incremental MERGE operations

---

## Step 8 — Glue Catalog

The Iceberg table is automatically registered inside the AWS Glue Data Catalog.

This allows downstream services such as Athena to discover the dataset without additional configuration.

---

## Step 9 — Query Layer

Amazon Athena can query the Iceberg tables directly without moving data into a warehouse.

---

# Repository Highlights

## Python

- Object-Oriented Design
- Logging
- Exception handling
- Configuration management
- Environment variables

---

## AWS

- Amazon S3
- Amazon EMR
- AWS Glue
- Amazon Athena
- IAM

---

## Data Engineering

- ELT Architecture
- Distributed Processing
- Apache Spark
- Apache Iceberg
- Data Validation
- Idempotent Processing

---

## Workflow Orchestration

- Apache Airflow DAGs
- EMR lifecycle automation
- Pipeline validation
- Automatic cluster termination

---

# Screenshots

## Airflow DAG

```
docs/screenshots/sample_airflow_dag_run.png
```

---

## End-to-End Successful Orchestration

```
docs/screenshots/end-to-end_airflow_orchestration_sample.png
```

---

## Iceberg Table

```
docs/screenshots/iceberg_table_sample_query.png
```

---

## Athena Query

```
docs/screenshots/athena_sample_query_result.png
```

---

# Key Features

- OAuth2 API Authentication
- ELT Data Pipeline
- Immutable Raw Data Storage
- Distributed Spark Processing
- Apache Iceberg Data Lake
- AWS Glue Data Catalog Integration
- Data Quality Validation
- Idempotent MERGE Processing
- Apache Airflow Orchestration
- Automatic EMR Cluster Lifecycle Management

---

# Learning Outcomes

This project demonstrates practical implementation of:

- Cloud-native data engineering
- Distributed data processing
- Modern data lake architecture
- Workflow orchestration
- Infrastructure cost optimisation
- Apache Iceberg implementation
- Metadata management with AWS Glue
- Production-style pipeline design

---

# Future Enhancements

- Infrastructure provisioning using Terraform
- CI/CD with GitHub Actions
- Automated unit and integration testing
- Data observability and monitoring
- Incremental partition optimisation
- Production deployment using Amazon MWAA