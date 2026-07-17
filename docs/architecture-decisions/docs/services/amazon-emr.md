# Use Amazon EMR for Distributed Data Processing

## Status

Accepted

## Date

2026-07-17

---

# Context

The Aviation Operations Data Platform requires a scalable distributed processing engine capable of transforming large raw aviation datasets stored in Amazon S3 into optimized analytical datasets.

The processing layer must support:

- distributed processing
- Apache Spark
- scalable execution
- integration with Amazon S3
- processing large datasets
- fault tolerance
- parallel execution
- future Apache Iceberg support
- future AWS Glue Data Catalog integration
- SQL analytics through Amazon Athena
- optional Snowflake integration

The processing layer should remain independent of the storage layer.

---

# Options Considered

## Option 1 — Amazon EMR ✅

Amazon EMR (Elastic MapReduce) is AWS's managed big data platform for running distributed frameworks such as Apache Spark, Hadoop, Hive, Flink, Trino and others.

### Advantages

- Fully managed Apache Spark environment
- Native integration with Amazon S3
- Easily scales from small development clusters to large production clusters
- Managed installation of Spark, Hadoop and YARN
- Supports distributed data processing
- Supports automatic scaling
- Supports logging to Amazon S3 and CloudWatch
- Supports execution of PySpark scripts stored in Amazon S3
- Supports future Apache Iceberg integration
- Integrates with AWS Glue Data Catalog
- Integrates with Amazon Athena
- Integrates with IAM roles for secure access
- Suitable for enterprise ETL and ELT workloads

### Disadvantages

- Cluster startup time
- More expensive than serverless options for very small workloads
- Requires cluster management
- Costs continue while the cluster is running

---

## Option 2 — Local Apache Spark

Run Spark locally using a desktop or laptop.

### Why Not

- Limited by local hardware
- Does not represent production environments
- Additional Hadoop and S3 connector configuration required
- Difficult to simulate distributed execution
- Less suitable for learning AWS-native services

---

## Option 3 — AWS Glue

Serverless Apache Spark ETL service.

### Why Not

- Higher level abstraction hides Spark infrastructure
- Less control over cluster configuration
- Primary learning objective is mastering Spark on EMR
- EMR better demonstrates distributed computing concepts

---

## Option 4 — AWS Lambda

Serverless function execution.

### Why Not

- Execution time limitations
- Memory limitations
- Not designed for distributed Spark processing
- Unsuitable for large-scale data transformations

---

## Option 5 — Amazon ECS

Container orchestration platform.

### Why Not

- Designed for containerized applications
- Spark cluster management would need additional configuration
- Greater operational complexity for this learning project

---

# Decision

Amazon EMR will serve as the distributed processing layer for the Aviation Operations Data Platform.

The platform will use Amazon EMR to execute Apache Spark jobs that transform raw aviation data stored in Amazon S3 into optimized Parquet datasets.

The cluster configuration for this project is:

```
Amazon EMR 7.13.0

Primary Node
    1 × m5.xlarge

Core Node
    1 × m5.xlarge

Task Nodes
    None
```

The Spark application will execute directly from Amazon S3.

```
scripts/emr/
```

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
Parquet
        │
        ▼
Athena / Snowflake
```

Storage remains separated from compute.

---

# Implementation

The following implementation decisions were made.

## Cluster Configuration

- Amazon EMR Version 7.13.0
- Uniform Instance Groups
- One Primary Node
- One Core Node
- No Task Nodes
- Amazon Linux operating system

---

## Instance Types

Primary Node

```
m5.xlarge
```

Core Node

```
m5.xlarge
```

Task Nodes

```
None
```

The chosen configuration provides sufficient resources for development while minimizing infrastructure complexity.

---

## Logging

Cluster logs are written to:

```
s3://aviation-operations-data-platform/emr-logs/
```

This keeps all project assets inside the same S3 data lake.

---

## IAM Roles

Service Role

```
EMR_DefaultRole_V2
```

Used by Amazon EMR to provision and manage cluster resources.

EC2 Instance Profile

```
EMR_EC2_DefaultRole
```

Attached to every EC2 instance within the cluster.

Additional project-specific permissions were granted through:

```
AviationPlatformEMRS3AccessPolicy
```

This policy allows Spark to:

- read processing scripts
- read raw aviation data
- write processed datasets
- write rejected records

---

## Auto Termination

Automatic cluster termination is enabled after a period of inactivity.

This reduces unnecessary infrastructure costs during development.

---

## Networking

The cluster uses:

- Default Amazon VPC
- Default subnet
- Default security groups

The cluster communicates directly with Amazon S3 through IAM-based authentication.

---

## Processing Scripts

Spark applications are stored separately from the data.

```
scripts/
    emr/
        states_to_parquet.py
```

The EMR cluster downloads and executes these scripts directly from Amazon S3.

---

# Why Amazon EMR?

Amazon EMR was selected because it allows the platform to process large datasets using distributed Apache Spark while remaining tightly integrated with the AWS analytics ecosystem.

Unlike local Spark execution, Amazon EMR provides a production-like environment where compute resources, security, networking and distributed execution can all be managed using AWS services.

This aligns with the project's objective of mastering enterprise cloud data engineering.

---

# Consequences

## Positive

- Managed Apache Spark environment
- Distributed processing
- Native S3 integration
- Fault-tolerant execution
- Production-like architecture
- Easy horizontal scaling
- Integrated monitoring
- Secure IAM integration
- Supports future Iceberg implementation
- Supports future Glue Catalog integration

## Negative

- Cluster startup delay
- Running clusters incur compute costs
- Requires cluster lifecycle management
- More components than serverless ETL

---

# Future Considerations

Future enhancements include:

- Apache Iceberg table format
- Spark job orchestration with Apache Airflow
- EMR Step automation
- Terraform-based cluster deployment
- Spot Instance integration
- EMR Managed Scaling
- CloudWatch monitoring and alerts
- EMR Serverless evaluation
- AWS Glue Data Catalog integration
- Athena querying over processed datasets