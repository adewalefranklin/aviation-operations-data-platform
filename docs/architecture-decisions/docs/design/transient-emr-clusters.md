# Use Transient Amazon EMR Clusters

## Status

Accepted

## Date

2026-07-21

---

# Context

The aviation processing workload is currently batch-oriented.

Each pipeline run processes a newly ingested OpenSky JSON object, transforms the aircraft state vectors using Apache Spark, and writes the result into an Apache Iceberg table.

A continuously running EMR cluster would remain idle between executions and generate unnecessary infrastructure cost.

---

# Decision

Create a temporary Amazon EMR cluster for each pipeline execution and terminate it automatically after processing and validation complete.

Apache Airflow is responsible for:

- Creating the EMR cluster
- Waiting until the cluster is ready
- Submitting the Spark step
- Monitoring the Spark step
- Validating the resulting Iceberg table
- Terminating the cluster

---

# Why Transient EMR

Transient clusters were selected because they provide:

- Cost control
- Compute isolation between pipeline runs
- Reproducible cluster configuration
- Automatic cleanup
- Independent scaling of storage and compute
- No idle EMR infrastructure

Amazon S3, Apache Iceberg, and the AWS Glue Data Catalog retain the data independently of the EMR cluster lifecycle.

---

# Alternatives Considered

## Persistent EMR Cluster

A permanent cluster would reduce startup time for frequent workloads.

It was not selected because the current workload does not run frequently enough to justify continuous cluster cost.

## AWS Glue ETL

AWS Glue could run Spark without explicit cluster provisioning.

It was not selected because the project intentionally demonstrates Amazon EMR cluster management and provides greater control over Spark configuration and installed applications.

## Amazon EMR Serverless

EMR Serverless would remove cluster lifecycle management.

It was not selected because transient EMR on EC2 provides stronger hands-on experience with cluster provisioning, instance profiles, Spark steps, YARN logs, and cost management.

---

# Consequences

## Positive

- No idle EMR cluster cost
- Compute is created only when required
- Each run starts from a known configuration
- Failed runs do not affect future clusters
- Data remains available after cluster termination

## Negative

- Cluster startup increases pipeline duration
- IAM roles and networking must be configured correctly
- Logs must be archived before cluster termination
- Repeated cluster creation is inefficient for very frequent jobs