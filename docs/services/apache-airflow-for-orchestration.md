# Use Apache Airflow for Workflow Orchestration

## Status

Accepted

## Date

2026-07-21

---

# Context

The Aviation Operations Data Platform contains several dependent processing steps:

- Extract aircraft state data from the OpenSky API
- Store raw JSON in Amazon S3
- Validate that new raw data exists
- Provision an Amazon EMR cluster
- Submit an Apache Spark processing job
- Wait for the Spark job to finish
- Validate the Apache Iceberg table
- Terminate the EMR cluster

These operations must run in the correct order and failures must be visible and recoverable.

---

# Decision

Use Apache Airflow to orchestrate the complete aviation data pipeline.

The Airflow DAG manages task dependencies, AWS service calls, validation steps, retries, logging, and EMR cluster lifecycle operations.

Airflow runs locally using Docker Compose while interacting with AWS through IAM credentials.

---

# Why Apache Airflow

Apache Airflow was selected because it provides:

- Explicit task dependencies
- Clear visualisation of pipeline execution
- Retry and failure handling
- AWS provider operators
- EMR cluster and step orchestration
- Task-level logging
- Support for future scheduling and monitoring
- Portability across local and managed environments

---

# Alternatives Considered

## AWS Step Functions

AWS Step Functions could orchestrate AWS services without maintaining an Airflow environment.

It was not selected because the project aims to demonstrate data engineering workflow orchestration using DAGs, operators, sensors, and task dependencies.

## Amazon MWAA

Amazon Managed Workflows for Apache Airflow provides a managed Airflow service.

It was not selected for the current implementation because running Airflow locally is more cost-effective for development and portfolio use.

The DAG can later be migrated to MWAA if a managed production deployment is required.

## Cron Jobs

Cron could schedule the ingestion script.

It was not selected because it does not provide dependency management, task-level retries, workflow visualisation, or EMR lifecycle orchestration.

---

# Consequences

## Positive

- The complete pipeline is visible in one DAG
- Failures can be traced to individual tasks
- EMR provisioning and termination are automated
- AWS operators reduce custom orchestration code
- The workflow can later be migrated to MWAA

## Negative

- Airflow requires its own runtime environment
- Operators and provider packages must remain compatible
- Local Airflow credentials must be managed securely
- Airflow adds operational complexity compared with a simple scheduled script