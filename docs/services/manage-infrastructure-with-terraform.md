# ADR-005: Manage Infrastructure Using Terraform

## Status

Accepted

## Date

2026-07-21

---

# Context

The Aviation Operations Data Platform requires cloud infrastructure to support data ingestion, storage, processing, metadata management, and security.

Provisioning these resources manually through the AWS Management Console is error-prone, difficult to reproduce, and challenging to maintain across environments.

The project also combines:

- Existing AWS resources that were created manually during development.
- New infrastructure created specifically for the platform.

A consistent Infrastructure as Code (IaC) approach is therefore required.

---

# Decision

Terraform is used as the Infrastructure as Code tool for managing AWS infrastructure.

The implementation follows three Terraform patterns:

## 1. Data Sources

Existing resources are referenced without being managed.

Examples:

- Existing Amazon S3 bucket
- AWS account information

These resources remain outside Terraform ownership.

---

## 2. Imported Resources

Existing infrastructure that should become managed is imported into Terraform state.

Example:

- AWS Glue Catalog Database

Terraform now manages the resource without recreating it.

---

## 3. Managed Resources

New infrastructure is provisioned entirely through Terraform.

Examples:

- EMR EC2 IAM Role
- EMR Instance Profile
- EMR Data Access Policy
- IAM Policy Attachment

---

# Consequences

## Advantages

- Reproducible infrastructure
- Version-controlled cloud resources
- Idempotent deployments
- Infrastructure review through Git
- Consistent environments
- Reduced manual configuration
- Easier disaster recovery

## Trade-offs

- Terraform state must be managed carefully.
- IAM permissions are required for provisioning.
- Existing resources must be imported before management.

---

# Result

Terraform manages the core infrastructure required by the Aviation Operations Data Platform while allowing previously created AWS resources to be adopted safely through Terraform state.