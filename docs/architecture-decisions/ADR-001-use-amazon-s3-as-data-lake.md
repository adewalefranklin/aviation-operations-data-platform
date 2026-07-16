# ADR-001: Use Amazon S3 as the Data Lake

## Status

Accepted

## Date

2026-07-15

---

# Context

The Aviation Operations Data Platform requires a central storage layer for aircraft data collected from the OpenSky Network API.

The platform must support:

- immutable raw data storage
- scalable data processing
- historical replay
- integration with Apache Spark (EMR)
- integration with AWS Glue Data Catalog
- SQL querying through Athena
- optional integration with Snowflake
- low storage cost
- long-term retention

The storage layer should remain independent of the compute layer.

---

# Options Considered

## Option 1 — Amazon S3 ✅

Object storage designed for data lakes.

### Advantages

- Virtually unlimited scalability
- 11 nines (99.999999999%) durability
- Very low storage cost
- Native integration with EMR
- Native integration with Glue
- Native integration with Athena
- Supports lifecycle policies
- Supports versioning
- Easy integration with Snowflake External Stages
- Decouples storage from compute

### Disadvantages

- Object storage only
- Not suitable for transactional workloads
- Small-file problems must be managed

---

## Option 2 — Amazon EBS

Block storage attached to EC2 instances.

### Why Not

- Tightly coupled to compute
- Not designed as a shared data lake
- Limited scalability compared to S3

---

## Option 3 — Amazon EFS

Shared network file system.

### Why Not

- Higher cost
- Designed for shared POSIX file systems
- Not optimized for analytical data lakes

---

## Option 4 — Amazon RDS

Relational database.

### Why Not

- Expensive for raw API storage
- Not suitable for large immutable datasets
- Scaling characteristics differ from a data lake

---

## Option 5 — Amazon DynamoDB

NoSQL key-value database.

### Why Not

- Optimized for operational workloads
- Not intended for storing historical raw API files
- Poor fit for analytical processing

---

# Decision

Amazon S3 will serve as the primary storage layer for the Aviation Operations Data Platform.

The platform will store data using a layered architecture.

```
raw/
processed/
curated/
scripts/
```

Compute services will remain independent of storage.

---

# Consequences

## Positive

- Durable storage
- Cost-effective
- Supports replay
- Native AWS analytics integration
- Easy expansion
- Supports future Snowflake integration

## Negative

- Object storage is immutable
- Small files require later optimization
- Partition strategy must be carefully designed

---

# Future Considerations

Future enhancements include:

- Apache Iceberg tables
- Lifecycle management
- Intelligent Tiering
- Data compaction
- Cross-region replication