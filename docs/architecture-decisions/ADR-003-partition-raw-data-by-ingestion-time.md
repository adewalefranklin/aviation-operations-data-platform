# ADR-003: Partition raw OpenSky data by ingestion time

## Status

Accepted

## Context

OpenSky returns a batch containing thousands of aircraft records with different
event timestamps. The platform requires a predictable S3 layout for replay,
monitoring, and downstream processing.

## Decision

Store each raw API response under:

raw/opensky/states/year=YYYY/month=MM/day=DD/hour=HH/

using the UTC ingestion timestamp.

## Alternatives considered

- No partitioning
- Partition by OpenSky source response time
- Partition by each aircraft's event timestamp
- Partition only by day

## Why this decision

- Each API response represents one ingestion batch.
- UTC ingestion time is generated and controlled by our platform.
- Hourly partitions limit the amount of data scanned downstream.
- The layout is simple for EMR, Glue, and Athena to discover.

## Consequences

### Positive

- Predictable object paths
- Easier replay and troubleshooting
- Supports partition pruning
- Separates ingestion time from event time

### Negative

- Frequent ingestion may create many small files
- Later compaction may be required