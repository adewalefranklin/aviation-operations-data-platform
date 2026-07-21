# Use Idempotent Iceberg MERGE Processing

## Status

Accepted

## Date

2026-07-21

---

# Context

Airflow tasks may be retried and pipeline runs may process the same OpenSky source object more than once.

A simple append operation would create duplicate records whenever:

- A task is retried
- A DAG is manually rerun
- The same raw file is submitted again
- Processing resumes after a partial failure

The processed table must therefore produce a consistent result regardless of how many times the same batch is processed.

---

# Decision

Use an idempotent Apache Iceberg `MERGE INTO` operation.

Each record receives:

- A stable `record_key` used for matching
- A `payload_hash` used to detect changes

The merge follows these rules:

```text
Matching record key and identical payload
→ Do nothing

Matching record key and changed payload
→ Update the existing record

New record key
→ Insert the record