# ADR-002: Preserve Raw OpenSky Payloads Without Modification

## Status

Accepted

## Date

2026-07-15

---

# Context

The OpenSky API returns aircraft state vectors in their original source format.

Initially, the project transformed these state vectors into dictionaries before storage.

After architectural review, the decision was reconsidered.

The platform should preserve the original API response before any business or structural transformation.

---

# Problem

Should the ingestion pipeline transform the data before loading into the data lake?

Or should it preserve the exact API response?

---

# Options Considered

## Option 1 — Transform Before Loading (ETL)

```
Extract
    ↓
Transform
    ↓
Load
```

### Advantages

- Cleaner files
- Easier manual inspection
- Immediate readability

### Disadvantages

- Original source payload is lost
- Python transformation logic becomes part of ingestion
- Historical replay becomes difficult
- Changes in transformation logic require re-extraction

---

## Option 2 — Preserve Raw Payload (ELT) ✅

```
Extract
    ↓
Load Raw
    ↓
Transform Later
```

### Advantages

- Source data remains immutable
- Full replay capability
- Easier auditing
- Future transformations can be rebuilt
- Spark, Glue, Snowflake and dbt all operate from the same source
- Supports schema evolution

### Disadvantages

- Raw files are less readable
- Additional transformation stage required

---

# Decision

The Aviation Operations Data Platform will preserve the OpenSky API response exactly as received.

No structural transformation will occur before the raw data is written into Amazon S3.

Transformations will occur after ingestion using distributed processing technologies such as:

- Apache Spark on Amazon EMR
- AWS Glue
- Snowflake
- dbt

---

# Rationale

The raw layer represents the single source of truth.

Every downstream dataset can be recreated from this layer.

Separating ingestion from transformation increases flexibility and reduces coupling between application logic and analytical processing.

---

# Consequences

## Positive

- Immutable source layer
- Better auditability
- Easier recovery
- Supports multiple downstream consumers
- Future-proof against transformation changes

## Negative

- Additional storage required
- Raw data is not immediately human-readable
- Transformation becomes a separate processing step

---

# Future Processing Flow

```
OpenSky API
        │
        ▼
Python Extract
        │
        ▼
Amazon S3 Raw
        │
        ▼
EMR Spark
        │
        ▼
Processed Layer
        │
        ▼
Curated Layer
        │
        ▼
Athena / Snowflake / dbt
```

---

# Lessons Learned

During the design phase, the project initially implemented light transformation within the Python application.

After evaluating replayability, auditability and future schema evolution, preserving the immutable raw payload was determined to be the stronger architectural decision.

The Python transformation module remains valuable for:

- local experimentation
- unit testing
- schema understanding

However, production transformations belong in the processing layer rather than the ingestion layer.