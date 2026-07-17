# Processed Layer Schema Design

## Document Purpose

This document defines the schema, transformation rules, grain, partitioning strategy, data-quality expectations, and output design for the processed OpenSky aircraft-state dataset.

The processed layer is created from immutable raw OpenSky API responses stored in Amazon S3.

Its purpose is to convert raw positional arrays into a clean, typed, analytics-ready dataset without introducing business aggregations.

---

# 1. Source Dataset

## Source System

OpenSky Network REST API

## Source Endpoint

```text
/states/all
```

## Raw S3 Location

```text
s3://aviation-operations-data-platform/raw/opensky/states/
```

## Raw Object Structure

Each raw S3 object contains one top-level JSON document.

Example:

```json
{
  "time": 1784188215,
  "states": [
    [
      "39de4f",
      "TVF686M ",
      "France",
      1784188214,
      1784188214,
      0.2578,
      46.3459,
      10172.7,
      false,
      237.89,
      26.73,
      -8.45,
      null,
      10622.28,
      "1000",
      false,
      0
    ]
  ]
}
```

The top-level fields are:

| Field | Description |
|---|---|
| `time` | Unix timestamp representing the OpenSky response time |
| `states` | Array containing aircraft state vectors |

Each element inside `states` contains 17 positional values.

---

# 2. Processed Dataset Purpose

The processed layer converts raw OpenSky aircraft-state payloads into a structured dataset suitable for:

- Apache Spark processing
- Amazon Athena queries
- AWS Glue Data Catalog registration
- Apache Iceberg tables
- Snowflake ingestion
- dbt modelling
- data-quality validation
- future joins with airports, airlines, routes, and weather data

The processed layer must remain source-oriented.

It should not contain business-level aggregations such as:

- airport congestion metrics
- airline performance rankings
- route performance calculations
- daily aircraft counts
- weather impact analysis
- route-level KPIs

Those transformations belong in the curated layer.

---

# 3. Dataset Grain

The grain of the processed dataset is:

> One aircraft state observation per aircraft per OpenSky response.

Each processed row represents one aircraft at one observation point in time.

For example, if a single raw OpenSky response contains 14,130 aircraft state vectors, the processed dataset should produce approximately 14,130 rows, excluding malformed or rejected records.

---

# 4. Processing Responsibilities

The Spark processing job will perform the following operations:

1. Read raw JSON objects from Amazon S3.
2. Extract the top-level OpenSky response timestamp.
3. Explode the `states` array.
4. Create one row per aircraft state vector.
5. Validate that each vector contains exactly 17 fields.
6. Map positional values to meaningful column names.
7. Cast fields into appropriate Spark data types.
8. Convert Unix timestamps into UTC timestamp values.
9. Trim leading and trailing whitespace from callsigns.
10. Add technical metadata.
11. Preserve raw-file lineage.
12. Separate valid and malformed records.
13. Write valid records in Apache Parquet format.
14. Partition the processed dataset by event date.
15. Write rejected records to a dedicated data-quality location.
16. Log raw, valid, and rejected record counts.

The processed layer will not:

- join external datasets;
- aggregate records;
- calculate business KPIs;
- infer airport or route information;
- apply advanced geospatial logic;
- deduplicate observations across historical batches;
- create dimensional models.

---

# 5. Source-to-Target Mapping

| Target Column | Data Type | Nullable | Source | Transformation | Description |
|---|---|---:|---|---|---|
| `icao24` | STRING | No | `state[0]` | Cast to string | Unique 24-bit ICAO aircraft transponder address |
| `callsign` | STRING | Yes | `state[1]` | Cast to string and trim whitespace | Aircraft or flight callsign |
| `origin_country` | STRING | No | `state[2]` | Cast to string | Country in which the aircraft is registered |
| `time_position` | TIMESTAMP | Yes | `state[3]` | Convert Unix seconds to timestamp | Time of the last position update |
| `last_contact` | TIMESTAMP | No | `state[4]` | Convert Unix seconds to timestamp | Time of the aircraft's last contact with OpenSky |
| `longitude` | DOUBLE | Yes | `state[5]` | Cast to double | Longitude in decimal degrees |
| `latitude` | DOUBLE | Yes | `state[6]` | Cast to double | Latitude in decimal degrees |
| `barometric_altitude` | DOUBLE | Yes | `state[7]` | Cast to double | Barometric altitude in metres |
| `on_ground` | BOOLEAN | No | `state[8]` | Cast to boolean | Indicates whether the aircraft is on the ground |
| `velocity` | DOUBLE | Yes | `state[9]` | Cast to double | Ground speed in metres per second |
| `true_track` | DOUBLE | Yes | `state[10]` | Cast to double | Direction of movement in degrees clockwise from north |
| `vertical_rate` | DOUBLE | Yes | `state[11]` | Cast to double | Vertical speed in metres per second |
| `sensors` | ARRAY | Yes | `state[12]` | Preserve as array when available | Sensor identifiers contributing to the state vector |
| `geometric_altitude` | DOUBLE | Yes | `state[13]` | Cast to double | Geometric altitude in metres |
| `squawk` | STRING | Yes | `state[14]` | Cast to string | Transponder squawk code |
| `spi` | BOOLEAN | No | `state[15]` | Cast to boolean | Special-purpose indicator |
| `position_source` | INTEGER | No | `state[16]` | Cast to integer | Source category used for the aircraft position |
| `source_response_time` | TIMESTAMP | No | Top-level `time` | Convert Unix seconds to timestamp | Time when OpenSky generated the API response |
| `ingestion_timestamp` | TIMESTAMP | No | Raw S3 path or processing metadata | Convert to UTC timestamp | Time when the raw file entered the platform |
| `source` | STRING | No | Derived | Set to `opensky` | Source-system identifier |
| `event_date` | DATE | No | Derived from `last_contact` | Extract date | Partition column for processed data |
| `raw_file_path` | STRING | No | Spark input metadata | Capture source object path | Lineage back to the exact raw S3 object |

---

# 6. Positional State-Vector Mapping

The OpenSky state vector follows this positional structure:

| Position | Field |
|---:|---|
| 0 | `icao24` |
| 1 | `callsign` |
| 2 | `origin_country` |
| 3 | `time_position` |
| 4 | `last_contact` |
| 5 | `longitude` |
| 6 | `latitude` |
| 7 | `barometric_altitude` |
| 8 | `on_ground` |
| 9 | `velocity` |
| 10 | `true_track` |
| 11 | `vertical_rate` |
| 12 | `sensors` |
| 13 | `geometric_altitude` |
| 14 | `squawk` |
| 15 | `spi` |
| 16 | `position_source` |

Every valid aircraft state vector is expected to contain exactly:

```text
17 fields
```

Any vector containing fewer or more than 17 values should be treated as malformed.

---

# 7. Data-Type Decisions

## String Fields

The following fields should remain strings:

```text
icao24
callsign
origin_country
squawk
source
raw_file_path
```

`icao24` and `squawk` should not be treated as numbers because:

- they may contain leading zeros;
- they are identifiers rather than measurable quantities;
- numeric calculations are not meaningful for them.

---

## Timestamp Fields

The following Unix timestamp fields should be converted into Spark timestamp values:

```text
time_position
last_contact
source_response_time
```

The source values are measured in seconds since the Unix epoch.

All processed timestamps must use UTC.

---

## Numeric Fields

The following fields should use `DOUBLE`:

```text
longitude
latitude
barometric_altitude
velocity
true_track
vertical_rate
geometric_altitude
```

`DOUBLE` is appropriate because these values may contain decimals and require analytical precision.

---

## Boolean Fields

The following fields should use `BOOLEAN`:

```text
on_ground
spi
```

---

## Integer Fields

The following field should use `INTEGER`:

```text
position_source
```

---

## Sensors Field

The `sensors` field may contain:

```text
null
```

or a list of sensor identifiers.

It should initially be preserved as an array rather than flattened or discarded.

This preserves source information for future use and avoids premature interpretation.

---

# 8. Nullability Rules

OpenSky state vectors may contain null values.

The following fields are expected to be nullable:

```text
callsign
time_position
longitude
latitude
barometric_altitude
velocity
true_track
vertical_rate
sensors
geometric_altitude
squawk
```

The following fields are expected to be present for valid processed records:

```text
icao24
origin_country
last_contact
on_ground
spi
position_source
source_response_time
ingestion_timestamp
source
event_date
raw_file_path
```

A record missing `icao24` or `last_contact` should be considered invalid for the processed layer.

---

# 9. Callsign Cleaning

OpenSky frequently returns callsigns with trailing whitespace.

Example source value:

```text
"TVF686M "
```

Expected processed value:

```text
"TVF686M"
```

The transformation should trim both leading and trailing whitespace.

If the source callsign is null, it must remain null.

---

# 10. Timestamp Semantics

## `time_position`

The time when OpenSky last received a valid geographic position from the aircraft.

This field may be null when no position is available.

## `last_contact`

The most recent time OpenSky received any message from the aircraft.

This is the preferred event timestamp for the first version of the processed dataset.

## `source_response_time`

The timestamp associated with the complete API response returned by OpenSky.

## `ingestion_timestamp`

The timestamp representing when the raw OpenSky response entered the S3 raw layer.

This timestamp is controlled by the ingestion platform.

## `event_date`

The UTC calendar date derived from `last_contact`.

This column is used as the processed-layer partition key.

---

# 11. Partitioning Strategy

## Raw Layer

The raw layer is partitioned by UTC ingestion time:

```text
raw/opensky/states/
year=YYYY/
month=MM/
day=DD/
hour=HH/
```

Example:

```text
raw/opensky/states/year=2026/month=07/day=16/hour=07/
```

The raw partition represents when the platform received the complete API response.

---

## Processed Layer

The processed layer will be partitioned by:

```text
event_date
```

Example:

```text
processed/opensky/states/event_date=2026-07-16/
```

## Why Event-Date Partitioning?

The processed layer supports analytical workloads.

Users are more likely to query:

- aircraft observations on a particular date;
- observations across a date range;
- historical aircraft activity;
- daily operational datasets.

Partitioning by event date allows Spark and Athena to skip unrelated partitions and reduces the amount of data scanned.

---

# 12. Output Format

The processed layer will use:

```text
Apache Parquet
```

## Why Parquet?

Parquet is:

- columnar;
- compressed;
- schema-aware;
- efficient for Apache Spark;
- efficient for Amazon Athena;
- suitable for the AWS Glue Data Catalog;
- compatible with Snowflake;
- more efficient for analytical scans than JSON.

A query such as:

```sql
SELECT
    origin_country,
    COUNT(*)
FROM processed_states
WHERE event_date = DATE '2026-07-16'
GROUP BY origin_country;
```

can read only the required columns and partition instead of scanning every field in the raw JSON files.

---

# 13. Proposed Processed S3 Layout

```text
s3://aviation-operations-data-platform/
└── processed/
    └── opensky/
        └── states/
            ├── event_date=2026-07-15/
            │   ├── part-00000-....snappy.parquet
            │   └── part-00001-....snappy.parquet
            │
            └── event_date=2026-07-16/
                ├── part-00000-....snappy.parquet
                └── part-00001-....snappy.parquet
```

Spark will generate the `part-...parquet` filenames automatically.

The application should not manually assign the final Parquet filenames.

---

# 14. Valid and Invalid Records

## Valid Record

A state vector is valid when:

- it contains exactly 17 values;
- `icao24` is not null;
- `last_contact` is not null;
- timestamp conversion succeeds;
- required fields can be cast into the target types;
- mandatory geographic or operational rules are not violated.

## Invalid Record

A record is invalid when:

- its vector contains fewer or more than 17 values;
- `icao24` is missing;
- `last_contact` is missing;
- mandatory type conversion fails;
- the record cannot be interpreted using the expected OpenSky schema.

---

# 15. Invalid-Record Handling

Malformed records should not silently disappear.

Rejected records should be written to:

```text
data-quality/
└── rejected/
    └── opensky/
        └── states/
```

Suggested partition structure:

```text
data-quality/rejected/opensky/states/
year=YYYY/
month=MM/
day=DD/
hour=HH/
```

Each rejected record should include:

- the raw state-vector value;
- the rejection reason;
- the raw file path;
- the source response time;
- the processing timestamp.

This supports auditing, debugging, and future reprocessing.

---

# 16. Initial Data-Quality Rules

## Structural Validation

```text
state-vector length = 17
```

## Mandatory Fields

```text
icao24 is not null
last_contact is not null
```

## Geographic Validation

```text
latitude between -90 and 90
longitude between -180 and 180
```

when those fields are not null.

## Velocity Validation

```text
velocity >= 0
```

when velocity is not null.

## Track Validation

```text
true_track between 0 and 360
```

when true track is not null.

## Timestamp Validation

```text
last_contact <= source_response_time
```

A small tolerance may be introduced if source behaviour shows minor timing differences.

## Record Reconciliation

The processing job should capture:

```text
raw record count
valid record count
rejected record count
```

The expected relationship is:

```text
raw record count = valid record count + rejected record count
```

---

# 17. Example Processed Record

```json
{
  "icao24": "39de4f",
  "callsign": "TVF686M",
  "origin_country": "France",
  "time_position": "2026-07-16T07:50:14Z",
  "last_contact": "2026-07-16T07:50:14Z",
  "longitude": 0.2578,
  "latitude": 46.3459,
  "barometric_altitude": 10172.7,
  "on_ground": false,
  "velocity": 237.89,
  "true_track": 26.73,
  "vertical_rate": -8.45,
  "sensors": null,
  "geometric_altitude": 10622.28,
  "squawk": "1000",
  "spi": false,
  "position_source": 0,
  "source_response_time": "2026-07-16T07:50:15Z",
  "ingestion_timestamp": "2026-07-16T07:50:17Z",
  "source": "opensky",
  "event_date": "2026-07-16",
  "raw_file_path": "s3://aviation-operations-data-platform/raw/opensky/states/year=2026/month=07/day=16/hour=07/20260716T075017Z.json"
}
```

---

# 18. Processed-Layer Data Contract

The processed dataset guarantees that:

1. Each row represents one aircraft observation.
2. Each valid row has a non-null `icao24`.
3. Each valid row has a non-null `last_contact`.
4. Timestamp columns use UTC.
5. Callsigns are trimmed.
6. Geographic fields remain nullable when unavailable.
7. Source lineage is preserved through `raw_file_path`.
8. The processed dataset is stored in Parquet format.
9. Data is partitioned by `event_date`.
10. Malformed records are separated and auditable.
11. Business aggregations are excluded.
12. The processed layer can be rebuilt completely from the immutable raw layer.

---

# 19. Why Transform with Spark?

The positional mapping could be performed using:

- local Python;
- AWS Lambda;
- Athena SQL;
- AWS Glue;
- Amazon EMR;
- Snowflake `FLATTEN`.

Spark is selected for the processed layer because the future platform must support:

- processing many raw files;
- historical replay;
- distributed transformations;
- schema enforcement;
- data-quality validation;
- partitioned Parquet writes;
- future deduplication;
- joins with additional datasets;
- Apache Iceberg integration;
- larger-scale reprocessing.

The first dataset is small enough for local Python, but the processing architecture is designed for future scale and for AWS data-engineering mastery.

---

# 20. Why Not Transform Before Raw Storage?

Transforming before raw storage would create a traditional ETL flow:

```text
Extract
    ↓
Transform
    ↓
Load
```

The selected architecture follows an ELT-oriented pattern:

```text
Extract
    ↓
Load immutable Raw
    ↓
Transform later
```

Preserving the raw payload provides:

- replayability;
- auditability;
- protection from transformation mistakes;
- flexibility when transformation logic changes;
- support for multiple downstream processing engines;
- an immutable source of truth.

The processed layer is therefore treated as reproducible output rather than original source data.

---

# 21. Downstream Consumers

The processed dataset may later be consumed by:

```text
AWS Glue Data Catalog
Amazon Athena
Apache Iceberg
Amazon EMR
Snowflake
dbt
Power BI
data-quality jobs
curated-layer processing
```

The processed schema should therefore remain stable, typed, documented, and source-oriented.

---

# 22. Future Schema Evolution

Potential future additions include:

```text
processing_timestamp
pipeline_run_id
schema_version
source_file_name
source_partition_year
source_partition_month
source_partition_day
source_partition_hour
aircraft_category
position_source_description
```

These fields should only be introduced when there is a clear technical or analytical requirement.

Significant schema changes must be documented through:

- an updated data contract;
- an Architecture Decision Record;
- updated Spark tests;
- updated Glue or Iceberg metadata;
- downstream compatibility review.

---

# 23. Production Considerations

In a production environment, the processed layer should also consider:

- Apache Iceberg instead of unmanaged Parquet files;
- schema-evolution controls;
- automated compaction;
- duplicate detection;
- late-arriving data handling;
- data-quality thresholds;
- pipeline-run identifiers;
- observability metrics;
- encryption with SSE-KMS where required;
- lifecycle rules;
- Glue Data Catalog integration;
- automated partition registration;
- retry and recovery behaviour;
- CloudWatch monitoring;
- lineage tracking;
- data-retention requirements;
- access-control policies;
- personally identifiable information review, although the current OpenSky payload does not contain conventional personal-data fields.

---

# 24. Acceptance Criteria

The processed Spark job is complete when:

- raw OpenSky JSON is read successfully;
- the `states` array is exploded;
- one row is produced per valid state vector;
- all 17 fields are mapped correctly;
- callsigns are trimmed;
- Unix timestamps are converted into UTC timestamps;
- the source response timestamp is retained;
- ingestion metadata is included;
- raw-file lineage is preserved;
- malformed records are written separately;
- valid records are written as Parquet;
- output is partitioned by `event_date`;
- row counts are reconciled;
- the output can be queried successfully;
- the transformation can be rerun from the immutable raw layer.

---

# 25. Next Implementation Step

The next step is to create the PySpark processing script:

```text
scripts/emr/states_to_parquet.py
```

The script will implement the source-to-target mapping and quality rules defined in this document.

The implementation flow will be:

```text
Read raw JSON
    ↓
Capture raw file path
    ↓
Extract source response time
    ↓
Explode states array
    ↓
Validate vector length
    ↓
Map positional fields
    ↓
Cast data types
    ↓
Convert timestamps
    ↓
Trim callsign
    ↓
Add metadata
    ↓
Separate valid and rejected records
    ↓
Write processed Parquet
    ↓
Write rejected records
    ↓
Log record reconciliation
```