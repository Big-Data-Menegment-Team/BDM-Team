# BDM-Team

## Quick start

```docker compose up -d```

Then open: 

Jupyter: http://localhost:8888 — token: bdm <br>
Spark UI: http://localhost:4040 (available once a SparkSession is started)

```docker compose down   # when done```

## 1. Correctness
Our cleaning rules:
| # | Rule | Column(s) | Condition kept |
|---|------|-----------|---------------|
| 1 | Non-null timestamps | pickup_datetime, dropoff_datetime | isNotNull() |
| 2 | Chronological trip | both timestamps | dropoff > pickup |
| 3 | Positive distance | trip_distance | > 0 |
| 4 | Valid passenger count | passenger_count | > 0 |
| 5 | Positive fare | total_amount, fare_amount | > 0 |
| 6 | Valid location IDs | PULocationID, DOLocationID | isNotNull() |

### Deduplication

Duplicate rows are removed using the following composite key. Two real trips cannot share the same origin, destination, exact times, and distance:

- `pickup_datetime`
- `dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `trip_distance`
- `VendorID`

#### Row counts: 
Rows input: 7052769 <br>
Rows after clean: 5472446 (removed 1580323) <br>
Rows after dedup: 5472446 (all removed during the cleaning step) <br>

#### 'Bad Rows' examples: 
![cleaning example](img/cleaning_example.png)

**Example 1: `trip_distance <= 0`** - Trips with zero or negative distance are physically invalid and are filtered out. These often appear as cancelled or test entries.

**Example 2: `passenger_count < 1`** - Rows with no passengers recorded are dropped, as they do not represent a real trip.

**Example 3: `dropoff_datetime <= pickup_datetime`** - Rows where the drop-off is not strictly after pickup are removed. This catches both same-timestamp entries and cases where dropoff precedes pickup.

### A Manifest File 
The manifest file can be found in the `state` folder (what is created on the first job).

Structure of the file: 
- `trip_files`: A list of trip data files that have been ingested.
    - `filename`: Name of the parquet file.
    - `file_size`: Size of the file in bytes at the time of ingestion. Used to detect duplicates or changes.
    - `processed_at`: Timestamp of when the file was processed.

How It Works:
1. Check for New Files
    - The ETL job scans the `data/inbox/` folder.
    - Files not listed in `trip_files` (matched by filename and file size) are considered new and read for processing.
2. Process and Clean
    - Only the new files are read, cast to the correct schema, cleaned, deduplicated, enriched, and appended to the output dataset.
3. Update Manifest
    - After a successful write, each new file's metadata is added to `trip_files`.
    - The manifest is updated only after the output has been written, so a failed run does not mark files as processed.

Idempotency is preserved by the manifest:
- Already processed files are skipped on rerun (matched by filename + file size).
- Rerunning with no new files prints a skip message and does not touch any output.
- The output only grows when new files are added.

### Output Datasets

Enriched trips are written (append mode, partitioned by `pickup_month`) to:
```
data/outbox/trips_enriched.parquet
```

Required fields included in the output:

| Field | Description |
|-------|-------------|
| `pickup_datetime`, `dropoff_datetime` | Cast timestamps |
| `PULocationID`, `DOLocationID` | Integer location IDs |
| `pickup_zone`, `pickup_borough` | Joined from zone lookup |
| `dropoff_zone`, `dropoff_borough` | Joined from zone lookup |
| `passenger_count`, `trip_distance` | Cleaned numeric fields |
| `trip_duration_minutes` | Derived: `(dropoff - pickup) / 60`, rounded to 2 decimal places |
| `pickup_date` | Derived: date part of `pickup_datetime` |
| `pickup_month` | Derived: `yyyy-MM` format, used for partitioning |
| `source_file` | Input filename via `input_file_name()` |
| `ingested_at` | Timestamp of the current run |

Zone enrichment is applied via `taxi_zone_lookup.parquet` using two explicit `broadcast` joins, one for pickup and one for dropoff, to avoid shuffles on the small lookup table.

## 2. Performance

> Full job runtime: **XX.XX s**

### Spark UI Screenshots

Two Spark Web UI screenshots are included in the repository:
- One showing total job / stage time
- One showing shuffle read / write or spill for the join or aggregation stage

### Optimization Choices

1. first choice here
2. second choice here

## 3. Scenario
`Maintain data/outbox/monthly_borough_summary.parquet that is updated incrementally: on each run, compute stats only for newly ingested months and append to the summary. Do not recompute from the full dataset. README must explain the append strategy and how idempotency is preserved.`

### Append Strategy

The monthly borough summary is built only from the current incremental batch, never from the full historical dataset:

1. From `df_output` (the enriched batch), extract the distinct `pickup_month` values present in this run.
2. If `monthly_borough_summary.parquet` already exists, read its existing distinct `pickup_month` values.
3. Compute `months_to_process` as a `left_anti` join, keeping only months in the new batch that are not already in the summary.
4. Filter `df_output` to those new months only, then aggregate:
   - `trip_count` - total trips
   - `total_revenue` - sum of `total_amount`
   - `avg_trip_distance` - mean trip distance
   - `avg_trip_duration_minutes` - mean trip duration
5. Append the result (partitioned by `pickup_month`) to `monthly_borough_summary.parquet`.

If no new months are found, the summary is left unchanged.

### Idempotency for the Monthly Summary

- **File-level:** Already processed files are skipped by the manifest, so their months never re-enter the pipeline.
- **Month-level:** Even if the manifest were bypassed, the `left_anti` join on `pickup_month` against the existing summary makes sure a month that is already present is never aggregated or appended again.
- **No new files -> no change:** If `has_new_files` is `False`, the entire summary block is skipped.

> Note: the summary is append-only. A month is summarized the first time its data appears in a new batch. If a future file contains additional rows for an already-summarized month, those rows are excluded from the summary (the month is filtered out by the `left_anti` join). This is a deliberate trade-off to keep the pipeline append-only and avoid full recomputation. For example, when we processed the 2025-01 and 2025-02 files, they also contained 21 rows for 2024-12. So 2024-12 was summarized at that point. When we later added the actual 2024-12 file, the summary was not updated since that month was already present. This is expected behavior. One solution would be to clean the files based on the file names but we do not know whether the file naming structure is always the same.