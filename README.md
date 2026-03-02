# BDM-Team

## Quick start

```docker compose up -d```

Then open: 

Jupyter: http://localhost:8888 — token: bdm <br>
Spark UI: http://localhost:4040 (available once a SparkSession is started)

```docker compose down   # when done```

## A Manifest File 
The manifest file can be found in the `state` folder (what is created on the first job).

Structure of the file: 
- `trip_files`: A list of trip data files that have been ingested.
    - `filename`: Name of the parquet file.
    - `file_size`: Size of the file in bytes at the time of ingestion. Used to detect duplicates or changes.
    - `processed_at`: Timestamp of when the file was processed.
- `lookup_loaded`: Boolean flag indicating if reference data (taxi zone lookup) has been loaded. Prevents reloading static data unnecessarily.

How It Works:
1. Check for New Files
    - The ETL job scans the data/inbox/ folder.
    - Files not listed in trip_files are considered new and read for processing.
2. Process and Clean
    - Only the new files are cleaned, cast to the proper schema, and appended to the final dataset.
3. Update Manifest
    - After processing, each new file’s metadata is added to trip_files.
    - If reference tables were loaded during this run, lookup_loaded is set to true.
