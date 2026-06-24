#SQL Server to Landing (Parquet)

Reads tables from a **Unity Catalog foreign catalog** backed by the on-prem
ADDS SQL Server and lands them as **Parquet files** in an ADLS Gen2 UC Volume.
Watermark-driven incremental load is supported alongside full reload.

## Layout

```
ingestion_code/
├── databricks.yml                  # DAB root config (bundle + targets)
├── resources/
│   └── ingestion_job.yml           # Parameterized job (per-table run)
├── src/
│   └── ingest_sql_to_landing.py    # PySpark landing script
├── conf/
│   └── tables_example.yml          # Example table-level configs
└── README.md
```

## Script parameters

| Parameter | Required | Description |
|---|---|---|
| `--source_catalog` | yes | UC foreign catalog name (e.g. `adds_sql_catalog`) |
| `--source_schema` | yes | Source schema (e.g. `dbo`) |
| `--source_table` | yes | Source table name |
| `--pk_columns` | yes | Comma-separated primary key columns (recorded for Bronze) |
| `--watermark_column` | conditional | Timestamp column. Required when `load_type=incr` |
| `--load_type` | yes | `full` or `incr` |
| `--landing_path` | yes | UC Volume root path for landing parquet |
| `--watermark_state_table` | yes | UC Delta table tracking last watermark per source table |

## Output layout

```
{landing_path}/{schema}/{table}/ingestion_date=YYYY-MM-DD/run_id=<uuid>/*.parquet
```

Each landing run also writes a `_metadata.json` sidecar with the table's
ingestion config (PK, watermark column, load type, row count, watermark value).

## Watermark state

A single Delta table tracks per-source-table watermark progression.
Incremental runs read the latest `SUCCESS` row for the table and filter the
source by `watermark_column > last_watermark_value`. The new high-water mark
is captured before the write and persisted on success.

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

## Run a single table

```bash
databricks bundle run propane_landing_ingestion -t dev \
  --params source_schema=dbo,source_table=Customer,pk_columns=CustomerId,watermark_column=ModifiedDate,load_type=incr
```

## Fan-out across multiple tables

Wrap the `ingest_table` task in a `for_each_task` driven by
`conf/tables_example.yml`. Each iteration re-runs the script with different
table-level parameters. (Pattern shown commented at the bottom of
`resources/ingestion_job.yml`.)
