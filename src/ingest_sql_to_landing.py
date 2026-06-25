"""
Ingest a SQL Server table from a Unity Catalog foreign catalog into the
landing zone as Parquet files.

Supports two load modes:
  - full : read the entire source table and overwrite the landing partition
  - incr : read only rows where <watermark_column> > last successful watermark
           (tracked in a UC Delta state table per source table)

The script is intended to be invoked by the parameterized DAB job in
resources/ingestion_job.yml. Each run lands one source table; multiple
tables are fanned out via the for_each_task pattern (see job YAML).

Output layout:
  {landing_path}/{schema}/{table}/ingestion_date=YYYY-MM-DD/run_id=<uuid>/*.parquet
  plus a sibling _metadata.json describing the run.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="SQL Server -> Landing Parquet ingestion")
    p.add_argument("--source_catalog", required=True,
                   help="UC foreign catalog name for SQL Server (e.g. adds_sql_catalog)")
    p.add_argument("--source_schema", required=True,
                   help="Source schema name (e.g. dbo)")
    p.add_argument("--source_table", required=True,
                   help="Source table name")
    p.add_argument("--pk_columns", required=True,
                   help="Comma-separated primary key columns (recorded for Bronze)")
    p.add_argument("--watermark_column", required=False, default="",
                   help="Timestamp column used for incremental loads")
    p.add_argument("--load_type", required=True, choices=["full", "incr"],
                   help="Load type: full or incr")
    p.add_argument("--landing_path", required=True,
                   help="UC Volume base path for landing parquet files")
    p.add_argument("--watermark_state_table", required=True,
                   help="UC Delta table storing last watermark per source table")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# State table helpers
# ---------------------------------------------------------------------------

def ensure_state_table(spark: SparkSession, state_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {state_table} (
            source_catalog         STRING,
            source_schema          STRING,
            source_table           STRING,
            pk_columns             STRING,
            watermark_column       STRING,
            last_watermark_value   TIMESTAMP,
            rows_loaded            BIGINT,
            load_timestamp         TIMESTAMP,
            load_type              STRING,
            load_status            STRING,
            run_id                 STRING,
            landing_path           STRING,
            error_message          STRING
        )
        USING DELTA
    """)


def get_last_watermark(spark: SparkSession, state_table: str,
                       src_catalog: str, src_schema: str, src_table: str):
    row = spark.sql(f"""
        SELECT MAX(last_watermark_value) AS last_wm
        FROM {state_table}
        WHERE source_catalog = '{src_catalog}'
          AND source_schema  = '{src_schema}'
          AND source_table   = '{src_table}'
          AND load_status    = 'SUCCESS'
    """).first()
    return row.last_wm if row and row.last_wm is not None else None


def write_state(spark: SparkSession, state_table: str, **row) -> None:
    df = spark.createDataFrame([row])
    df.write.mode("append").saveAsTable(state_table)


# ---------------------------------------------------------------------------
# Sidecar metadata
# ---------------------------------------------------------------------------

def write_metadata_sidecar(spark: SparkSession, target_path: str, payload: dict) -> None:
    """Write a single-line JSON metadata file alongside the landed parquet."""
    sidecar = f"{target_path}/_metadata.json"
    rdd = spark.sparkContext.parallelize([json.dumps(payload, default=str)], numSlices=1)
    # coalesce(1) ensures a single text file; we use overwrite semantics via path uniqueness
    rdd.saveAsTextFile(sidecar)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    args = parse_args(argv)

    if args.load_type == "incr" and not args.watermark_column:
        raise ValueError("watermark_column is required when load_type=incr")

    spark = (
        SparkSession.builder
        .appName(f"landing-ingest-{args.source_schema}.{args.source_table}")
        .getOrCreate()
    )

    run_id = str(uuid.uuid4())
    load_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    ingestion_date = load_ts.strftime("%Y-%m-%d")

    src_fqn = f"`{args.source_catalog}`.`{args.source_schema}`.`{args.source_table}`"
    if args.load_type == "full":
        # Full loads land at a single "current" location and overwrite the
        # previous snapshot on every run. No history of past full loads is
        # retained in landing (the state table still records each run).
        landing_target = (
            f"{args.landing_path}/{args.source_schema}/{args.source_table}"
            f"/load_type=full"
        )
    else:  # incr
        # Incremental runs are partitioned by ingestion_date and run_id so
        # every successful run is its own folder under landing.
        landing_target = (
            f"{args.landing_path}/{args.source_schema}/{args.source_table}"
            f"/load_type=incr/ingestion_date={ingestion_date}/run_id={run_id}"
        )

    print(f"[ingest] start  src={src_fqn}")
    print(f"[ingest] target {landing_target}")
    print(f"[ingest] load_type={args.load_type}  pk={args.pk_columns}  wm_col={args.watermark_column}")

    ensure_state_table(spark, args.watermark_state_table)

    new_wm = None
    last_wm = None
    row_count = 0

    try:
        NUM_PARTITIONS = int(args.num_partitions)
        pk_cols = [c.strip() for c in args.pk_columns.split(",") if c.strip()]

        if NUM_PARTITIONS > 1 and pk_cols:
            if len(pk_cols) == 1:
                hash_expr = f"ABS(CHECKSUM([{pk_cols[0]}]))"
            else:
                concat = " + '||' + ".join(f"CAST([{c}] AS NVARCHAR(MAX))" for c in pk_cols)
                hash_expr = f"ABS(CHECKSUM({concat}))"

            predicates = [
                f"{hash_expr} % {NUM_PARTITIONS} = {i}"
                for i in range(NUM_PARTITIONS)
            ]
            print(f"[ingest] partitioned read with {NUM_PARTITIONS} predicates: {predicates[0]} ...")

            df = spark.read.jdbc(
                url=jdbc_url,
                table=f"[{args.source_schema}].[{args.source_table}]",
                predicates=predicates,
                properties={
                    "user":      jdbc_user,
                    "password":  jdbc_password,
                    "driver":    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "fetchsize": "10000",
                },
            )
        else:
            df = spark.read.table(src_fqn)   # fall back to single-threaded federation read

        if args.load_type == "incr":
            last_wm = get_last_watermark(
                spark, args.watermark_state_table,
                args.source_catalog, args.source_schema, args.source_table,
            )
            if last_wm is None:
                print("[ingest] no prior watermark found — initial incremental load (full pull this run)")
            else:
                print(f"[ingest] applying incremental filter: {args.watermark_column} > {last_wm}")
                df = df.filter(F.col(args.watermark_column) > F.lit(last_wm))

        # Audit columns
        df = (
            df
            .withColumn("_ingest_run_id",        F.lit(run_id))
            .withColumn("_ingest_load_ts",       F.lit(load_ts))
            .withColumn("_ingest_load_type",     F.lit(args.load_type))
            .withColumn("_ingest_source_table",
                        F.lit(f"{args.source_catalog}.{args.source_schema}.{args.source_table}"))
        )

        # Capture the upper-bound watermark from the filtered dataframe BEFORE writing
        if args.watermark_column:
            wm_row = df.agg(F.max(F.col(args.watermark_column)).alias("max_wm")).first()
            new_wm = wm_row.max_wm if wm_row and wm_row.max_wm is not None else last_wm

        df.cache()
        row_count = df.count()
        print(f"[ingest] rows to land: {row_count}")

        if row_count > 0:
            (
                df.write
                  .mode("overwrite")
                  .format("parquet")
                  .save(landing_target)
            )
            sidecar_payload = {
                "source_catalog":   args.source_catalog,
                "source_schema":    args.source_schema,
                "source_table":     args.source_table,
                "pk_columns":       args.pk_columns,
                "watermark_column": args.watermark_column or None,
                "load_type":        args.load_type,
                "rows_loaded":      row_count,
                "last_watermark":   str(last_wm) if last_wm else None,
                "new_watermark":    str(new_wm) if new_wm else None,
                "run_id":           run_id,
                "load_timestamp":   load_ts.isoformat(),
                "landing_path":     landing_target,
            }
            write_metadata_sidecar(spark, landing_target, sidecar_payload)
        else:
            print("[ingest] no rows to land — skipping write")

        write_state(
            spark, args.watermark_state_table,
            source_catalog       = args.source_catalog,
            source_schema        = args.source_schema,
            source_table         = args.source_table,
            pk_columns           = args.pk_columns,
            watermark_column     = args.watermark_column or None,
            last_watermark_value = new_wm if new_wm is not None else last_wm,
            rows_loaded          = int(row_count),
            load_timestamp       = load_ts,
            load_type            = args.load_type,
            load_status          = "SUCCESS",
            run_id               = run_id,
            landing_path         = landing_target,
            error_message        = None,
        )
        print(f"[ingest] SUCCESS  rows={row_count}  new_wm={new_wm}")
        return 0

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        print(f"[ingest] FAILED  {err_msg}", file=sys.stderr)
        write_state(
            spark, args.watermark_state_table,
            source_catalog       = args.source_catalog,
            source_schema        = args.source_schema,
            source_table         = args.source_table,
            pk_columns           = args.pk_columns,
            watermark_column     = args.watermark_column or None,
            last_watermark_value = None,
            rows_loaded          = int(row_count),
            load_timestamp       = load_ts,
            load_type            = args.load_type,
            load_status          = "FAILED",
            run_id               = run_id,
            landing_path         = landing_target,
            error_message        = err_msg,
        )
        raise


if __name__ == "__main__":
    sys.exit(main())
