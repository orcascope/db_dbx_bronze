from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# API hard limit: max readings returned per page (documented).
PAGE_SIZE = 10000

# Default odata DataService endpoints, primary first then secondary.
DEFAULT_BASE_URLS = ",".join([
    "https://company.odata.ca/public/api/v1/DataService.svc",
    "https://telematics.odatanetwork.com:4431/v1.0/DataService.svc",
    "https://company2.odata.ca/public/api/v1/DataService.svc",
    "https://telematics02.odatanetwork.com:4431/v1.0/DataService.svc",
])


# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="odata API -> Landing Parquet ingestion")
    p.add_argument("--source_schema", required=False, default="odata",
                   help="Logical schema for the landing layout (default: odata)")
    p.add_argument("--source_table", required=False, default="tanklevels",
                   help="Logical table name for the landing layout (default: tanklevels)")
    p.add_argument("--start_date_utc", required=True,
                   help="ISO8601 UTC window start, e.g. 2018-04-25T08:30:00Z")
    p.add_argument("--end_date_utc", required=False, default="",
                   help="ISO8601 UTC window end (default: now, UTC)")
    p.add_argument("--device_id", required=True,
                   help="Device id for the /devices/{id}/tanklevels path")
    p.add_argument("--api_base_urls", required=False, default=DEFAULT_BASE_URLS,
                   help="Comma-separated DataService.svc base URLs, primary first")
    p.add_argument("--kv_scope", required=False, default="",
                   help="Databricks secret scope holding the API key")
    p.add_argument("--kv_secret_name_apikey", required=False, default="odata-api-key",
                   help="Secret key name for the odata API key")
    p.add_argument("--landing_path", required=True,
                   help="UC Volume base path for landing parquet files")
    p.add_argument("--request_timeout", required=False, default="60",
                   help="Per-request timeout in seconds (default: 60)")
    p.add_argument("--max_pages", required=False, default="10000",
                   help="Safety cap on pages fetched per run (default: 10000)")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

_API_FMT = "%Y-%m-%dT%H:%M:%SZ"


def parse_iso_utc(s: str) -> datetime:
    """Parse an ISO8601 UTC string into an aware UTC datetime."""
    s = s.strip()
    try:
        return datetime.strptime(s, _API_FMT).replace(tzinfo=timezone.utc)
    except ValueError:
        # Tolerate offsets / fractional seconds via fromisoformat (handles Z on 3.11+).
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def fmt_api(dt: datetime) -> str:
    """Format an aware datetime as the API's ISO8601 UTC string."""
    return dt.astimezone(timezone.utc).strftime(_API_FMT)


def to_naive_utc(dt: datetime) -> datetime:
    """Strip tzinfo for the Spark TIMESTAMP audit columns."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Secret / dbutils helpers
# ---------------------------------------------------------------------------

def get_api_key(spark: SparkSession, scope: str, key_name: str) -> str:
    """Resolve the API key: env override first (local dev), else Databricks secret."""
    env = os.environ.get("odata_API_KEY")
    if env:
        return env.strip()
    if not scope:
        raise RuntimeError(
            "No API key: set odata_API_KEY, or pass --kv_scope with a Databricks "
            "secret scope containing --kv_secret_name_apikey."
        )
    try:
        from pyspark.dbutils import DBUtils  # available on Databricks clusters
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=scope, key=key_name)
    except Exception as exc:  # noqa: BLE001 — surface a clear, actionable message
        raise RuntimeError(
            f"Could not read API key from secret {scope}/{key_name}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    """A session with retry/backoff on transient statuses and connect errors."""
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.5,               # 0s, 1.5s, 3s, 6s ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("https://", adapter)
    return s


def http_get_json(session, base_urls, path, params, headers, timeout):
    """GET `path` against each base URL in order; return parsed JSON from the
    first server that answers 200. Auth / bad-request statuses fail fast."""
    last_err = None
    for base in base_urls:
        url = base.rstrip("/") + path
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            last_err = exc
            print(f"[odata] transport error on {base}: {exc} — trying next server")
            continue

        if r.status_code == 200:
            return r.json()
        if r.status_code in (401, 403):
            raise PermissionError(
                f"{r.status_code} auth failed ({url}): API key missing/invalid — {r.text[:200]}"
            )
        if r.status_code in (400, 404):
            raise ValueError(
                f"{r.status_code} bad request ({url}): check startDateUtc/endDateUtc/id — {r.text[:200]}"
            )
        # 5xx / 429 that survived Retry, or anything unexpected: try the next server.
        last_err = RuntimeError(f"{r.status_code} from {url}: {r.text[:200]}")
        print(f"[odata] {last_err} — trying next server")

    raise RuntimeError(f"All servers failed for {path}: {last_err}")


def fetch_all_readings(session, base_urls, path, base_params, headers, timeout, max_pages):
    """Page from index 0 until a short/empty page is returned (last-page signal).
    Each page's JSON payload is the readings array. Returns
    (readings_list, pages_fetched)."""
    readings = []
    page = 0
    while page < max_pages:
        params = dict(base_params, page=page)
        payload = http_get_json(session, base_urls, path, params, headers, timeout)
        batch = payload
        got = len(batch)
        readings.extend(batch)
        print(f"[odata] page={page} readings={got} (running total={len(readings)})")

        if got < PAGE_SIZE:                    # short or empty page = last page
            return readings, page + 1
        page += 1

    print(f"[odata] hit --max_pages={max_pages}; stopping (there may be more data)")
    return readings, page


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    args = parse_args(argv)

    base_urls = [u.strip() for u in args.api_base_urls.split(",") if u.strip()]
    if not base_urls:
        raise ValueError("--api_base_urls resolved to an empty list")

    spark = (
        SparkSession.builder
        .appName(f"odata-ingest-{args.source_schema}.{args.source_table}")
        .getOrCreate()
    )

    run_id = str(uuid.uuid4())
    load_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    # ---- Resolve the [start, end] window (caller-supplied every run) ---------
    if not args.start_date_utc:
        raise ValueError("--start_date_utc is required")
    start_dt = parse_iso_utc(args.start_date_utc)
    end_dt = parse_iso_utc(args.end_date_utc) if args.end_date_utc \
        else datetime.now(timezone.utc)
    if start_dt >= end_dt:
        raise ValueError(f"empty window: start {fmt_api(start_dt)} >= end {fmt_api(end_dt)}")

    # ---- Build request ------------------------------------------------------
    path = f"/devices/{args.device_id}/tanklevels"
    base_params = {
        "startDateUtc": fmt_api(start_dt),
        "endDateUtc":   fmt_api(end_dt),
    }
    api_key = get_api_key(spark, args.kv_scope, args.kv_secret_name_apikey)
    headers = {
        "Accept": "application/json; charset=utf-8",
        "Authorization": f"Bearer {api_key}",
    }

    landing_target = f"{args.landing_path}/{args.source_schema}/{args.source_table}"

    print(f"[odata] start  table={args.source_schema}.{args.source_table}  device={args.device_id}")
    print(f"[odata] window {base_params['startDateUtc']} .. {base_params['endDateUtc']}")
    print(f"[odata] target {landing_target}")

    session = build_session()
    readings, pages = fetch_all_readings(
        session, base_urls, path, base_params, headers,
        timeout=int(args.request_timeout), max_pages=int(args.max_pages),
    )
    row_count = len(readings)
    print(f"[odata] fetched {row_count} readings across {pages} page(s)")

    if row_count == 0:
        print("[odata] no readings in window — nothing to land")
        return 0

    # Shape JSON readings -> DataFrame via Spark's JSON reader (robust to nested
    # objects), then land as parquet (overwrite) like the SQL ingestion.
    json_lines = [json.dumps(r, default=str) for r in readings]
    rdd = spark.sparkContext.parallelize(json_lines)
    df = spark.read.json(rdd)

    df = (
        df
        .withColumn("_ingest_run_id",       F.lit(run_id))
        .withColumn("_ingest_load_ts",      F.lit(load_ts))
        .withColumn("_ingest_source",       F.lit(f"odata.{args.source_table}"))
        .withColumn("_ingest_window_start", F.lit(to_naive_utc(start_dt)))
        .withColumn("_ingest_window_end",   F.lit(to_naive_utc(end_dt)))
    )

    (
        df.write
          .mode("overwrite")
          .format("parquet")
          .save(landing_target)
    )

    print(f"[odata] SUCCESS  rows={row_count}  pages={pages}  target={landing_target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
