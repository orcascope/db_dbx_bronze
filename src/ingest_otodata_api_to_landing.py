"""
Pull tank-level readings from the odata DataService REST API and store the raw
JSON pages in the landing zone. Plain Python — no Spark.

Source API:
  GET {base}/devices/{id}/tanklevels?startDateUtc={iso}&endDateUtc={iso}&page={n}

Auth:
  API key sent as an HTTP header  ->  Authorization: Bearer <key>
  Read from the ODATA_API_KEY environment variable. On Databricks, wire that
  env var to a secret in the job config (no dbutils needed), e.g.
    ODATA_API_KEY: {{secrets/<scope>/odata-api-key}}

Paging:
  The API caps each page at 10000 readings and exposes no total-count field, so
  we page from index 0 upward and stop when a page returns fewer than page_size
  readings (a short or empty page = the last page). --max_pages is a safety cap.

Server failover:
  --api_base_urls is an ordered, comma-separated list (primary first). Each
  request tries the servers in order; transient failures (connect errors, 5xx,
  429) fall through to the next server. Auth (401/403) and bad-request (400/404)
  responses fail fast.

Output:
  One raw JSON file per page, overwriting any prior pages for the same device:
    {landing_path}/{source_schema}/{source_table}/device_id={device_id}/page=NNNNN.json
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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
    p = argparse.ArgumentParser(description="odata API -> Landing (raw JSON) ingestion")
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
    p.add_argument("--landing_path", required=True,
                   help="Base path for the landing JSON files")
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


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_api_key() -> str:
    """Read the API key from the ODATA_API_KEY environment variable."""
    key = os.environ.get("ODATA_API_KEY", "").strip()
    if not key:
        raise RuntimeError("ODATA_API_KEY is not set (env var holding the odata API key)")
    return key


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


# ---------------------------------------------------------------------------
# Landing
# ---------------------------------------------------------------------------

def prepare_landing_dir(landing_target: str) -> None:
    """Create the target dir and remove any page files from a previous run."""
    os.makedirs(landing_target, exist_ok=True)
    for old in glob.glob(f"{landing_target}/page=*.json"):
        os.remove(old)


def fetch_and_store(session, base_urls, path, base_params, headers,
                    timeout, max_pages, landing_target):
    """Page from index 0, writing each page's raw JSON to its own file, until a
    short/empty page is returned. Returns (total_readings, pages_written)."""
    total = 0
    page = 0
    while page < max_pages:
        params = dict(base_params, page=page)
        payload = http_get_json(session, base_urls, path, params, headers, timeout)
        got = len(payload)

        with open(f"{landing_target}/page={page:05d}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f)

        total += got
        print(f"[odata] page={page} readings={got} (running total={total})")

        if got < PAGE_SIZE:                    # short or empty page = last page
            return total, page + 1
        page += 1

    print(f"[odata] hit --max_pages={max_pages}; stopping (there may be more data)")
    return total, page


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    args = parse_args(argv)

    base_urls = [u.strip() for u in args.api_base_urls.split(",") if u.strip()]
    if not base_urls:
        raise ValueError("--api_base_urls resolved to an empty list")

    # ---- Resolve the [start, end] window (caller-supplied every run) ---------
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
    headers = {
        "Accept": "application/json; charset=utf-8",
        "Authorization": f"Bearer {get_api_key()}",
    }

    landing_target = (
        f"{args.landing_path}/{args.source_schema}/{args.source_table}"
        f"/device_id={args.device_id}"
    )

    print(f"[odata] start  table={args.source_schema}.{args.source_table}  device={args.device_id}")
    print(f"[odata] window {base_params['startDateUtc']} .. {base_params['endDateUtc']}")
    print(f"[odata] target {landing_target}")

    prepare_landing_dir(landing_target)

    session = build_session()
    total, pages = fetch_and_store(
        session, base_urls, path, base_params, headers,
        timeout=int(args.request_timeout), max_pages=int(args.max_pages),
        landing_target=landing_target,
    )

    print(f"[odata] SUCCESS  readings={total}  pages={pages}  target={landing_target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
