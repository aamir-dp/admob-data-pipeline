#!/usr/bin/env python3
import os
import csv
from datetime import date, timedelta, datetime

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from google.cloud import storage, bigquery

# ─── PICK REPORT DATE ──────────────────────────────────────────────────────────
raw = os.getenv("REPORT_DATE") or os.getenv("INPUT_RUN_DATE")
if raw:
    report_date = datetime.strptime(raw, "%Y-%m-%d").date()
else:
    # default to yesterday UTC
    report_date = date.today() - timedelta(days=1)

# ─── WHICH APPS TO REPORT ───────────────────────────────────────────────────────
APP1 = os.getenv("APP1")
APP2 = os.getenv("APP2")
if not APP1 or not APP2:
    raise RuntimeError("Missing required env vars APP1 and/or APP2")
APP_LIST = [APP1, APP2]

# ─── REQUIRED ENV VARS ──────────────────────────────────────────────────────────
required = {
    "ADMOB_CLIENT_ID":     os.getenv("ADMOB_CLIENT_ID"),
    "ADMOB_CLIENT_SECRET": os.getenv("ADMOB_CLIENT_SECRET"),
    "ADMOB_REFRESH_TOKEN": os.getenv("ADMOB_REFRESH_TOKEN"),
    "ADMOB_PUBLISHER_ID":  os.getenv("ADMOB_PUBLISHER_ID"),
    "GCP_PROJECT":         os.getenv("GCP_PROJECT"),
    "GCS_BUCKET_NAME":     os.getenv("GCS_BUCKET_NAME"),
    "BQ_DATASET":          os.getenv("BQ_DATASET"),
    "BQ_TABLE_NETWORK":    os.getenv("BQ_TABLE_NETWORK"),
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

CLIENT_ID       = required["ADMOB_CLIENT_ID"]
CLIENT_SECRET   = required["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN   = required["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID    = required["ADMOB_PUBLISHER_ID"].split('/')[-1]  # allow either "123" or "accounts/123"
PROJECT         = required["GCP_PROJECT"]
BUCKET_NAME     = required["GCS_BUCKET_NAME"]
DATASET_NAME    = required["BQ_DATASET"]
TABLE_NAME      = required["BQ_TABLE_NETWORK"]

API_SCOPE = "https://www.googleapis.com/auth/admob.report"

# ─── AUTH ───────────────────────────────────────────────────────────────────────
def get_admob_creds():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[API_SCOPE],
    )
    creds.refresh(Request())
    return creds

def build_service(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

# ─── METRIC/DIMENSION HELPERS ─────────────────────────────────────────────────
def get_int(mv: dict, key: str) -> int:
    d = mv.get(key, {}) or {}
    # integerValue or microsValue
    if d.get("integerValue") is not None:
        return int(d["integerValue"])
    if d.get("microsValue") is not None:
        return int(d["microsValue"])
    # fallback decimalValue or value
    for fld in ("decimalValue", "value"):
        if d.get(fld) is not None:
            try:
                return int(float(d[fld]))
            except:
                pass
    return 0

def get_float(mv: dict, key: str) -> float:
    d = mv.get(key, {}) or {}
    # doubleValue
    if d.get("doubleValue") is not None:
        return float(d["doubleValue"])
    # fallback decimalValue or value
    for fld in ("decimalValue", "value"):
        if d.get(fld) is not None:
            try:
                return float(d[fld])
            except:
                pass
    return 0.0

def disp(dims: dict, key: str) -> str:
    dv = dims.get(key, {}) or {}
    return dv.get("displayLabel") or dv.get("value") or ""

# ─── FETCH & WRITE CSV ─────────────────────────────────────────────────────────
def fetch_and_write_network_csv(service, publisher_id: str, report_date: date, local_path: str) -> str:
    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year,  "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year,  "month": report_date.month, "day": report_date.day},
        },
        "dimensions": ["DATE", "APP", "FORMAT", "AD_UNIT"],
        "metrics": [
            "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
            "IMPRESSION_CTR", "MATCHED_REQUESTS", "MATCH_RATE", "IMPRESSION_RPM", "SHOW_RATE"
        ],
        "sortConditions": [{"dimension": "DATE", "order": "ASCENDING"}],
        "dimensionFilters": [
            {
                "dimension": "APP",
                "matchesAny": {"values": APP_LIST}
            }
        ]
    }

    response = service.accounts().networkReport().generate(
        parent=f"accounts/{publisher_id}",
        body={"reportSpec": spec}
    ).execute()

    with open(local_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "date", "app_name", "format", "ad_unit_name",
            "ad_requests", "clicks", "estimated_earnings_micros", "impressions",
            "impression_ctr", "matched_requests", "match_rate", "impression_rpm", "show_rate"
        ])

        for chunk in response:
            row = chunk.get("row")
            if not row:
                continue
            dims = row.get("dimensionValues", {})
            mets = row.get("metricValues", {})

            raw = dims.get("DATE", {}).get("value", "")
            iso_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}" if len(raw) == 8 else raw

            writer.writerow([
                iso_date,
                disp(dims, "APP"),
                disp(dims, "FORMAT"),
                disp(dims, "AD_UNIT"),
                get_int(mets, "AD_REQUESTS"),
                get_int(mets, "CLICKS"),
                get_int(mets, "ESTIMATED_EARNINGS"),
                get_int(mets, "IMPRESSIONS"),
                get_float(mets, "IMPRESSION_CTR"),
                get_int(mets, "MATCHED_REQUESTS"),
                get_float(mets, "MATCH_RATE"),
                get_float(mets, "IMPRESSION_RPM"),
                get_float(mets, "SHOW_RATE"),
            ])

    print(f"Wrote CSV to {local_path}")
    return local_path

# ─── UPLOAD TO GCS ─────────────────────────────────────────────────────────────
def upload_to_gcs(local_path: str, bucket_name: str) -> str:
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path, content_type="text/csv")
    uri = f"gs://{bucket_name}/{os.path.basename(local_path)}"
    print(f"Uploaded {local_path} → {uri}")
    return uri

# ─── DELETE EXISTING ROWS FOR THE DATE ──────────────────────────────────────────
def delete_existing_date(client: bigquery.Client, project: str, dataset: str, table: str, report_date: date):
    """
    Deletes any rows in project.dataset.table whose date = report_date.
    """
    table_fq = f"`{project}.{dataset}.{table}`"
    sql = f"""
      DELETE FROM {table_fq}
      WHERE date = '{report_date.isoformat()}'
    """
    query_job = client.query(sql)
    query_job.result()  # wait for deletion to finish
    print(f"Deleted rows for date = {report_date} from {table_fq}")

# ─── LOAD INTO BQ (WITH DELETE FIRST) ──────────────────────────────────────────
def load_csv_to_bq(gcs_uri: str, project: str, dataset: str, table: str, report_date: date):
    client = bigquery.Client(project=project)

    # 1) DELETE any existing rows for this date (so we don’t append duplicates)
    delete_existing_date(client, project, dataset, table, report_date)

    # 2) Configure the LoadJob as before (WRITE_APPEND)
    table_ref = client.dataset(dataset).table(table)
    job_config = bigquery.LoadJobConfig(
        source_format       = bigquery.SourceFormat.CSV,
        skip_leading_rows   = 1,
        autodetect          = False,
        write_disposition   = bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition  = bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    # 3) Actually load the CSV from GCS
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # wait for load to finish
    print(f"Appended data into {project}.{dataset}.{table} for date {report_date}")

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    creds   = get_admob_creds()
    service = build_service(creds)

    local_csv = f"network_{report_date:%Y%m%d}.csv"
    fetch_and_write_network_csv(service, PUBLISHER_ID, report_date, local_csv)
    gcs_uri = upload_to_gcs(local_csv, BUCKET_NAME)
    load_csv_to_bq(gcs_uri, PROJECT, DATASET_NAME, TABLE_NAME, report_date)

if __name__ == "__main__":
    main()