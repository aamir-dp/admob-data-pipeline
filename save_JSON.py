import os
import json
from datetime import date, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage, bigquery

# ─── CONFIG & VALIDATION ───────────────────────────────────────────────────────
required = {
    "ADMOB_CLIENT_ID":     os.getenv("ADMOB_CLIENT_ID"),
    "ADMOB_CLIENT_SECRET": os.getenv("ADMOB_CLIENT_SECRET"),
    "ADMOB_REFRESH_TOKEN": os.getenv("ADMOB_REFRESH_TOKEN"),
    "ADMOB_PUBLISHER_ID":  os.getenv("ADMOB_PUBLISHER_ID"),
    "GCP_PROJECT":         os.getenv("GCP_PROJECT"),
    "GCS_BUCKET_NAME":     os.getenv("GCS_BUCKET_NAME"),
    "BQ_DATASET":          os.getenv("BQ_DATASET"),
    "BQ_TABLE":            os.getenv("BQ_TABLE"),
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

CLIENT_ID      = required["ADMOB_CLIENT_ID"]
CLIENT_SECRET  = required["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN  = required["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID   = required["ADMOB_PUBLISHER_ID"]
PROJECT        = required["GCP_PROJECT"]
BUCKET_NAME    = required["GCS_BUCKET_NAME"]
DATASET        = required["BQ_DATASET"]
TABLE          = required["BQ_TABLE"]

API_SCOPE = "https://www.googleapis.com/auth/admob.report"

# ─── AD MOB AUTH & CLIENT ──────────────────────────────────────────────────────
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

def build_admob_service(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

# ─── FETCH & SAVE RAW JSON ─────────────────────────────────────────────────────
def fetch_and_save_raw_json(service, publisher_id, report_date):
    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year, "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year, "month": report_date.month, "day": report_date.day},
        },
        "dimensions": [
            "DATE",
            "APP", "AD_UNIT",
            "AD_SOURCE", "AD_SOURCE_INSTANCE", "MEDIATION_GROUP",
            "COUNTRY"
        ],
        "metrics": [
            "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
            "IMPRESSION_CTR", "MATCHED_REQUESTS", "MATCH_RATE", "OBSERVED_ECPM"
        ],
        "sortConditions": [{"dimension": "DATE", "order": "ASCENDING"}]
    }

    response = service.accounts().mediationReport().generate(
        parent=f"accounts/{publisher_id}",
        body={"reportSpec": spec}
    ).execute()

    filename = f"mediation_{report_date:%Y%m%d}_raw.json"
    with open(filename, "w") as f:
        json.dump(response, f, indent=2)
    print(f"Wrote raw API response to {filename}")
    return filename

# ─── UPLOAD TO GCS ───────────────────────────────────────────────────────────────
def upload_to_gcs(local_path, bucket_name):
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path)
    uri = f"gs://{bucket_name}/{os.path.basename(local_path)}"
    print(f"Uploaded {local_path} to {uri}")
    return uri

# ─── LOAD INTO BIGQUERY ─────────────────────────────────────────────────────────
def load_jsonl_to_bq(
    gcs_uri: str,
    project: str,
    dataset: str,
    table: str,
    use_explicit_schema: bool = False
):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)

    if use_explicit_schema:
        schema = [
            bigquery.SchemaField("date",                      "DATE"),
            bigquery.SchemaField("app",                       "STRING"),
            bigquery.SchemaField("ad_unit",                   "STRING"),
            bigquery.SchemaField("ad_source",                 "STRING"),
            bigquery.SchemaField("ad_source_instance",        "STRING"),
            bigquery.SchemaField("mediation_group",           "STRING"),
            bigquery.SchemaField("country",                   "STRING"),
            bigquery.SchemaField("ad_requests",               "INT64"),
            bigquery.SchemaField("clicks",                    "INT64"),
            bigquery.SchemaField("estimated_earnings_micros","INT64"),
            bigquery.SchemaField("impressions",               "INT64"),
            bigquery.SchemaField("impression_ctr",            "FLOAT64"),
            bigquery.SchemaField("matched_requests",          "INT64"),
            bigquery.SchemaField("match_rate",                "FLOAT64"),
            bigquery.SchemaField("observed_ecpm_micros",      "INT64"),
        ]
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition="WRITE_APPEND",
        )
    else:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
        )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {project}.{dataset}.{table}")

# ─── MAIN FLOW ─────────────────────────────────────────────────────────────────
def main():
    creds       = get_admob_creds()
    service     = build_admob_service(creds)
    report_date = date.today() - timedelta(days=1)

    # 1) Fetch & save the raw JSON
    local_file = fetch_and_save_raw_json(service, PUBLISHER_ID, report_date)

    # 2) Upload that file to GCS
    gcs_uri = upload_to_gcs(local_file, BUCKET_NAME)

    # 3) Load the JSONL into BigQuery
    load_jsonl_to_bq(
        gcs_uri=gcs_uri,
        project=PROJECT,
        dataset=DATASET,
        table=TABLE,
        use_explicit_schema=False
    )

if __name__ == "__main__":
    main()
