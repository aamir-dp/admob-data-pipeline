import os
import json
from datetime import date, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage, bigquery

# ─── CONFIGURATION & VALIDATION ───────────────────────────────────────────────
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

# ─── STEP 1: AUTHENTICATE TO ADMOB ────────────────────────────────────────────
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

# ─── STEP 2: FETCH & WRITE NDJSON ─────────────────────────────────────────────
def fetch_and_save_ndjson(service, publisher_id, report_date):
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

    # The API returns a *list* of chunk dicts 
    response = service.accounts().mediationReport().generate(
        parent=f"accounts/{publisher_id}",
        body={"reportSpec": spec}
    ).execute()

    # Write each dict as its own line (NDJSON) :contentReference[oaicite:3]{index=3}
    filename = f"mediation_{report_date:%Y%m%d}.jsonl"
    with open(filename, "w") as f:
        for chunk in response:
            f.write(json.dumps(chunk))
            f.write("\n")
    print(f"Wrote NDJSON to {filename}")
    return filename

# ─── STEP 3: UPLOAD NDJSON TO GCS ──────────────────────────────────────────────
def upload_to_gcs(local_path, bucket_name):
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path)                                  # 
    uri = f"gs://{bucket_name}/{os.path.basename(local_path)}"
    print(f"Uploaded {local_path} → {uri}")
    return uri

# ─── STEP 4: LOAD NDJSON INTO BIGQUERY ────────────────────────────────────────
def load_ndjson_to_bq(gcs_uri, project, dataset, table):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,        # :contentReference[oaicite:5]{index=5}
        write_disposition="WRITE_APPEND",
        # existing table schema will be used—no autodetect
    )
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {project}.{dataset}.{table}")

# ─── MAIN FLOW ────────────────────────────────────────────────────────────────
def main():
    creds       = get_admob_creds()
    service     = build_admob_service(creds)
    report_date = date.today() - timedelta(days=1)

    ndjson_file = fetch_and_save_ndjson(service, PUBLISHER_ID, report_date)
    gcs_uri     = upload_to_gcs(ndjson_file, BUCKET_NAME)
    load_ndjson_to_bq(gcs_uri, PROJECT, DATASET, TABLE)

if __name__ == "__main__":
    main()
