import os
import csv
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
    "GCS_BUCKET":          os.getenv("GCS_BUCKET_NAME"),
    "BQ_DATASET":          os.getenv("BQ_DATASET"),
    "BQ_TABLE":            os.getenv("BQ_TABLE"),
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

CLIENT_ID     = required["ADMOB_CLIENT_ID"]
CLIENT_SECRET = required["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN = required["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID  = required["ADMOB_PUBLISHER_ID"]
API_SCOPE     = "https://www.googleapis.com/auth/admob.report"

PROJECT       = required["GCP_PROJECT"]
BUCKET_NAME   = required["GCS_BUCKET"]
DATASET_NAME  = required["BQ_DATASET"]
TABLE_NAME    = required["BQ_TABLE"]

# ─── AUTHENTICATION ────────────────────────────────────────────────────────────
def get_admob_creds():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[API_SCOPE],
    )
    creds.refresh(Request())  # refresh OAuth2 token :contentReference[oaicite:5]{index=5}
    return creds

def build_service(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

# ─── FETCH & WRITE CSV ─────────────────────────────────────────────────────────
def fetch_and_write_csv(service, account_name, report_date, local_path):
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

    resp = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}",
        body={"reportSpec": spec}
    ).execute()

    # Open CSV with newline='' so csv.writer handles line endings itself :contentReference[oaicite:6]{index=6}
    with open(local_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        # Header row
        header = [
            "date", "app", "ad_unit", "ad_source", "ad_source_instance",
            "mediation_group", "country",
            "ad_requests", "clicks", "estimated_earnings_micros", "impressions",
            "impression_ctr", "matched_requests", "match_rate", "observed_ecpm_micros"
        ]
        writer.writerow(header)

        # The response is a list of chunks: first is header, last footer; only chunks with "row" are data :contentReference[oaicite:7]{index=7}
        for chunk in resp:
            if "row" not in chunk:
                continue
            r = chunk["row"]
            dv = r["dimensionValues"]
            mv = r["metricValues"]

            # Convert AdMob "YYYYMMDD" string to "YYYY-MM-DD" for BigQuery DATE :contentReference[oaicite:8]{index=8}
            raw_date = dv["DATE"]["value"]  # e.g. "20250515"
            iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

            writer.writerow([
                iso_date,
                dv["APP"]["value"],
                dv["AD_UNIT"]["value"],
                dv["AD_SOURCE"]["value"],
                dv["AD_SOURCE_INSTANCE"]["value"],
                dv["MEDIATION_GROUP"]["value"],
                dv["COUNTRY"]["value"],
                int(mv["AD_REQUESTS"]["integerValue"]),
                int(mv["CLICKS"]["integerValue"]),
                int(float(mv["ESTIMATED_EARNINGS"].get("micros", mv["ESTIMATED_EARNINGS"].get("decimalValue")))),
                int(mv["IMPRESSIONS"]["integerValue"]),
                float(mv["IMPRESSION_CTR"]["doubleValue"]),
                int(mv["MATCHED_REQUESTS"]["integerValue"]),
                float(mv["MATCH_RATE"]["doubleValue"]),
                int(float(mv["OBSERVED_ECPM"].get("micros", mv["OBSERVED_ECPM"].get("decimalValue")))),
            ])

    print(f"Wrote CSV to {local_path}")
    return local_path

# ─── UPLOAD TO GCS ─────────────────────────────────────────────────────────────
def upload_to_gcs(local_path, bucket_name):
    client = storage.Client()                   # storage.Client() uses Application Default Credentials
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path)       # Upload from local file :contentReference[oaicite:9]{index=9}
    gcs_uri = f"gs://{bucket_name}/{os.path.basename(local_path)}"
    print(f"Uploaded {local_path} → {gcs_uri}")
    return gcs_uri

# ─── LOAD CSV TO BIGQUERY ───────────────────────────────────────────────────────
def load_csv_to_bq(gcs_uri, project, dataset_id, table_id):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # ignore header row :contentReference[oaicite:10]{index=10}
        autodetect=False      # assume table already exists with correct schema
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()  # Waits for job to complete
    print(f"Loaded {gcs_uri} into {project}:{dataset_id}.{table_id}")

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    creds       = get_admob_creds()
    service     = build_service(creds)
    report_date = date.today() - timedelta(days=1)

    local_csv = f"mediation_{report_date:%Y%m%d}.csv"
    fetch_and_write_csv(service, PUBLISHER_ID, report_date, local_csv)

    gcs_uri = upload_to_gcs(local_csv, BUCKET_NAME)
    load_csv_to_bq(gcs_uri, PROJECT, DATASET_NAME, TABLE_NAME)

if __name__ == "__main__":
    main()
