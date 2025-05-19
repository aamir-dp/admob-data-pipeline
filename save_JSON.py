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

# ─── STEP 1: AUTHENTICATE TO ADMOB ─────────────────────────────────────────────
def get_admob_creds():
    """Refresh OAuth2 credentials using your refresh token."""
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
    """Build the AdMob API client."""
    return build("admob", "v1", credentials=creds, cache_discovery=False)

# ─── STEP 2: FETCH & FLATTEN REPORT ────────────────────────────────────────────
def fetch_rows(service, publisher_id, report_date):
    """Fetches mediation report and returns flattened rows as dicts."""
    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year, "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year, "month": report_date.month, "day": report_date.day},
        },
        "dimensions": [
            "DATE", "APP", "AD_UNIT",
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
    ).execute()  # returns a list of chunk dicts 

    rows = []
    for chunk in response:
        row = chunk.get("row", {})
        dv = row.get("dimensionValues", {})
        mv = row.get("metricValues", {})
        rec = {
            "date":                   dv.get("DATE", {}).get("value"),
            "app":                    dv.get("APP", {}).get("value"),
            "ad_unit":                dv.get("AD_UNIT", {}).get("value"),
            "ad_source":              dv.get("AD_SOURCE", {}).get("value"),
            "ad_source_instance":     dv.get("AD_SOURCE_INSTANCE", {}).get("value"),
            "mediation_group":        dv.get("MEDIATION_GROUP", {}).get("value"),
            "country":                dv.get("COUNTRY", {}).get("value"),
            "ad_requests":            int(mv.get("AD_REQUESTS", {}).get("integerValue", 0)),
            "clicks":                 int(mv.get("CLICKS", {}).get("integerValue", 0)),
            "estimated_earnings_micros": int(mv.get("ESTIMATED_EARNINGS", {}).get("microsValue", 0)),
            "impressions":            int(mv.get("IMPRESSIONS", {}).get("integerValue", 0)),
            "impression_ctr":         float(mv.get("IMPRESSION_CTR", {}).get("doubleValue", 0.0)),
            "matched_requests":       int(mv.get("MATCHED_REQUESTS", {}).get("integerValue", 0)),
            "match_rate":             float(mv.get("MATCH_RATE", {}).get("doubleValue", 0.0)),
            "observed_ecpm_micros":   int(mv.get("OBSERVED_ECPM", {}).get("microsValue", 0)),
        }
        rows.append(rec)
    return rows

# ─── STEP 3: WRITE CSV ──────────────────────────────────────────────────────────
def write_csv(filename, rows):
    """Writes a list of dicts to a CSV file."""
    fieldnames = [
        "date", "app", "ad_unit", "ad_source", "ad_source_instance",
        "mediation_group", "country", "ad_requests", "clicks",
        "estimated_earnings_micros", "impressions", "impression_ctr",
        "matched_requests", "match_rate", "observed_ecpm_micros"
    ]
    with open(filename, mode="w", newline="") as csvfile:  # avoid blank lines 
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)  # write header 
        writer.writeheader()
        writer.writerows(rows)

# ─── STEP 4: UPLOAD CSV TO GCS ─────────────────────────────────────────────────
def upload_to_gcs(local_path, bucket_name):
    """Uploads a local file to Cloud Storage."""
    client = storage.Client(project=PROJECT)  # uses ADC or service account :contentReference[oaicite:5]{index=5}
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path)      # upload via filename :contentReference[oaicite:6]{index=6}
    uri = f"gs://{bucket_name}/{os.path.basename(local_path)}"
    print(f"Uploaded {local_path} → {uri}")
    return uri

# ─── STEP 5: LOAD CSV INTO BIGQUERY ───────────────────────────────────────────
def load_csv_to_bq(gcs_uri, project, dataset, table):
    """Loads a CSV file from GCS into BigQuery."""
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,            # set CSV format :contentReference[oaicite:7]{index=7}
        skip_leading_rows=1,                                # skip header row :contentReference[oaicite:8]{index=8}
        write_disposition="WRITE_APPEND",
    )
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)  # initiate job :contentReference[oaicite:9]{index=9}
    load_job.result()  # wait for completion :contentReference[oaicite:10]{index=10}
    print(f"Loaded {load_job.output_rows} rows into {project}.{dataset}.{table}")

# ─── MAIN FLOW ────────────────────────────────────────────────────────────────
def main():
    creds       = get_admob_creds()
    service     = build_admob_service(creds)
    report_date = date.today() - timedelta(days=1)

    rows     = fetch_rows(service, PUBLISHER_ID, report_date)
    if not rows:
        print(f"No data for {report_date}")
        return

    csv_file = f"mediation_{report_date:%Y%m%d}.csv"
    write_csv(csv_file, rows)  
    gcs_uri  = upload_to_gcs(csv_file, BUCKET_NAME)
    load_csv_to_bq(gcs_uri, PROJECT, DATASET, TABLE)

if __name__ == "__main__":
    main()
