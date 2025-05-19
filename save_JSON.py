import os, csv, json
from datetime import date, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage, bigquery

# ─── CONFIG & VALIDATION ───────────────────────────────────────────────────────
required_env = {
    "ADMOB_CLIENT_ID":     os.getenv("ADMOB_CLIENT_ID"),
    "ADMOB_CLIENT_SECRET": os.getenv("ADMOB_CLIENT_SECRET"),
    "ADMOB_REFRESH_TOKEN": os.getenv("ADMOB_REFRESH_TOKEN"),
    "ADMOB_PUBLISHER_ID":  os.getenv("ADMOB_PUBLISHER_ID"),
    "GCS_BUCKET_NAME":     os.getenv("GCS_BUCKET_NAME"),
    "BQ_PROJECT":          os.getenv("GCP_PROJECT"),
    "BQ_DATASET":          os.getenv("BQ_DATASET"),
    "BQ_TABLE":            os.getenv("BQ_TABLE"),
}
missing = [k for k,v in required_env.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

CLIENT_ID     = required_env["ADMOB_CLIENT_ID"]
CLIENT_SECRET = required_env["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN = required_env["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID  = required_env["ADMOB_PUBLISHER_ID"]
GCS_BUCKET    = required_env["GCS_BUCKET_NAME"]
BQ_PROJECT    = required_env["BQ_PROJECT"]
BQ_DATASET    = required_env["BQ_DATASET"]
BQ_TABLE      = required_env["BQ_TABLE"]
API_SCOPE     = "https://www.googleapis.com/auth/admob.report"

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
    creds.refresh(Request())  # uses google-auth Request transport :contentReference[oaicite:3]{index=3}
    return creds

def build_service(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

# ─── FETCH & FLATTEN ────────────────────────────────────────────────────────────
def fetch_mediation(service, account_name, report_date):
    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year, "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year, "month": report_date.month, "day": report_date.day},
        },
        "dimensions": ["DATE","APP","AD_UNIT","AD_SOURCE","AD_SOURCE_INSTANCE","MEDIATION_GROUP","COUNTRY"],
        "metrics":    ["AD_REQUESTS","CLICKS","ESTIMATED_EARNINGS","IMPRESSIONS","IMPRESSION_CTR",
                       "MATCHED_REQUESTS","MATCH_RATE","OBSERVED_ECPM"],
        "sortConditions": [{"dimension":"DATE","order":"ASCENDING"}],
    }
    resp = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}", body={"reportSpec": spec}
    ).execute()  # returns dict with 'rows' :contentReference[oaicite:4]{index=4}

    rows = []
    for row in resp.get("rows", []):
        rec = {}
        # Flatten dimensions
        for dv in row.get("dimensionValues", []):
            dim = dv["key"]
            raw = dv["value"]
            if dim == "DATE" and raw and len(raw)==8:
                rec["date"] = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"  # convert to YYYY-MM-DD :contentReference[oaicite:5]{index=5}
            else:
                rec[dim.lower()] = raw
        # Flatten metrics
        for mv in row.get("metricValues", []):
            rec[mv["key"].lower()] = mv["value"]
        rows.append(rec)
    return rows

# ─── CSV + GCS + BQ LOAD ────────────────────────────────────────────────────────
def upload_csv_to_gcs(rows, filename):
    # Write CSV locally
    fieldnames = list(rows[0].keys())
    with open(filename, "w", newline="") as f:  # newline='' prevents blank lines :contentReference[oaicite:6]{index=6}
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename, content_type="text/csv")
    print(f"Uploaded {filename} → gs://{GCS_BUCKET}/{filename}")

def load_csv_to_bq(filename):
    client = bigquery.Client(project=BQ_PROJECT)
    uri = f"gs://{GCS_BUCKET}/{filename}"
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("app", "STRING"),
            bigquery.SchemaField("ad_unit", "STRING"),
            bigquery.SchemaField("ad_source", "STRING"),
            bigquery.SchemaField("ad_source_instance", "STRING"),
            bigquery.SchemaField("mediation_group", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("ad_requests", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("estimated_earnings", "FLOAT"),
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("impression_ctr", "FLOAT"),
            bigquery.SchemaField("matched_requests", "INTEGER"),
            bigquery.SchemaField("match_rate", "FLOAT"),
            bigquery.SchemaField("observed_ecpm", "FLOAT"),
        ],
        max_bad_records=0,  # fail fast on parse issues :contentReference[oaicite:7]{index=7}
    )
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # wait for completion :contentReference[oaicite:8]{index=8}
    print("Loaded CSV into BigQuery")

def main():
    creds       = get_admob_creds()
    service     = build_service(creds)
    report_date = date.today() - timedelta(days=1)
    rows        = fetch_mediation(service, PUBLISHER_ID, report_date)
    filename    = f"mediation_{report_date:%Y%m%d}.csv"
    upload_csv_to_gcs(rows, filename)
    load_csv_to_bq(filename)

if __name__ == "__main__":
    main()
