import os, csv, json
from datetime import date, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage, bigquery

# ─ CONFIG ─────────────────────────────────────────────────────────────────────
required = {
    "ADMOB_CLIENT_ID":     os.getenv("ADMOB_CLIENT_ID"),
    "ADMOB_CLIENT_SECRET": os.getenv("ADMOB_CLIENT_SECRET"),
    "ADMOB_REFRESH_TOKEN": os.getenv("ADMOB_REFRESH_TOKEN"),
    "ADMOB_PUBLISHER_ID":  os.getenv("ADMOB_PUBLISHER_ID"),
    "GCS_BUCKET_NAME":     os.getenv("GCS_BUCKET_NAME"),
    "BQ_DATASET":          os.getenv("BQ_DATASET"),
    "BQ_TABLE":            os.getenv("BQ_TABLE"),
    "GCP_PROJECT":         os.getenv("GCP_PROJECT"),
}
missing = [k for k,v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {missing}")

CLIENT_ID     = required["ADMOB_CLIENT_ID"]
CLIENT_SECRET = required["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN = required["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID  = required["ADMOB_PUBLISHER_ID"]
GCS_BUCKET    = required["GCS_BUCKET_NAME"]
BQ_DATASET    = required["BQ_DATASET"]
BQ_TABLE      = required["BQ_TABLE"]
GCP_PROJECT   = required["GCP_PROJECT"]
API_SCOPE     = "https://www.googleapis.com/auth/admob.report"

# ─ AUTH ────────────────────────────────────────────────────────────────────────
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

# ─ FETCH ───────────────────────────────────────────────────────────────────────
def fetch_mediation(service, publisher_id, report_date):
    # (use code from section 2 above)
    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year, "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year, "month": report_date.month, "day": report_date.day},
        },
        "dimensions": ["DATE","APP","AD_UNIT","AD_SOURCE","AD_SOURCE_INSTANCE","MEDIATION_GROUP","COUNTRY"],
        "metrics":    ["AD_REQUESTS","CLICKS","ESTIMATED_EARNINGS","IMPRESSIONS",
                       "IMPRESSION_CTR","MATCHED_REQUESTS","MATCH_RATE","OBSERVED_ECPM"],
        "sortConditions": [{"dimension":"DATE","order":"ASCENDING"}]
    }
    response = service.accounts().mediationReport().generate(
        parent=f"accounts/{publisher_id}", body={"reportSpec": spec}
    ).execute()
    rows = []
    for chunk in response:
        row = chunk["row"]
        dv, mv = row["dimensionValues"], row["metricValues"]
        raw_date = dv["DATE"]["value"]
        iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        rows.append({
            "date":               iso_date,
            "app":                dv["APP"]["value"],
            "ad_unit":            dv["AD_UNIT"]["value"],
            "ad_source":          dv["AD_SOURCE"]["value"],
            "ad_source_instance": dv["AD_SOURCE_INSTANCE"]["value"],
            "mediation_group":    dv["MEDIATION_GROUP"]["value"],
            "country":            dv["COUNTRY"]["value"],
            "ad_requests":        int(mv["AD_REQUESTS"]["value"]),
            "clicks":             int(mv["CLICKS"]["value"]),
            "estimated_earnings": int(mv["ESTIMATED_EARNINGS"]["value"]),
            "impressions":        int(mv["IMPRESSIONS"]["value"]),
            "impression_ctr":     float(mv["IMPRESSION_CTR"]["value"]),
            "matched_requests":   int(mv["MATCHED_REQUESTS"]["value"]),
            "match_rate":         float(mv["MATCH_RATE"]["value"]),
            "observed_ecpm":      float(mv["OBSERVED_ECPM"]["value"])
        })
    return rows

# ─ CSV & UPLOAD ────────────────────────────────────────────────────────────────
def write_csv(rows, path):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

def upload_to_gcs(local_path, bucket_name, dest_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    bucket.blob(dest_blob).upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{bucket_name}/{dest_blob}")

# ─ BQ LOAD ─────────────────────────────────────────────────────────────────────
def load_csv_to_bq(gcs_uri, project, dataset, table):
    bq = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = bq.load_table_from_uri(
        gcs_uri, f"{project}.{dataset}.{table}", job_config=job_config
    )
    job.result()
    print(f"Loaded data into {project}.{dataset}.{table}")

# ─ MAIN ────────────────────────────────────────────────────────────────────────
def main():
    creds       = get_admob_creds()
    service     = build_service(creds)
    report_date = date.today() - timedelta(days=1)

    rows     = fetch_mediation(service, PUBLISHER_ID, report_date)
    local_csv = f"mediation_{report_date:%Y%m%d}.csv"
    write_csv(rows, local_csv)

    blob_name = local_csv
    upload_to_gcs(local_csv, GCS_BUCKET, blob_name)

    gcs_uri = f"gs://{GCS_BUCKET}/{blob_name}"
    load_csv_to_bq(gcs_uri, GCP_PROJECT, BQ_DATASET, BQ_TABLE)

if __name__ == "__main__":
    main()
