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

def get_int(mv, key):
    """Safely extract an integer from metricValues[key]. Defaults to 0."""
    d = mv.get(key, {}) or {}
    if "integerValue" in d and d["integerValue"] is not None:
        return int(d["integerValue"])
    if "microsValue" in d and d["microsValue"] is not None:
        return int(d["microsValue"])
    # some metrics return as decimalValue strings
    val = d.get("decimalValue")
    if val is not None:
        try:
            return int(float(val))
        except ValueError:
            return 0
    return 0

def get_float(mv, key):
    """Safely extract a float from metricValues[key]. Defaults to 0.0."""
    d = mv.get(key, {}) or {}
    if "doubleValue" in d and d["doubleValue"] is not None:
        return float(d["doubleValue"])
    # fallback if the API ever returns string in 'value'
    val = d.get("value")
    if val is not None:
        try:
            return float(val)
        except ValueError:
            return 0.0
    return 0.0

def fetch_and_write_csv(service, account_name, report_date, local_path):
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

    resp = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}",
        body={"reportSpec": spec}
    ).execute()

    with open(local_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "date", "app", "ad_unit", "ad_source", "ad_source_instance",
            "mediation_group", "country",
            "ad_requests", "clicks", "estimated_earnings_micros", "impressions",
            "impression_ctr", "matched_requests", "match_rate", "observed_ecpm_micros"
        ])

        for chunk in resp:
            if "row" not in chunk:
                continue
            dv = chunk["row"]["dimensionValues"]
            mv = chunk["row"]["metricValues"]

            raw_date = dv["DATE"]["value"]
            iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

            writer.writerow([
                iso_date,
                dv["APP"]["value"],
                dv["AD_UNIT"]["value"],
                dv["AD_SOURCE"]["value"],
                dv["AD_SOURCE_INSTANCE"]["value"],
                dv["MEDIATION_GROUP"]["value"],
                dv["COUNTRY"]["value"],
                get_int(mv, "AD_REQUESTS"),
                get_int(mv, "CLICKS"),
                get_int(mv, "ESTIMATED_EARNINGS"),
                get_int(mv, "IMPRESSIONS"),
                get_float(mv, "IMPRESSION_CTR"),
                get_int(mv, "MATCHED_REQUESTS"),
                get_float(mv, "MATCH_RATE"),
                get_int(mv, "OBSERVED_ECPM"),
            ])

    print(f"Wrote CSV to {local_path}")
    return local_path

# ─── UPLOAD + BQ LOAD (unchanged) ───────────────────────────────────────────────
def upload_to_gcs(local_path, bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path)
    uri = f"gs://{bucket_name}/{os.path.basename(local_path)}"
    print(f"Uploaded {local_path} → {uri}")
    return uri

def load_csv_to_bq(gcs_uri, project, dataset, table):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,       # or True, or define explicit schema
        write_disposition="WRITE_TRUNCATE"
    )
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded into {project}.{dataset}.{table}")

def main():
    creds       = get_admob_creds()
    service     = build_service(creds)
    report_date = date.today() - timedelta(days=1)
    local_csv   = f"mediation_{report_date:%Y%m%d}.csv"

    csv_path    = fetch_and_write_csv(service, PUBLISHER_ID, report_date, local_csv)
    gcs_uri     = upload_to_gcs(csv_path, BUCKET_NAME)
    load_csv_to_bq(gcs_uri, PROJECT, DATASET_NAME, TABLE_NAME)

if __name__ == "__main__":
    main()
