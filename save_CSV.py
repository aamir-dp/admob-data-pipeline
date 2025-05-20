import os
import csv
from datetime import date, timedelta, datetime

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from google.cloud import storage, bigquery

raw = os.getenv("REPORT_DATE") or os.getenv("INPUT_RUN_DATE")
if raw:
    # if someone set REPORT_DATE (or passed INPUT_RUN_DATE in a manual run)
    report_date = datetime.strptime(raw, "%Y-%m-%d").date()
else:
    # scheduled run → fallback to yesterday
    report_date = date.today() - timedelta(days=1)


# ─── CONFIGURATION & VALIDATION ───────────────────────────────────────────────
# Make sure these environment variables are set in your GitHub repo or shell:
# ADMOB_CLIENT_ID, ADMOB_CLIENT_SECRET, ADMOB_REFRESH_TOKEN,
# ADMOB_PUBLISHER_ID, GCP_PROJECT, GCS_BUCKET_NAME, BQ_DATASET, BQ_TABLE
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
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

CLIENT_ID       = required["ADMOB_CLIENT_ID"]
CLIENT_SECRET   = required["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN   = required["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID    = required["ADMOB_PUBLISHER_ID"]
API_SCOPE       = "https://www.googleapis.com/auth/admob.report"

PROJECT         = required["GCP_PROJECT"]
BUCKET_NAME     = required["GCS_BUCKET_NAME"]
DATASET_NAME    = required["BQ_DATASET"]
TABLE_NAME      = required["BQ_TABLE"]

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
    creds.refresh(Request())
    return creds

def build_service(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

# ─── HELPERS TO SAFELY EXTRACT NUMERIC METRICS ────────────────────────────────
def get_int(mv: dict, key: str) -> int:
    """Extract an integer metric, defaulting to 0 if missing."""
    d = mv.get(key, {}) or {}
    if "integerValue" in d and d["integerValue"] is not None:
        return int(d["integerValue"])
    if "microsValue" in d and d["microsValue"] is not None:
        return int(d["microsValue"])
    # sometimes comes back as decimalValue string
    dec = d.get("decimalValue")
    if dec is not None:
        try:
            return int(float(dec))
        except ValueError:
            pass
    return 0

def get_float(mv: dict, key: str) -> float:
    """Extract a float metric, defaulting to 0.0 if missing."""
    d = mv.get(key, {}) or {}
    if "doubleValue" in d and d["doubleValue"] is not None:
        return float(d["doubleValue"])
    # fallback to any string "value"
    val = d.get("value") or d.get("decimalValue")
    if val is not None:
        try:
            return float(val)
        except ValueError:
            pass
    return 0.0

# ─── FETCH & WRITE CSV ─────────────────────────────────────────────────────────
def fetch_and_write_csv(service, publisher_id: str, report_date: date, local_path: str) -> str:
    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year,  "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year,  "month": report_date.month, "day": report_date.day},
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
    ).execute()  # returns a list of chunks

    # Write CSV, using newline='' to avoid extra blank lines
    with open(local_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)

        # header row
        writer.writerow([
            "date",
            "app_name", "ad_unit_name",
            "ad_source_name", "ad_source_instance_name", "mediation_group_name",
            "country",
            "ad_requests", "clicks", "estimated_earnings_micros", "impressions",
            "impression_ctr", "matched_requests", "match_rate", "observed_ecpm_micros"
        ])

        for chunk in response:
            if "row" not in chunk:
                continue
            dims = chunk["row"]["dimensionValues"]
            mets = chunk["row"]["metricValues"]

            # Convert YYYYMMDD → YYYY-MM-DD
            raw_date = dims["DATE"]["value"]
            iso_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

            # helper to pick displayLabel if present, else raw value
            def disp(key):
                dv = dims.get(key, {})
                return dv.get("displayLabel") or dv.get("value") or ""

            writer.writerow([
                iso_date,
                disp("APP"),
                disp("AD_UNIT"),
                disp("AD_SOURCE"),
                disp("AD_SOURCE_INSTANCE"),
                disp("MEDIATION_GROUP"),
                dims["COUNTRY"]["value"] or "",

                get_int(mets, "AD_REQUESTS"),
                get_int(mets, "CLICKS"),
                get_int(mets, "ESTIMATED_EARNINGS"),
                get_int(mets, "IMPRESSIONS"),
                get_float(mets, "IMPRESSION_CTR"),
                get_int(mets, "MATCHED_REQUESTS"),
                get_float(mets, "MATCH_RATE"),
                get_int(mets, "OBSERVED_ECPM"),
            ])

    print(f"Wrote CSV to {local_path}")
    return local_path

# ─── UPLOAD CSV TO GCS ─────────────────────────────────────────────────────────
def upload_to_gcs(local_path: str, bucket_name: str) -> str:
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(local_path))
    blob.upload_from_filename(local_path, content_type="text/csv")
    uri = f"gs://{bucket_name}/{os.path.basename(local_path)}"
    print(f"Uploaded {local_path} → {uri}")
    return uri

# ─── LOAD CSV INTO BIGQUERY ────────────────────────────────────────────────────
def load_csv_to_bq(gcs_uri: str, project: str, dataset: str, table: str):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # wait for completion
    print(f"Appended data into {project}.{dataset}.{table}")

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    creds   = get_admob_creds()
    service = build_service(creds)

    local_csv = f"mediation_{report_date:%Y%m%d}.csv"
    fetch_and_write_csv(service, PUBLISHER_ID, report_date, local_csv)
    gcs_uri   = upload_to_gcs(local_csv, BUCKET_NAME)
    load_csv_to_bq(gcs_uri, PROJECT, DATASET_NAME, TABLE_NAME)

if __name__ == "__main__":
    main()
