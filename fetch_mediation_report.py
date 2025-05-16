import os
import json
from datetime import date, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage, bigquery

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
CLIENT_ID       = os.getenv("ADMOB_CLIENT_ID")
CLIENT_SECRET   = os.getenv("ADMOB_CLIENT_SECRET")
REFRESH_TOKEN   = os.getenv("ADMOB_REFRESH_TOKEN")
PUBLISHER_ID    = os.getenv("ADMOB_PUBLISHER_ID")  # e.g. 'pub-1234567890123456'
GCS_BUCKET      = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT      = os.getenv("GCP_PROJECT")
BQ_DATASET      = os.getenv("BQ_DATASET")
BQ_TABLE        = os.getenv("BQ_TABLE")
API_SCOPE       = "https://www.googleapis.com/auth/admob.report"

def get_admob_creds():
    """Refresh OAuth2 credentials for AdMob API calls."""
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[API_SCOPE],
    )
    creds.refresh(Request())  # uses google-auth Request transport :contentReference[oaicite:5]{index=5}
    return creds

def build_service(creds):
    """Build the AdMob API client."""
    return build("admob", "v1", credentials=creds, cache_discovery=False)

def fetch_mediation(service, account_name, report_date):
    """
    Calls accounts.mediationReport.generate() for the given date and returns rows.
    """
    # Only DATE allowed as the single time dimension :contentReference[oaicite:6]{index=6}
    dimensions = [
        "DATE",
        "APP", "AD_UNIT",
        "AD_SOURCE", "AD_SOURCE_INSTANCE", "MEDIATION_GROUP",
        "COUNTRY",
    ]
    # Metrics list as requested 
    metrics = [
        "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
        "IMPRESSION_CTR", "MATCHED_REQUESTS", "MATCH_RATE", "OBSERVED_ECPM"
    ]

    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year, "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year, "month": report_date.month, "day": report_date.day},
        },
        "dimensions":    dimensions,
        "metrics":       metrics,
        "sortConditions":[{"dimension":"DATE","order":"ASCENDING"}]
    }

    response = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}",
        body={"reportSpec": spec}
    ).execute()  # returns a list of chunks :contentReference[oaicite:8]{index=8}

    rows = []
    for chunk in response:
        row = chunk.get("row")
        if not row:
            continue
        dv = row["dimensionValues"]  # dict of dim→value
        mv = row["metricValues"]     # dict of met→value

        record = {}
        # Extract dimensions by name, safe lookup :contentReference[oaicite:9]{index=9}
        for d in dimensions:
            record[d.lower()] = dv.get(d, {}).get("value")
        # Extract metrics, defaulting missing metrics to 0 :contentReference[oaicite:10]{index=10}
        for m in metrics:
            val = mv.get(m)
            if not val:
                record[m.lower()] = 0
            elif "integerValue" in val:
                record[m.lower()] = int(val["integerValue"])
            elif "doubleValue" in val:
                record[m.lower()] = float(val["doubleValue"])
            elif "microsValue" in val:
                record[f"{m.lower()}_micros"] = int(val["microsValue"])
        rows.append(record)

    return rows

def upload_to_gcs(data_str, filename):
    """Uploads the given NDJSON string to GCS."""
    client = storage.Client(project=BQ_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(data_str, content_type="application/json")  # :contentReference[oaicite:11]{index=11}
    print(f"Uploaded gs://{GCS_BUCKET}/{filename}")

def load_to_bq(gcs_uri):
    """Loads the JSONL file from GCS into BigQuery (appending)."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()  # wait :contentReference[oaicite:12]{index=12}
    print(f"Loaded {job.output_rows} rows into {BQ_DATASET}.{BQ_TABLE}")

def main():
    creds    = get_admob_creds()
    service  = build_service(creds)
    report_date = date.today() - timedelta(days=1)

    rows = fetch_mediation(service, PUBLISHER_ID, report_date)
    if not rows:
        print("No data returned for", report_date)
        return

    # Create NDJSON payload
    ndjson = "\n".join(json.dumps(r) for r in rows)
    filename = f"mediation_{report_date:%Y%m%d}.jsonl"

    # Upload & load
    upload_to_gcs(ndjson, filename)
    gcs_uri = f"gs://{GCS_BUCKET}/{filename}"
    load_to_bq(gcs_uri)

if __name__ == "__main__":
    main()
