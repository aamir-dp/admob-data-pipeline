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
    "GCS_BUCKET_NAME":     os.getenv("GCS_BUCKET_NAME"),
    "GCP_PROJECT":         os.getenv("GCP_PROJECT"),
    "BQ_DATASET":          os.getenv("BQ_DATASET"),
    "BQ_TABLE":            os.getenv("BQ_TABLE"),
}
missing = [k for k,v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

CLIENT_ID      = required["ADMOB_CLIENT_ID"]
CLIENT_SECRET  = required["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN  = required["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID   = required["ADMOB_PUBLISHER_ID"]
GCS_BUCKET     = required["GCS_BUCKET_NAME"]
BQ_PROJECT     = required["GCP_PROJECT"]
BQ_DATASET     = required["BQ_DATASET"]
BQ_TABLE       = required["BQ_TABLE"]
API_SCOPE      = "https://www.googleapis.com/auth/admob.report"

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

# ─── FETCH MEDIATION REPORT ───────────────────────────────────────────────────
def fetch_mediation(service, account_name, report_date):
    dims = [
        "DATE",
        "APP", "AD_UNIT",
        "AD_SOURCE", "AD_SOURCE_INSTANCE", "MEDIATION_GROUP",
        "COUNTRY",
    ]
    mets = [
        "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
        "IMPRESSION_CTR", "MATCHED_REQUESTS", "MATCH_RATE", "OBSERVED_ECPM"
    ]

    spec = {
        "dateRange": {
            "startDate": {"year": report_date.year, "month": report_date.month, "day": report_date.day},
            "endDate":   {"year": report_date.year, "month": report_date.month, "day": report_date.day},
        },
        "dimensions":    dims,
        "metrics":       mets,
        "sortConditions":[{"dimension":"DATE","order":"ASCENDING"}]
    }

    resp = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}",
        body={"reportSpec": spec}
    ).execute()

    rows = []
    for chunk in resp:
        row = chunk.get("row")
        if not row:
            continue
        dv = row["dimensionValues"]
        mv = row["metricValues"]

        rec = {}
        for d in dims:
            rec[d.lower()] = dv.get(d, {}).get("value")
        for m in mets:
            val = mv.get(m)
            if not val:
                rec[m.lower()] = 0
            elif "integerValue" in val:
                rec[m.lower()] = int(val["integerValue"])
            elif "doubleValue" in val:
                rec[m.lower()] = float(val["doubleValue"])
            elif "microsValue" in val:
                rec[f"{m.lower()}_micros"] = int(val["microsValue"])
        rows.append(rec)

    return rows

# ─── GCS UPLOAD & BQ LOAD ───────────────────────────────────────────────────────
def upload_to_gcs(data_str, filename):
    client = storage.Client(project=BQ_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(data_str, content_type="application/json")
    print(f"Uploaded gs://{GCS_BUCKET}/{filename}")

def load_to_bq(gcs_uri):
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
        # AUTODETECT OFF: use existing table schema
    )
    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {BQ_DATASET}.{BQ_TABLE}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    creds       = get_admob_creds()
    service     = build_service(creds)
    report_date = date.today() - timedelta(days=1)

    rows = fetch_mediation(service, PUBLISHER_ID, report_date)
    if not rows:
        print(f"No data returned for {report_date}")
        return

    ndjson   = "\n".join(json.dumps(r) for r in rows)
    filename = f"mediation_{report_date:%Y%m%d}.jsonl"
    gcs_uri  = f"gs://{GCS_BUCKET}/{filename}"

    upload_to_gcs(ndjson, filename)
    load_to_bq(gcs_uri)

if __name__ == "__main__":
    main()
