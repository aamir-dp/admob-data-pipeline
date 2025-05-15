import os
import json
from datetime import date, timedelta

import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.cloud import storage, bigquery

# ─── Configuration via ENV ─────────────────────────────────────────────────────
CLIENT_ID        = os.getenv("ADMOB_CLIENT_ID")
CLIENT_SECRET    = os.getenv("ADMOB_CLIENT_SECRET")
REFRESH_TOKEN    = os.getenv("ADMOB_REFRESH_TOKEN")

GCS_BUCKET       = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT       = os.getenv("GCP_PROJECT")
BQ_DATASET       = os.getenv("BQ_DATASET")
BQ_TABLE         = os.getenv("BQ_TABLE")

# ─── AdMob OAuth2 Scope ─────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/admob.report"]

def get_admob_creds():
    """
    Uses your stored refresh token to fetch a new access token.
    """
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=SCOPES,
    )
    # this does the token exchange under the hood
    creds.refresh(requests_kwargs={})
    return creds

def fetch_admob_report(creds, account_name, report_date):
    """
    Calls accounts.networkReport.generate for the given date.
    Returns an iterable of JSON chunks.
    """
    service = build("admob", "v1", credentials=creds, cache_discovery=False)
    report_spec = {
        "dateRange": {
            "startDate": {
                "year": report_date.year,
                "month": report_date.month,
                "day": report_date.day,
            },
            "endDate": {
                "year": report_date.year,
                "month": report_date.month,
                "day": report_date.day,
            },
        },
        "dimensions": [
            "DATE", "MONTH", "WEEK",
            "AD_SOURCE", "AD_SOURCE_INSTANCE",
            "AD_UNIT", "APP", "MEDIATION_GROUP",
            "COUNTRY", "APP_VERSION_NAME"
        ],
        "metrics": [
            "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
            "IMPRESSION_CTR", "IMPRESSION_RPM", "MATCH_RATE", "SHOW_RATE"
        ],
        "sortConditions": [
            {"dimension": "DATE", "order": "ASCENDING"}
        ]
    }
    request = service.accounts().networkReport().generate(
        parent=account_name,
        body={"reportSpec": report_spec}
    )
    return request.execute()

def parse_report_rows(stream):
    """
    Given the JSON stream from the API, extract each row into a dict.
    """
    rows = []
    for chunk in stream:
        if "row" not in chunk:
            continue
        dv = chunk["row"]["dimensionValues"]
        mv = chunk["row"]["metricValues"]
        record = {
            "date": dv[0]["value"],
            "month": dv[1]["value"],
            "week": dv[2]["value"],
            "ad_source": dv[3]["value"],
            "ad_source_instance": dv[4]["value"],
            "ad_unit": dv[5]["value"],
            "app": dv[6]["value"],
            "mediation_group": dv[7]["value"],
            "country": dv[8]["value"],
            "app_version_name": dv[9]["value"],
            "ad_requests":      int(mv[0].get("integerValue", 0)),
            "clicks":           int(mv[1].get("integerValue", 0)),
            "estimated_earnings_micros": int(mv[2].get("microsValue", 0)),
            "impressions":      int(mv[3].get("integerValue", 0)),
            "impression_ctr":   float(mv[4].get("doubleValue", 0)),
            "impression_rpm_micros":     int(mv[5].get("microsValue", 0)),
            "match_rate":       float(mv[6].get("doubleValue", 0)),
            "show_rate":        float(mv[7].get("doubleValue", 0)),
        }
        rows.append(record)
    return rows

def upload_to_gcs(rows, filename):
    """
    Write newline-delimited JSON to your GCS bucket.
    """
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    payload = "\n".join(json.dumps(r) for r in rows)
    blob.upload_from_string(payload, content_type="application/json")
    print(f"✅ Wrote {len(rows)} rows to gs://{GCS_BUCKET}/{filename}")

def load_into_bigquery(filename):
    """
    Load the JSONL file from GCS into BigQuery with autodetect schema.
    """
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )
    uri = f"gs://{GCS_BUCKET}/{filename}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # wait
    print(f"✅ Loaded {load_job.output_rows} rows into {BQ_DATASET}.{BQ_TABLE}")

def main():
    # 1. Refresh token & build creds
    creds = get_admob_creds()

    # 2. Get your AdMob publisher account name
    service = build("admob", "v1", credentials=creds, cache_discovery=False)
    accounts = service.accounts().list().execute().get("account", [])
    if not accounts:
        raise RuntimeError("No AdMob accounts found.")
    account_name = accounts[0]["name"]
    print("Using AdMob account:", account_name)

    # 3. Define the date to export (yesterday)
    export_date = date.today() - timedelta(days=1)
    print("Exporting data for:", export_date)

    # 4. Fetch & parse
    stream = fetch_admob_report(creds, account_name, export_date)
    rows = parse_report_rows(stream)

    if not rows:
        print("⚠️ No rows returned for", export_date)
        return

    # 5. Upload to GCS & load into BQ
    filename = f"admob_{export_date:%Y%m%d}.jsonl"
    upload_to_gcs(rows, filename)
    load_into_bigquery(filename)

if __name__ == "__main__":
    main()
