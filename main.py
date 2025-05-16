import os, json
from datetime import date, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage, bigquery

# ─── Environment Configuration ────────────────────────────────────────────────
CLIENT_ID        = os.getenv("ADMOB_CLIENT_ID")
CLIENT_SECRET    = os.getenv("ADMOB_CLIENT_SECRET")
REFRESH_TOKEN    = os.getenv("ADMOB_REFRESH_TOKEN")

GCS_BUCKET       = os.getenv("GCS_BUCKET_NAME")
BQ_PROJECT       = os.getenv("GCP_PROJECT")
BQ_DATASET       = os.getenv("BQ_DATASET")
BQ_TABLE         = os.getenv("BQ_TABLE")

API_SCOPE        = "https://www.googleapis.com/auth/admob.report"

def get_admob_creds():
    """Refreshes and returns OAuth2 credentials for AdMob API calls."""
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[API_SCOPE],
    )
    creds.refresh(Request())  # Correct refresh call :contentReference[oaicite:5]{index=5}:contentReference[oaicite:6]{index=6}
    return creds

def get_admob_service(creds):
    """Builds and returns the AdMob API service object."""
    return build("admob", "v1", credentials=creds, cache_discovery=False)

def get_account_name(service, publisher_id):
    """Fetches and prints AdMob account info, returns the account resource name."""
    response = service.accounts().get(name=f"accounts/{publisher_id}").execute()
    print("Account:", response["name"], "Publisher ID:", response["publisherId"])  # :contentReference[oaicite:7]{index=7}
    return response["name"]

def fetch_report(service, parent, method, report_spec):
    """
    Executes either networkReport.generate or mediationReport.generate
    and returns the streaming response.
    """
    if method == "network":
        return service.accounts().networkReport().generate(
            parent=parent, body={"reportSpec": report_spec}
        ).execute()  # :contentReference[oaicite:8]{index=8}
    else:
        return service.accounts().mediationReport().generate(
            parent=parent, body={"reportSpec": report_spec}
        ).execute()  # :contentReference[oaicite:9]{index=9}

def parse_rows(stream):
    """Extracts and returns list of dict records from the API stream."""
    rows = []
    for chunk in stream:
        if "row" not in chunk:
            continue
        dv = chunk["row"]["dimensionValues"]
        mv = chunk["row"]["metricValues"]
        record = {
            "date": dv["DATE"]["value"],
            "month": dv["MONTH"]["value"],
            "week": dv["WEEK"]["value"],
            "ad_source": dv["AD_SOURCE"]["value"],
            "ad_source_instance": dv["AD_SOURCE_INSTANCE"]["value"],
            "ad_unit": dv["AD_UNIT"]["value"],
            "app": dv["APP"]["value"],
            "mediation_group": dv["MEDIATION_GROUP"]["value"],
            "country": dv["COUNTRY"]["value"],
            "app_version_name": dv["APP_VERSION_NAME"]["value"],
            "ad_requests": int(mv["AD_REQUESTS"]["integerValue"]),
            "clicks": int(mv["CLICKS"]["integerValue"]),
            "estimated_earnings_micros": int(mv["ESTIMATED_EARNINGS"]["microsValue"]),
            "impressions": int(mv["IMPRESSIONS"]["integerValue"]),
            "impression_ctr": float(mv["IMPRESSION_CTR"]["doubleValue"]),
            "impression_rpm_micros": int(mv["IMPRESSION_RPM"]["microsValue"]),
            "match_rate": float(mv["MATCH_RATE"]["doubleValue"]),
            "show_rate": float(mv["SHOW_RATE"]["doubleValue"]),
        }
        rows.append(record)
    return rows

def upload_jsonl_to_gcs(records, filename):
    """Uploads newline-delimited JSON records to GCS."""
    client = storage.Client()  # :contentReference[oaicite:10]{index=10}
    blob = client.bucket(GCS_BUCKET).blob(filename)
    payload = "\n".join(json.dumps(r) for r in records)
    blob.upload_from_string(payload, content_type="application/json")
    print(f"Uploaded {len(records)} rows to gs://{GCS_BUCKET}/{filename}")  

def load_jsonl_to_bq(filename):
    """Appends the JSONL file from GCS into BigQuery."""
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )  # :contentReference[oaicite:11]{index=11}
    uri = f"gs://{GCS_BUCKET}/{filename}"
    job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {BQ_DATASET}.{BQ_TABLE}")

def main():
    # 1. Authenticate to AdMob
    creds   = get_admob_creds()
    service = get_admob_service(creds)

    # 2. Get account resource name
    PUBLISHER_ID = os.getenv("ADMOB_PUBLISHER_ID")  # e.g. 'pub-123456…'
    account_name = get_account_name(service, PUBLISHER_ID)

    # 3. Build a report spec for yesterday’s metrics
    yesterday = date.today() - timedelta(days=1)
    spec = {
        "dateRange": {
            "startDate": {"year": yesterday.year,"month": yesterday.month,"day": yesterday.day},
            "endDate":   {"year": yesterday.year,"month": yesterday.month,"day": yesterday.day}
        },
        "dimensions": [
            "DATE","MONTH","WEEK","AD_SOURCE","AD_SOURCE_INSTANCE",
            "AD_UNIT","APP","MEDIATION_GROUP","COUNTRY","APP_VERSION_NAME"
        ],
        "metrics": [
            "AD_REQUESTS","CLICKS","ESTIMATED_EARNINGS","IMPRESSIONS",
            "IMPRESSION_CTR","IMPRESSION_RPM","MATCH_RATE","SHOW_RATE"
        ],
        "sortConditions": [{"dimension":"DATE","order":"ASCENDING"}]
    }

    # 4. Fetch & parse network report
    net_stream = fetch_report(service, account_name, "network", spec)
    net_rows   = parse_rows(net_stream)

    # 5. Fetch & parse mediation report
    med_stream = fetch_report(service, account_name, "mediation", spec)
    med_rows   = parse_rows(med_stream)

    all_rows = net_rows + med_rows
    if not all_rows:
        print("No data for", yesterday)
        return

    # 6. Upload & load
    filename = f"admob_{yesterday:%Y%m%d}.jsonl"
    upload_jsonl_to_gcs(all_rows, filename)
    load_jsonl_to_bq(filename)

if __name__ == "__main__":
    main()
