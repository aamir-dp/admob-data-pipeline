import os, json
from datetime import date, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

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
missing = [k for k, v in required.items() if not v]
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

def fetch_mediation(service, account_name, report_date):
    # … your spec setup …
    response = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}",
        body={"reportSpec": spec}
    ).execute()

    # ←— NEW: dump the raw JSON response exactly as returned
    raw_filename = f"mediation_{report_date:%Y%m%d}_raw.json"
    with open(raw_filename, "w") as raw_file:
        json.dump(response, raw_file, indent=2)
    print(f"Wrote raw API response to {raw_filename}")

    # If you still want to parse rows afterward, return them or continue here…
    return response  # or [] if you no longer need row parsing

def main():
    creds       = get_admob_creds()
    service     = build_service(creds)
    report_date = date.today() - timedelta(days=1)

    raw = fetch_mediation(service, os.getenv("ADMOB_PUBLISHER_ID"), report_date)
    # raw now holds the exact JSON dict from AdMob; the file raw_filename has it on disk.

if __name__ == "__main__":
    main()
