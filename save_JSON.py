import os
import json
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
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

CLIENT_ID     = required["ADMOB_CLIENT_ID"]
CLIENT_SECRET = required["ADMOB_CLIENT_SECRET"]
REFRESH_TOKEN = required["ADMOB_REFRESH_TOKEN"]
PUBLISHER_ID  = required["ADMOB_PUBLISHER_ID"]
API_SCOPE     = "https://www.googleapis.com/auth/admob.report"

# ─── AUTHENTICATION ────────────────────────────────────────────────────────────
def get_admob_creds():
    """
    Refreshes OAuth2 credentials using your refresh token.
    """
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[API_SCOPE],
    )  # Uses google-auth Request transport :contentReference[oaicite:0]{index=0}
    creds.refresh(Request())
    return creds

def build_service(creds):
    """
    Builds the AdMob API client.
    """
    return build("admob", "v1", credentials=creds, cache_discovery=False)

# ─── FETCH AND SAVE RAW JSON ───────────────────────────────────────────────────
def fetch_and_save_raw_json(service, account_name, report_date):
    """
    Calls accounts.mediationReport.generate() and saves the raw JSON response.
    """
    # Construct the MediationReportSpec exactly as required by the API :contentReference[oaicite:1]{index=1}
    spec = {
        "dateRange": {
            "startDate": {
                "year":  report_date.year,
                "month": report_date.month,
                "day":   report_date.day,
            },
            "endDate": {
                "year":  report_date.year,
                "month": report_date.month,
                "day":   report_date.day,
            },
        },
        # Only one time dimension allowed: DATE :contentReference[oaicite:2]{index=2}
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
        "sortConditions": [
            {"dimension": "DATE", "order": "ASCENDING"}
        ]
    }  # Matches the MediationReportSpec JSON schema :contentReference[oaicite:3]{index=3}

    # Execute the API call; response is a dict representing the JSON payload :contentReference[oaicite:4]{index=4}
    response = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}",
        body={"reportSpec": spec}
    ).execute()

    # Write the raw JSON to a file
    filename = f"mediation_{report_date:%Y%m%d}_raw.json"
    with open(filename, "w") as f:
        json.dump(response, f, indent=2)
    print(f"Wrote raw API response to {filename}")

    return response

def main():
    creds       = get_admob_creds()                            # Authenticate :contentReference[oaicite:5]{index=5}
    service     = build_service(creds)                         # Build client :contentReference[oaicite:6]{index=6}
    report_date = date.today() - timedelta(days=1)             # Yesterday’s date

    # Fetch and save the raw mediation report JSON
    fetch_and_save_raw_json(service, PUBLISHER_ID, report_date)

if __name__ == "__main__":
    main()
