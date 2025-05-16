import os
from datetime import date, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Environment variables
CLIENT_ID       = os.getenv("ADMOB_CLIENT_ID")
CLIENT_SECRET   = os.getenv("ADMOB_CLIENT_SECRET")
REFRESH_TOKEN   = os.getenv("ADMOB_REFRESH_TOKEN")
PUBLISHER_ID    = os.getenv("ADMOB_PUBLISHER_ID")
API_SCOPE       = "https://www.googleapis.com/auth/admob.report"

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
        "dimensions": [
            "DATE",            # only this time dimension
            "APP", "AD_UNIT",
            "AD_SOURCE", "AD_SOURCE_INSTANCE", "MEDIATION_GROUP",
            "COUNTRY", "APP_VERSION_NAME"
        ],
        "metrics": [
            "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
            "IMPRESSION_CTR", "MATCHED_REQUESTS", "MATCH_RATE", "OBSERVED_ECPM"
        ],
        "sortConditions": [
            {"dimension": "DATE", "order": "ASCENDING"}
        ]
    }

    response = service.accounts().mediationReport().generate(
        parent=f"accounts/{account_name}",
        body={"reportSpec": spec}
    ).execute()

    rows = []
    for chunk in response:
        if "row" not in chunk:
            continue
        dv = chunk["row"]["dimensionValues"]
        mv = chunk["row"]["metricValues"]
        rows.append({
            "date":                      dv["DATE"]["value"],
            "app":                       dv["APP"]["value"],
            "ad_unit":                   dv["AD_UNIT"]["value"],
            "ad_source":                 dv["AD_SOURCE"]["value"],
            "ad_source_instance":        dv["AD_SOURCE_INSTANCE"]["value"],
            "mediation_group":           dv["MEDIATION_GROUP"]["value"],
            "country":                   dv["COUNTRY"]["value"],
            "app_version_name":          dv["APP_VERSION_NAME"]["value"],
            "ad_requests":               int(mv["AD_REQUESTS"]["integerValue"]),
            "clicks":                    int(mv["CLICKS"]["integerValue"]),
            "estimated_earnings_micros": int(mv["ESTIMATED_EARNINGS"]["microsValue"]),
            "impressions":               int(mv["IMPRESSIONS"]["integerValue"]),
            "impression_ctr":            float(mv["IMPRESSION_CTR"]["doubleValue"]),
            "matched_requests":          int(mv["MATCHED_REQUESTS"]["integerValue"]),
            "match_rate":                float(mv["MATCH_RATE"]["doubleValue"]),
            "observed_ecpm_micros":      int(mv["OBSERVED_ECPM"]["microsValue"])
        })
    return rows

def main():
    creds   = get_admob_creds()
    service = build_service(creds)
    # PUBLISHER_ID should be the 'pub-XXXX' string
    report_date = date.today() - timedelta(days=1)
    rows = fetch_mediation(service, os.getenv("ADMOB_PUBLISHER_ID"), report_date)
    for r in rows:
        print(r)

if __name__ == "__main__":
    main()
