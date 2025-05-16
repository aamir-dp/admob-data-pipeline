import os
from datetime import date, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ─── CONFIG via ENV ────────────────────────────────────────────────────────────
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
    # Only DATE as time dimension; removed APP_VERSION_NAME
    dimensions = [
        "DATE",
        "APP", "AD_UNIT",
        "AD_SOURCE", "AD_SOURCE_INSTANCE", "MEDIATION_GROUP",
        "COUNTRY"
    ]
    metrics = [
        "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
        "IMPRESSION_CTR", "MATCHED_REQUESTS", "MATCH_RATE", "OBSERVED_ECPM"
    ]

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
        "dimensions": dimensions,
        "metrics":    metrics,
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
        dv_list = chunk["row"]["dimensionValues"]
        mv_list = chunk["row"]["metricValues"]

        record = {}
        for i, dim in enumerate(dimensions):
            record[dim.lower()] = dv_list[i]["value"]
        for i, met in enumerate(metrics):
            mv = mv_list[i]
            if "integerValue" in mv:
                record[met.lower()] = int(mv["integerValue"])
            elif "doubleValue" in mv:
                record[met.lower()] = float(mv["doubleValue"])
            elif "microsValue" in mv:
                record[met.lower() + "_micros"] = int(mv["microsValue"])
        rows.append(record)

    return rows

def main():
    creds   = get_admob_creds()
    service = build_service(creds)
    report_date = date.today() - timedelta(days=1)
    rows = fetch_mediation(service, os.getenv("ADMOB_PUBLISHER_ID"), report_date)
    for r in rows:
        print(r)

if __name__ == "__main__":
    main()
