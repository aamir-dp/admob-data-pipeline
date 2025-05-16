import os
from datetime import date, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ─── CONFIG via ENV ─────────────────────────────
CLIENT_ID     = os.getenv("ADMOB_CLIENT_ID")
CLIENT_SECRET = os.getenv("ADMOB_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("ADMOB_REFRESH_TOKEN")
PUBLISHER_ID  = os.getenv("ADMOB_PUBLISHER_ID")
API_SCOPE     = "https://www.googleapis.com/auth/admob.report"

def get_admob_creds():
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[API_SCOPE],
    )
    creds.refresh(Request())  # safe refresh call :contentReference[oaicite:9]{index=9}
    return creds

def build_service(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

def fetch_mediation(service, account_name, report_date):
    dims = [
      "DATE",
      "APP", "AD_UNIT",
      "AD_SOURCE", "AD_SOURCE_INSTANCE", "MEDIATION_GROUP",
      "COUNTRY"
    ]
    mets = [
      "AD_REQUESTS", "CLICKS", "ESTIMATED_EARNINGS", "IMPRESSIONS",
      "IMPRESSION_CTR", "MATCHED_REQUESTS", "MATCH_RATE", "OBSERVED_ECPM"
    ]

    spec = {
      "dateRange": {
        "startDate": {"year": report_date.year, "month": report_date.month, "day": report_date.day},
        "endDate":   {"year": report_date.year, "month": report_date.month, "day": report_date.day}
      },
      "dimensions":    dims,
      "metrics":       mets,
      "sortConditions":[{"dimension":"DATE","order":"ASCENDING"}]
    }

    resp = service.accounts().mediationReport().generate(
      parent=f"accounts/{account_name}", body={"reportSpec": spec}
    ).execute()  # returns a list of chunks :contentReference[oaicite:10]{index=10}

    rows = []
    for chunk in resp:
        row = chunk.get("row")
        if not row:
            continue
        dv = row["dimensionValues"]  # dict keyed by dim name
        mv = row["metricValues"]     # dict keyed by metric name

        record = {}
        # Extract dimensions
        for d in dims:
            record[d.lower()] = dv.get(d, {}).get("value")
        # Extract metrics safely
        for m in mets:
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

def main():
    creds   = get_admob_creds()
    service = build_service(creds)
    report_date = date.today() - timedelta(days=1)
    rows = fetch_mediation(service, os.getenv("ADMOB_PUBLISHER_ID"), report_date)
    for r in rows:
        print(r)

if __name__ == "__main__":
    main()
