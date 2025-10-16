import requests
import pandas as pd
from datetime import datetime
import schedule
import time
import os
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

# === GOOGLE DRIVE CONFIG ===
SERVICE_ACCOUNT_FILE = "nse-drive-key.json"  # Secret file added in Render
FOLDER_ID = "YOUR_GOOGLE_DRIVE_FOLDER_ID"   # Replace with your Drive folder ID

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=creds)

def upload_to_drive(file_path):
    """Upload a CSV file to Google Drive folder."""
    file_metadata = {
        "name": os.path.basename(file_path),
        "parents": [FOLDER_ID]
    }
    media = MediaFileUpload(file_path, mimetype="text/csv")
    drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"☁️ Uploaded to Google Drive: {os.path.basename(file_path)}")

# === NSE DATA FETCH FUNCTION ===
def fetch_nse_option_data(symbol="NIFTY"):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    base_url = "https://www.nseindia.com"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    session = requests.Session()
    session.headers.update(headers)
    session.get(base_url, timeout=5)  # Get cookies
    response = session.get(url, timeout=10)

    data = response.json().get("records", {}).get("data", [])
    ce_data, pe_data = [], []

    for item in data:
        strike = item["strikePrice"]
        expiry = item["expiryDate"]

        if "CE" in item:
            ce = item["CE"]
            ce_data.append({
                "timestamp": datetime.now(),
                "expiry": expiry,
                "strike": strike,
                "type": "CE",
                "LTP": ce.get("lastPrice"),
                "OI": ce.get("openInterest"),
                "ChangeOI": ce.get("changeinOpenInterest")
            })

        if "PE" in item:
            pe = item["PE"]
            pe_data.append({
                "timestamp": datetime.now(),
                "expiry": expiry,
                "strike": strike,
                "type": "PE",
                "LTP": pe.get("lastPrice"),
                "OI": pe.get("openInterest"),
                "ChangeOI": pe.get("changeinOpenInterest")
            })

    return pd.DataFrame(ce_data + pe_data)

# === MAIN JOB FUNCTION ===
def job():
    try:
        df = fetch_nse_option_data("NIFTY")
        os.makedirs("data", exist_ok=True)
        filename = f"data/nifty_option_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved: {filename}")

        upload_to_drive(filename)  # Upload CSV to Google Drive
    except Exception as e:
        print(f"❌ Error: {e} at {datetime.now()}")

# === SCHEDULER ===
schedule.every(5).minutes.do(job)  # Runs every 5 minutes

print("🚀 NSE Option Data Collector started with Google Drive backup...")
job()  # Run once at startup

while True:
    schedule.run_pending()
    time.sleep(1)
