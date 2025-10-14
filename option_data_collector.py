import requests
import pandas as pd
from datetime import datetime
import schedule
import time
import os

def fetch_nse_option_data(symbol="NIFTY"):
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }

    session = requests.Session()
    response = session.get(url, headers=headers)
    data = response.json()["records"]["data"]

    ce_data = []
    pe_data = []

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

    df = pd.DataFrame(ce_data + pe_data)
    return df


def job():
    df = fetch_nse_option_data("NIFTY")
    os.makedirs("data", exist_ok=True)
    filename = f"data/nifty_option_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Data saved to {filename} at {datetime.now()}")


# Run every 5 minutes
schedule.every(5).minutes.do(job)

print("🚀 Option Data Collector started...")
job()  # Run once at startup

while True:
    schedule.run_pending()
    time.sleep(1)
