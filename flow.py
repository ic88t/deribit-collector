import time
import json
import requests
from datetime import datetime, timedelta
from google.cloud import storage

# 0) Configuration
CURRENCY     = "BTC"
KIND         = "option"
HISTORY_HOST = "https://history.deribit.com"
OUTPUT_FILE  = "all_trades.json"
GCS_BUCKET   = "anfi_json"  # Update to match the bucket name used in the error
GCS_FOLDER   = "deribit_trades"     # Optional folder within the bucket

# 1) Fetch all live BTC options
resp = requests.get(
    "https://www.deribit.com/api/v2/public/get_instruments",
    params={"currency": CURRENCY, "kind": KIND}
)
resp.raise_for_status()
instruments = resp.json()["result"]

# 2) Filter to those expiring within the next 30 days
now       = datetime.utcnow()
one_month = now + timedelta(days=30)
now_ms       = int(now.timestamp() * 1000)
one_month_ms = int(one_month.timestamp() * 1000)

candidates = [
    ins for ins in instruments
    if now_ms <= ins["expiration_timestamp"] <= one_month_ms
]

print(f"→ Found {len(candidates)} {CURRENCY} options expiring by {one_month.date()}")

# 3) Prepare our 7‑day window
start_ts = int((now - timedelta(days=7)).timestamp() * 1000)
end_ts   = now_ms

all_trades = {}

# 4) Loop through each instrument
for ins in candidates:
    name = ins["instrument_name"]
    print(f"Fetching trades for {name}…", end="", flush=True)

    params = {
        "instrument_name": name,
        "start_timestamp": start_ts,
        "end_timestamp":   end_ts,
        "count":           1000
    }

    trades = []
    while True:
        r = requests.get(
            f"{HISTORY_HOST}/api/v2/public/get_last_trades_by_instrument_and_time",
            params=params
        )
        r.raise_for_status()
        result = r.json()["result"]

        # append this batch
        trades.extend(result.get("trades", []))

        # pagination: either continuation token or has_more flag
        if "continuation" in result:
            params["continuation"] = result["continuation"]
        elif result.get("has_more"):
            # advance start to last timestamp +1
            last_ts = trades[-1]["timestamp"]
            params["start_timestamp"] = last_ts + 1
        else:
            break

        time.sleep(0.1)

    print(f" {len(trades):,} trades")
    all_trades[name] = trades

# 5) Convert ms timestamps → ISO8601
for name, trades in all_trades.items():
    for t in trades:
        ms = t.get("timestamp")
        if ms is not None:
            dt = datetime.utcfromtimestamp(ms / 1000)
            t["timestamp"] = dt.isoformat() + "Z"

# 6) Write out JSON locally
with open(OUTPUT_FILE, "w") as f:
    json.dump(all_trades, f, indent=2)

print(f"Saved trades for {len(all_trades)} instruments to {OUTPUT_FILE}")

# 7) Upload to Google Cloud Storage
try:
    # Use a fixed filename instead of timestamp-based one
    gcs_filename = f"{GCS_FOLDER}/{CURRENCY}_{KIND}_trades.json"
    
    # Initialize GCS client and upload the file
    credentials_path = "D:/anfiai/public/login.json"  # Update this with the full path to your credentials.json
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_filename)
    
    # Upload JSON data as a string
    blob.upload_from_string(
        data=json.dumps(all_trades, indent=2),
        content_type="application/json"
    )
    
    print(f"Successfully uploaded to GCS: gs://{GCS_BUCKET}/{gcs_filename}")
except Exception as e:
    print(f"Error uploading to Google Cloud Storage: {e}")

print("\nDone!")
