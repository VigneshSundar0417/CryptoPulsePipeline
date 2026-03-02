import httpx
import json
from datetime import datetime
import time

API_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin,ethereum"
    "&vs_currencies=usd"
    "&include_24hr_change=true"
    "&include_market_cap=true"
    "&include_24hr_vol=true"
)
def ingest_prices():
    timeout_seconds = 5
    retry_delay_seconds = 1
    max_attempts = 2

    retry_count = 0
    start_time = time.time()
    while retry_count < max_attempts:
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                response = client.get(API_URL)
                data = response.json()

            response_time = round(time.time() - start_time, 3)

            if retry_count == 0:
                print(f"Ingestion succeeded (no retries). Response time: {response_time}s")
            else:
                print(f"Ingestion succeeded on retry {retry_count}. Response time: {response_time}s")

            break

        except Exception as e:
            retry_count += 1
            print(f"Attempt {retry_count} failed: {e}")

            if retry_count < max_attempts:
                print(f"Retrying in {retry_delay_seconds} second...")
                time.sleep(retry_delay_seconds)
            else:
                print("Ingestion failed after all attempts.")
                return {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "retry_count": retry_count,
                    "error": str(e)
                }
        # Extract cleaned fields
    btc = data.get("bitcoin", {})
    eth = data.get("ethereum", {})

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = {
        "timestamp": timestamp,
        "btc_price_usd": btc.get("usd"),
        "eth_price_usd": eth.get("usd"),
        "btc_24h_change_pct": btc.get("usd_24h_change"),
        "eth_24h_change_pct": eth.get("usd_24h_change"),
        "btc_market_cap": btc.get("usd_market_cap"),
        "eth_market_cap": eth.get("usd_market_cap"),
        "btc_volume_24h": btc.get("usd_24h_vol"),
        "eth_volume_24h": eth.get("usd_24h_vol"),
        "response_time_seconds": response_time,
        "retry_count": retry_count,
        "raw_api_response": json.dumps(data),
        "error": None
    }

    return result