import os
import time
import duckdb

DB_PATH = "storage/realtime_crypto.duckdb"

def fetch_latest(n=20):
    conn = duckdb.connect(DB_PATH)
    rows = conn.execute("""
        SELECT 
            timestamp,
            btc_price_usd,
            eth_price_usd,
            btc_24h_change_pct,
            eth_24h_change_pct,
            response_time_seconds,
            retry_count
        FROM crypto_prices
        ORDER BY timestamp DESC
        LIMIT ?
    """, [n]).fetchall()
    conn.close()
    return rows

def run_dashboard():
    while True:
        os.system("clear")

        rows = fetch_latest(20)
        if not rows:
            print("No data yet...")
            time.sleep(2)
            continue

        latest = rows[0]

        print("=== CryptoPulse Dashboard ===")
        print(f"Last update: {latest[0]}")
        print()
        print(f"BTC: ${latest[1]:,.2f}   (24h: {latest[3]:.2f}%)")
        print(f"ETH: ${latest[2]:,.2f}   (24h: {latest[4]:.2f}%)")
        print()
        print(f"Response time: {latest[5]:.3f}s   Retries: {latest[6]}")
        print()
        print("Recent BTC prices:")
        btc_prices = [row[1] for row in rows[::-1]]
        print("  " + " ".join(f"{p:.0f}" for p in btc_prices))

        print()
        print("Recent ETH prices:")
        eth_prices = [row[2] for row in rows[::-1]]
        print("  " + " ".join(f"{p:.0f}" for p in eth_prices))

        time.sleep(5)

if __name__ == "__main__":
    run_dashboard()