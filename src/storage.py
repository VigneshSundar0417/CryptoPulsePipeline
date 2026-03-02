import duckdb

DB_PATH = "storage/realtime_crypto.duckdb"

def insert_record(record):
    conn = duckdb.connect(DB_PATH)

    conn.execute("""
        INSERT INTO crypto_prices (
            timestamp,
            btc_price_usd,
            eth_price_usd,
            btc_24h_change_pct,
            eth_24h_change_pct,
            btc_market_cap,
            eth_market_cap,
            btc_volume_24h,
            eth_volume_24h,
            response_time_seconds,
            retry_count,
            raw_api_response,
            error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        record["timestamp"],
        record["btc_price_usd"],
        record["eth_price_usd"],
        record["btc_24h_change_pct"],
        record["eth_24h_change_pct"],
        record["btc_market_cap"],
        record["eth_market_cap"],
        record["btc_volume_24h"],
        record["eth_volume_24h"],
        record["response_time_seconds"],
        record["retry_count"],
        record["raw_api_response"],
        record["error"]
    ])

    conn.close()