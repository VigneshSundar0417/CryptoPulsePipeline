import streamlit as st
import duckdb
import pandas as pd
import time

DB_PATH = "storage/realtime_crypto.duckdb"

def load_data(limit=100):
    conn = duckdb.connect(DB_PATH)
    df = conn.execute("""
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
    """, [limit]).df()
    conn.close()
    return df

st.set_page_config(page_title="CryptoPulse Dashboard", layout="wide")

st.title("📈 CryptoPulse Web Dashboard")

placeholder = st.empty()

alert_threshold = st.sidebar.slider("Price movement alert threshold (%)", 0.5, 5.0, 1.0)

ma_window = st.sidebar.slider("Rolling average window (points)", 3, 50, 10)

while True:
    df = load_data(200)
    df_sorted = df.sort_values("timestamp")
    df_sorted["btc_ma"] = df_sorted["btc_price_usd"].rolling(window=ma_window).mean()
    df_sorted["eth_ma"] = df_sorted["eth_price_usd"].rolling(window=ma_window).mean()

    # Price movement alert (last 10 points)
    if len(df_sorted) >= 10:
        btc_old = df_sorted["btc_price_usd"].iloc[-10]
        btc_new = df_sorted["btc_price_usd"].iloc[-1]
        btc_change = ((btc_new - btc_old) / btc_old) * 100

        eth_old = df_sorted["eth_price_usd"].iloc[-10]
        eth_new = df_sorted["eth_price_usd"].iloc[-1]
        eth_change = ((eth_new - eth_old) / eth_old) * 100

        if abs(btc_change) >= alert_threshold:
            st.warning(f"BTC moved {btc_change:.2f}% in the last few minutes!")

        if abs(eth_change) >= alert_threshold:
            st.warning(f"ETH moved {eth_change:.2f}% in the last few minutes!")

    with placeholder.container():
        latest = df.iloc[0]

        col1, col2, col3 = st.columns(3)
        col1.metric("BTC Price (USD)", f"${latest.btc_price_usd:,.2f}", f"{latest.btc_24h_change_pct:.2f}%")
        col2.metric("ETH Price (USD)", f"${latest.eth_price_usd:,.2f}", f"{latest.eth_24h_change_pct:.2f}%")
        col3.metric("Response Time", f"{latest.response_time_seconds:.3f}s", f"Retries: {latest.retry_count}")
        
        st.subheader("BTC Price Trend (with Rolling Average)")
        btc_chart_data = df_sorted[["btc_price_usd", "btc_ma"]]
        st.line_chart(btc_chart_data)

        st.subheader("ETH Price Trend (with Rolling Average)")
        eth_chart_data = df_sorted[["eth_price_usd", "eth_ma"]]
        st.line_chart(eth_chart_data)

        st.subheader("BTC vs ETH Comparison")
        comparison_data = df_sorted[["btc_price_usd", "eth_price_usd"]]
        st.line_chart(comparison_data)

    time.sleep(5)