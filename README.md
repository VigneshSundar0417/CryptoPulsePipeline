# CryptoPulse: Real-Time Crypto Monitoring Pipeline

CryptoPulse is a real-time data engineering project that ingests live Bitcoin and Ethereum prices, stores them in DuckDB, and visualizes trends through an interactive Streamlit dashboard.

## Features
- Real-time BTC & ETH ingestion pipeline (CoinGecko API)
- DuckDB storage with structured logging
- Interactive Streamlit dashboard
- Adjustable rolling average smoothing
- Adjustable alert thresholds
- BTC vs ETH comparison chart
- Production-style architecture and error handling

## Architecture
Ingestion → Storage → Dashboard

## Tech Stack
- Python
- DuckDB
- Streamlit
- CoinGecko API
- Pandas

## How to Run

### Start the ingestion pipeline:
