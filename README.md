# Stock Data Intelligence Dashboard

A mini financial data platform built with Python, FastAPI, SQLite, Pandas, and NumPy. It demonstrates a full workflow from stock data ingestion and cleaning to REST APIs and a browser-based dashboard.

## Features

- Seeded stock dataset for six Indian market-style companies
- Optional real-data ingestion:
  - `POST /ingest/yfinance/{symbol}`
  - `POST /ingest/bhavcopy?path_or_url=...&symbol=...`
- Dynamic yfinance-backed dashboard refresh:
  - automatic sync on app startup
  - automatic background refresh for tracked dashboard symbols
  - manual refresh from the UI with `Refresh Data`
- Calculated metrics:
  - Daily Return
  - 7-day Moving Average
  - 52-week High / Low
  - Correlation between two companies
  - Custom Volatility Score
  - Mock Sentiment Index
- REST APIs:
  - `GET /companies`
  - `GET /data/{symbol}?days=30`
  - `GET /summary/{symbol}`
  - `GET /compare?symbol1=INFY&symbol2=TCS&days=90`
- Swagger docs via FastAPI at `/docs`
- Single-page dashboard with:
  - company picker
  - price chart
  - top gainers / losers
  - stock comparison snapshot

## Data Approach

This project uses a deterministic mock stock dataset by default, and also includes ingestion paths for yfinance and bhavcopy-style CSV files. The pipeline intentionally introduces a few missing and invalid values in the seeded dataset, then cleans them with Pandas so the transformation stage is visible and testable even without external APIs.

## Cleaning Steps

- Parse and normalize dates
- Convert numeric columns safely
- Remove invalid negative prices
- Forward-fill and backfill missing OHLC values
- Fill missing volume with per-symbol median

## Derived Metrics

- `daily_return = (close - open) / open`
- `moving_average_7d = 7-day rolling average of close`
- `rolling_52w_high = 252-day rolling max`
- `rolling_52w_low = 252-day rolling min`
- `volatility_score = annualized 14-day rolling std of daily returns`
- `correlation = aligned daily-return correlation between two companies`
- `sentiment_index = mock score from return, volatility, and price vs moving average`

## Run Locally

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Coding Standards

This project uses professional Python code-quality tools so the backend stays consistent and review-friendly.

Install dependencies:

```bash
pip install -r requirements.txt
```

Format all Python code in the project:

```bash
black .
ruff format .
```

Lint and auto-fix simple issues across the project:

```bash
ruff check . --fix
```

Run a full standards check before submission:

```bash
ruff check .
black . --check
ruff format . --check
```

Recommended command order:

```bash
ruff check . --fix
black .
ruff format .
```

Windows PowerShell shortcut:

```powershell
.\scripts\quality.ps1
```

Open:

- Dashboard: `http://127.0.0.1:8000/`
- Swagger UI: `http://127.0.0.1:8000/docs`

## Project Structure

```text
app/
  main.py
  database.py
  schemas.py
  services/
    data_pipeline.py
    repository.py
  static/
    index.html
data/
requirements.txt
README.md
```

## Notes

- SQLite is used for portability and easy review.
- The seeded dataset is generated automatically on app startup.
- You can replace any seeded symbol with live yfinance data or import bhavcopy-style CSV data.
- The dashboard is intentionally lightweight and dependency-free on the frontend.


## Dashboard Preview




## Author

Built as part of the Jarnox Internship Assignment.

Developer: Lokith Aksha S https://github.com/Lokith03/Portfolio
