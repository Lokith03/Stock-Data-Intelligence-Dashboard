from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.schemas import ComparisonResponse, Company, DashboardPayload, StockPoint, Summary
from app.services.data_pipeline import (
    fetch_yfinance_history,
    fetch_market_symbol_history,
    load_market_csv,
    save_company_and_prices,
    seed_database,
)
from app.services.repository import (
    fetch_companies,
    fetch_compare,
    fetch_market_movers,
    fetch_stock_data,
    fetch_summary,
)


app = FastAPI(
    title="Stock Data Intelligence Dashboard",
    description="Mini financial data platform with cleaned stock data, summary analytics, and a lightweight dashboard.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).resolve().parent / "static"
REALTIME_COMPANIES = {
    "INFY": {"ticker": "INFY.NS", "name": "Infosys Limited", "sector": "Technology"},
    "TCS": {"ticker": "TCS.NS", "name": "Tata Consultancy Services", "sector": "Technology"},
    "RELIANCE": {"ticker": "RELIANCE.NS", "name": "Reliance Industries", "sector": "Energy"},
    "HDFCBANK": {"ticker": "HDFCBANK.NS", "name": "HDFC Bank", "sector": "Banking"},
    "ICICIBANK": {"ticker": "ICICIBANK.NS", "name": "ICICI Bank", "sector": "Banking"},
}
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

LAST_REALTIME_REFRESH: datetime | None = None
LAST_REALTIME_ERROR: str | None = None
REFRESH_TTL_MINUTES = 15


def refresh_tracked_symbols(period: str = "6mo") -> dict:
    global LAST_REALTIME_REFRESH, LAST_REALTIME_ERROR

    imported = []
    errors = []
    for company_symbol, details in REALTIME_COMPANIES.items():
        try:
            frame, company = fetch_market_symbol_history(
                ticker_symbol=details["ticker"],
                company_symbol=company_symbol,
                company_name=details["name"],
                sector=details["sector"],
                period=period,
            )
            save_company_and_prices(company, frame)
            imported.append({"symbol": company_symbol, "rows": len(frame), "ticker": details["ticker"]})
        except Exception as exc:
            errors.append({"symbol": company_symbol, "ticker": details["ticker"], "error": str(exc)})

    if imported:
        LAST_REALTIME_REFRESH = datetime.utcnow()
        LAST_REALTIME_ERROR = None if not errors else "; ".join(
            f"{item['symbol']}: {item['error']}" for item in errors
        )
        return {
            "message": "Realtime market data refreshed from yfinance.",
            "imported": imported,
            "errors": errors,
            "status": "partial_success" if errors else "success",
        }

    LAST_REALTIME_ERROR = "; ".join(f"{item['symbol']}: {item['error']}" for item in errors) or "Unknown refresh error."
    raise RuntimeError(LAST_REALTIME_ERROR)


def ensure_realtime_data(force: bool = False) -> None:
    global LAST_REALTIME_REFRESH

    if not force and LAST_REALTIME_REFRESH is not None:
        if datetime.utcnow() - LAST_REALTIME_REFRESH < timedelta(minutes=REFRESH_TTL_MINUTES):
            return

    try:
        refresh_tracked_symbols()
    except Exception:
        # Keep the app serving cached SQLite data if yfinance or network is temporarily unavailable.
        pass


@app.on_event("startup")
def startup_event() -> None:
    seed_database()
    ensure_realtime_data(force=True)


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/companies", response_model=list[Company])
def get_companies() -> list[Company]:
    ensure_realtime_data()
    return [Company(**company) for company in fetch_companies()]


@app.get("/data/{symbol}", response_model=list[StockPoint])
def get_stock_data(symbol: str, days: int = Query(default=30, ge=7, le=365)) -> list[StockPoint]:
    if symbol.upper() in REALTIME_COMPANIES:
        ensure_realtime_data()
    items = fetch_stock_data(symbol, days)
    if not items:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol.upper()}' not found.")
    return [StockPoint(**item) for item in items]


@app.get("/summary/{symbol}", response_model=Summary)
def get_summary(symbol: str) -> Summary:
    if symbol.upper() in REALTIME_COMPANIES:
        ensure_realtime_data()
    summary = fetch_summary(symbol)
    if not summary:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol.upper()}' not found.")
    return Summary(**summary)


@app.get("/compare", response_model=ComparisonResponse)
def compare_stocks(
    symbol1: str = Query(..., min_length=2),
    symbol2: str = Query(..., min_length=2),
    days: int = Query(default=90, ge=30, le=252),
) -> ComparisonResponse:
    if symbol1.upper() in REALTIME_COMPANIES or symbol2.upper() in REALTIME_COMPANIES:
        ensure_realtime_data()
    if symbol1.upper() == symbol2.upper():
        raise HTTPException(status_code=400, detail="Please choose two different symbols.")

    comparison = fetch_compare(symbol1, symbol2, days)
    if not comparison:
        raise HTTPException(status_code=404, detail="One or both symbols were not found.")
    return ComparisonResponse(**comparison)


@app.get("/dashboard-data", response_model=DashboardPayload)
def get_dashboard_data() -> DashboardPayload:
    ensure_realtime_data()
    companies = [Company(**company) for company in fetch_companies()]
    gainers, losers = fetch_market_movers()
    return DashboardPayload(
        companies=companies,
        top_gainers=[Summary(**item) for item in gainers],
        top_losers=[Summary(**item) for item in losers],
    )


@app.get("/correlation")
def get_correlation(days: int = Query(default=90, ge=30, le=252)) -> dict:
    ensure_realtime_data()
    symbols = list(REALTIME_COMPANIES.keys())
    series: dict[str, list[dict]] = {symbol: fetch_stock_data(symbol, days) for symbol in symbols}
    available = {symbol: rows for symbol, rows in series.items() if rows}
    if len(available) < 2:
        raise HTTPException(status_code=404, detail="Not enough company data available for correlation.")

    import pandas as pd

    frames = []
    for symbol, rows in available.items():
        frame = pd.DataFrame(rows)[["date", "daily_return"]].copy()
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.rename(columns={"daily_return": symbol})
        frames.append(frame)

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="date", how="inner")

    matrix = merged.drop(columns=["date"]).corr().round(2).fillna(0.0)
    return {"symbols": list(matrix.columns), "matrix": matrix.values.tolist(), "days": days}


@app.post("/ingest/yfinance/{symbol}")
def ingest_from_yfinance(symbol: str, period: str = Query(default="18mo")) -> dict:
    try:
        frame, company = fetch_yfinance_history(symbol, period=period)
        save_company_and_prices(company, frame)
        return {
            "message": f"Imported {len(frame)} rows for {company['symbol']} from yfinance.",
            "symbol": company["symbol"],
            "source": "yfinance",
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/ingest/bhavcopy")
def ingest_from_bhavcopy(
    path_or_url: str = Query(..., description="Local CSV path or bhavcopy CSV URL"),
    symbol: str | None = Query(default=None),
    company_name: str | None = Query(default=None),
    sector: str = Query(default="Imported"),
) -> dict:
    try:
        frame, company = load_market_csv(
            path_or_url=path_or_url,
            symbol=symbol,
            company_name=company_name,
            sector=sector,
        )
        save_company_and_prices(company, frame)
        return {
            "message": f"Imported {len(frame)} rows for {company['symbol']} from bhavcopy-style CSV.",
            "symbol": company["symbol"],
            "source": "bhavcopy_csv",
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/refresh/realtime")
def refresh_realtime_data(period: str = Query(default="6mo")) -> dict:
    try:
        return refresh_tracked_symbols(period=period)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/refresh/status")
def refresh_status() -> dict:
    return {
        "source": "yfinance",
        "refresh_ttl_minutes": REFRESH_TTL_MINUTES,
        "last_refresh_utc": LAST_REALTIME_REFRESH.isoformat() if LAST_REALTIME_REFRESH else None,
        "last_error": LAST_REALTIME_ERROR,
        "tracked_symbols": list(REALTIME_COMPANIES.keys()),
    }
