from __future__ import annotations

import math
from io import StringIO
from datetime import date, timedelta
from typing import Iterable

import numpy as np
import pandas as pd
import requests

from app.database import get_connection


COMPANIES = [
    {"symbol": "INFY", "name": "Infosys Ltd.", "sector": "Technology", "base_price": 1525.0},
    {"symbol": "TCS", "name": "Tata Consultancy Services", "sector": "Technology", "base_price": 3810.0},
    {"symbol": "RELIANCE", "name": "Reliance Industries", "sector": "Energy", "base_price": 2870.0},
    {"symbol": "HDFCBANK", "name": "HDFC Bank", "sector": "Banking", "base_price": 1670.0},
    {"symbol": "ICICIBANK", "name": "ICICI Bank", "sector": "Banking", "base_price": 1130.0},
    {"symbol": "SBIN", "name": "State Bank of India", "sector": "Banking", "base_price": 820.0},
]

EXPECTED_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


def _business_days(num_days: int = 420) -> pd.DatetimeIndex:
    end = date.today()
    start = end - timedelta(days=int(num_days * 1.6))
    return pd.bdate_range(start=start, end=end)[-num_days:]


def _build_symbol_frame(symbol: str, base_price: float, index: int, days: Iterable[pd.Timestamp]) -> pd.DataFrame:
    seed = sum(ord(char) for char in symbol) + index * 17
    rng = np.random.default_rng(seed)
    dates = pd.Index(days)
    t = np.arange(len(dates))

    market_wave = np.sin(t / 21.0) * 0.010
    symbol_wave = np.cos((t + index * 3) / 13.0) * 0.006
    trend = 0.0004 + (index - 2) * 0.00003
    noise = rng.normal(0.0, 0.012, len(dates))
    daily_return_curve = trend + market_wave + symbol_wave + noise

    close_prices = [base_price]
    for change in daily_return_curve[1:]:
        close_prices.append(max(close_prices[-1] * (1 + change), base_price * 0.55))

    closes = np.array(close_prices)
    opens = closes * (1 - daily_return_curve * 0.35 + rng.normal(0.0, 0.004, len(dates)))
    highs = np.maximum(opens, closes) * (1 + rng.uniform(0.002, 0.018, len(dates)))
    lows = np.minimum(opens, closes) * (1 - rng.uniform(0.002, 0.018, len(dates)))
    volume = rng.integers(900_000, 8_000_000, len(dates))

    frame = pd.DataFrame(
        {
            "date": dates,
            "symbol": symbol,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

    if len(frame) > 30:
        frame.loc[frame.index[10], "close"] = np.nan
        frame.loc[frame.index[20], "volume"] = np.nan
        frame.loc[frame.index[25], "open"] = frame.loc[frame.index[25], "open"] * -1

    return frame


def generate_stock_data() -> pd.DataFrame:
    days = _business_days()
    frames = [
        _build_symbol_frame(company["symbol"], company["base_price"], idx, days)
        for idx, company in enumerate(COMPANIES)
    ]
    return pd.concat(frames, ignore_index=True)


def _rename_market_columns(frame: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "Date": "date",
        "DATE": "date",
        "DATE1": "date",
        "TIMESTAMP": "date",
        "Datetime": "date",
        "Symbol": "symbol",
        "SYMBOL": "symbol",
        "Open": "open",
        "OPEN": "open",
        "High": "high",
        "HIGH": "high",
        "Low": "low",
        "LOW": "low",
        "Close": "close",
        "CLOSE": "close",
        "Prev Close": "close",
        "Volume": "volume",
        "VOLUME": "volume",
        "TOTTRDQTY": "volume",
        "TOTTRDVAL": "volume",
    }
    return frame.rename(columns=rename_map)


def normalize_external_frame(
    frame: pd.DataFrame,
    default_symbol: str | None = None,
    company_name: str | None = None,
    sector: str = "Imported",
    source_label: str = "external",
) -> tuple[pd.DataFrame, dict]:
    normalized = _rename_market_columns(frame.copy())
    for column in EXPECTED_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = np.nan

    if default_symbol:
        normalized["symbol"] = normalized["symbol"].fillna(default_symbol.upper())
    normalized["symbol"] = normalized["symbol"].astype(str).str.upper()

    normalized = normalized[EXPECTED_COLUMNS]
    cleaned = clean_stock_data(normalized)
    enriched = add_metrics(cleaned)
    if enriched.empty:
        raise RuntimeError("The imported dataset did not contain usable rows after cleaning.")
    enriched["sentiment_index"] = (
        np.clip(
            50
            + (enriched["daily_return"] * 850)
            - (enriched["volatility_score"] * 0.65)
            + ((enriched["close"] - enriched["moving_average_7d"]) / enriched["moving_average_7d"]) * 120,
            0,
            100,
        )
    ).round(2)
    enriched["data_source"] = source_label

    symbol = str(enriched["symbol"].iloc[0])
    company_record = {
        "symbol": symbol,
        "name": company_name or symbol,
        "sector": sector,
        "base_price": round(float(enriched["close"].iloc[0]), 2),
    }
    return enriched, company_record


def fetch_yfinance_history(symbol: str, period: str = "18mo") -> tuple[pd.DataFrame, dict]:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run pip install -r requirements.txt.") from exc

    ticker = yf.Ticker(symbol)
    history = ticker.history(period=period, interval="1d", auto_adjust=False)
    if history.empty:
        raise RuntimeError(f"No yfinance data returned for symbol '{symbol}'.")

    history = history.reset_index()
    info = getattr(ticker, "info", {}) or {}
    enriched, company_record = normalize_external_frame(
        history,
        default_symbol=symbol,
        company_name=info.get("shortName") or info.get("longName") or symbol.upper(),
        sector=info.get("sector") or "Imported",
        source_label="yfinance",
    )
    return enriched, company_record


def fetch_market_symbol_history(
    ticker_symbol: str,
    company_symbol: str,
    company_name: str | None = None,
    sector: str = "Imported",
    period: str = "18mo",
) -> tuple[pd.DataFrame, dict]:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run pip install -r requirements.txt.") from exc

    ticker = yf.Ticker(ticker_symbol)
    history = ticker.history(period=period, interval="1d", auto_adjust=False)
    if history.empty:
        raise RuntimeError(f"No yfinance data returned for symbol '{ticker_symbol}'.")

    history = history.reset_index()
    info = getattr(ticker, "info", {}) or {}
    enriched, company_record = normalize_external_frame(
        history,
        default_symbol=company_symbol,
        company_name=company_name or info.get("shortName") or info.get("longName") or company_symbol.upper(),
        sector=sector or info.get("sector") or "Imported",
        source_label=f"yfinance:{ticker_symbol}",
    )
    company_record["symbol"] = company_symbol.upper()
    company_record["name"] = company_name or company_record["name"]
    company_record["sector"] = sector or company_record["sector"]
    return enriched, company_record


def load_market_csv(
    path_or_url: str,
    symbol: str | None = None,
    company_name: str | None = None,
    sector: str = "Imported",
) -> tuple[pd.DataFrame, dict]:
    if path_or_url.lower().startswith(("http://", "https://")):
        response = requests.get(path_or_url, timeout=30)
        response.raise_for_status()
        frame = pd.read_csv(StringIO(response.text))
    else:
        frame = pd.read_csv(path_or_url)

    if symbol:
        symbol_frame = _rename_market_columns(frame.copy())
        if "symbol" in symbol_frame.columns:
            symbol_frame["symbol"] = symbol_frame["symbol"].astype(str).str.upper()
            frame = symbol_frame[symbol_frame["symbol"] == symbol.upper()]
        if frame.empty:
            raise RuntimeError(f"No rows found for symbol '{symbol}'.")

    return normalize_external_frame(
        frame,
        default_symbol=symbol,
        company_name=company_name,
        sector=sector,
        source_label="bhavcopy_csv",
    )


def clean_stock_data(frame: pd.DataFrame) -> pd.DataFrame:
    cleaned = frame.copy()
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    for column in ["open", "high", "low", "close"]:
        cleaned.loc[cleaned[column] <= 0, column] = np.nan

    cleaned = cleaned.sort_values(["symbol", "date"]).reset_index(drop=True)
    cleaned[numeric_columns] = cleaned.groupby("symbol")[numeric_columns].transform(
        lambda series: series.ffill().bfill()
    )
    cleaned = cleaned.dropna(subset=["date", "open", "high", "low", "close"])
    cleaned["volume"] = cleaned["volume"].fillna(cleaned.groupby("symbol")["volume"].transform("median"))
    cleaned["volume"] = cleaned["volume"].round().astype(int)
    return cleaned


def add_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    grouped = enriched.groupby("symbol", group_keys=False)
    enriched["daily_return"] = ((enriched["close"] - enriched["open"]) / enriched["open"]).round(6)
    enriched["moving_average_7d"] = grouped["close"].transform(lambda s: s.rolling(7, min_periods=1).mean())
    enriched["rolling_52w_high"] = grouped["close"].transform(lambda s: s.rolling(252, min_periods=1).max())
    enriched["rolling_52w_low"] = grouped["close"].transform(lambda s: s.rolling(252, min_periods=1).min())
    rolling_volatility = grouped["daily_return"].transform(lambda s: s.rolling(14, min_periods=5).std())
    enriched["volatility_score"] = (rolling_volatility.fillna(0) * math.sqrt(252) * 100).round(4)
    return enriched.round(
        {
            "open": 2,
            "high": 2,
            "low": 2,
            "close": 2,
            "moving_average_7d": 2,
            "rolling_52w_high": 2,
            "rolling_52w_low": 2,
        }
    )


def save_company_and_prices(company_record: dict, frame: pd.DataFrame) -> None:
    connection = get_connection()
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS companies (
                symbol TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                sector TEXT NOT NULL,
                base_price REAL NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_prices (
                date TEXT,
                symbol TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                daily_return REAL,
                moving_average_7d REAL,
                rolling_52w_high REAL,
                rolling_52w_low REAL,
                volatility_score REAL,
                sentiment_index REAL,
                data_source TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO companies(symbol, name, sector, base_price)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = excluded.name,
                sector = excluded.sector,
                base_price = excluded.base_price
            """,
            (
                company_record["symbol"],
                company_record["name"],
                company_record["sector"],
                company_record["base_price"],
            ),
        )
        connection.execute("DELETE FROM stock_prices WHERE symbol = ?", (company_record["symbol"],))
        frame.to_sql("stock_prices", connection, if_exists="append", index=False)
        connection.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbol_date ON stock_prices(symbol, date)")
        connection.commit()
    finally:
        connection.close()


def seed_database(force: bool = False) -> None:
    connection = get_connection()
    try:
        if not force:
            existing = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_prices'"
            ).fetchone()
            if existing:
                columns = {
                    row["name"] for row in connection.execute("PRAGMA table_info(stock_prices)").fetchall()
                }
                required = {
                    "date",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "daily_return",
                    "moving_average_7d",
                    "rolling_52w_high",
                    "rolling_52w_low",
                    "volatility_score",
                    "sentiment_index",
                    "data_source",
                }
                count = connection.execute("SELECT COUNT(*) AS count FROM stock_prices").fetchone()["count"]
                if count > 0 and required.issubset(columns):
                    return

        raw = generate_stock_data()
        cleaned = clean_stock_data(raw)
        enriched = add_metrics(cleaned)
        enriched["sentiment_index"] = (
            np.clip(
                50
                + (enriched["daily_return"] * 850)
                - (enriched["volatility_score"] * 0.65)
                + ((enriched["close"] - enriched["moving_average_7d"]) / enriched["moving_average_7d"]) * 120,
                0,
                100,
            )
        ).round(2)
        enriched["data_source"] = "mock_seed"
        companies = pd.DataFrame(COMPANIES)

        companies.to_sql("companies", connection, if_exists="replace", index=False)
        enriched.to_sql("stock_prices", connection, if_exists="replace", index=False)
        connection.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbol_date ON stock_prices(symbol, date)")
        connection.commit()
    finally:
        connection.close()
