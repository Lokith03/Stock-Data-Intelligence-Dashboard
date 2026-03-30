from __future__ import annotations

import pandas as pd

from app.database import get_connection


def fetch_companies() -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT symbol, name, sector, base_price FROM companies ORDER BY symbol"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def fetch_stock_data(symbol: str, days: int = 30) -> list[dict]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT date, open, high, low, close, volume, daily_return,
                   moving_average_7d, rolling_52w_high, rolling_52w_low, volatility_score,
                   sentiment_index, data_source
            FROM stock_prices
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol.upper(), days),
        ).fetchall()
        return [dict(row) for row in reversed(rows)]
    finally:
        connection.close()


def fetch_summary(symbol: str) -> dict | None:
    connection = get_connection()
    try:
        row = connection.execute(
            """
            WITH latest AS (
                SELECT sp.symbol, c.name AS company_name, sp.close AS last_close, sp.volatility_score
                FROM stock_prices sp
                JOIN companies c ON c.symbol = sp.symbol
                WHERE sp.symbol = ?
                ORDER BY sp.date DESC
                LIMIT 1
            )
            SELECT
                latest.symbol,
                latest.company_name,
                latest.last_close,
                ROUND(AVG(sp.close), 2) AS average_close,
                ROUND(MAX(sp.close), 2) AS week_52_high,
                ROUND(MIN(sp.close), 2) AS week_52_low,
                ROUND(AVG(sp.daily_return), 6) AS average_daily_return,
                latest.volatility_score,
                ROUND(AVG(sp.sentiment_index), 2) AS average_sentiment
            FROM stock_prices sp
            JOIN latest ON latest.symbol = sp.symbol
            WHERE sp.symbol = ?
            """,
            (symbol.upper(), symbol.upper()),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def fetch_compare(symbol1: str, symbol2: str, days: int = 90) -> dict | None:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            SELECT symbol, date, close, daily_return
            FROM stock_prices
            WHERE symbol IN (?, ?)
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol1.upper(), symbol2.upper(), days * 2 + 40),
        ).fetchall()
        if not rows:
            return None

        frame = pd.DataFrame([dict(row) for row in rows])
        if frame.empty:
            return None

        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.sort_values("date")
        filtered = frame.groupby("symbol").tail(days).copy()
        if set(filtered["symbol"].unique()) != {symbol1.upper(), symbol2.upper()}:
            return None

        aligned_close = filtered.pivot(index="date", columns="symbol", values="close").dropna()
        aligned_return = filtered.pivot(index="date", columns="symbol", values="daily_return").dropna()
        if aligned_close.empty or aligned_return.empty:
            return None

        correlation = (
            float(aligned_return[symbol1.upper()].corr(aligned_return[symbol2.upper()]))
            if len(aligned_return) > 2
            else 0.0
        )

        result: dict[str, dict | float | int | str] = {}
        for symbol in [symbol1.upper(), symbol2.upper()]:
            data = aligned_close[[symbol]].dropna().reset_index()
            first_close = float(data.iloc[0][symbol])
            last_close = float(data.iloc[-1][symbol])
            source_slice = filtered[filtered["symbol"] == symbol].copy()
            result[symbol.lower()] = {
                "symbol": symbol,
                "normalized_change_pct": round(((last_close - first_close) / first_close) * 100, 2),
                "average_close": round(float(source_slice["close"].mean()), 2),
                "average_daily_return": round(float(source_slice["daily_return"].mean()), 6),
            }

        winner = result[symbol1.lower()]["symbol"]
        if result[symbol2.lower()]["normalized_change_pct"] > result[symbol1.lower()]["normalized_change_pct"]:
            winner = result[symbol2.lower()]["symbol"]

        return {
            "symbol1": result[symbol1.lower()],
            "symbol2": result[symbol2.lower()],
            "correlation": round(correlation, 4),
            "winner": winner,
            "compared_days": int(min(days, len(filtered[filtered["symbol"] == symbol1.upper()]))),
            "aligned_dates": int(len(aligned_close)),
        }
    finally:
        connection.close()


def fetch_market_movers(limit: int = 3) -> tuple[list[dict], list[dict]]:
    connection = get_connection()
    try:
        rows = connection.execute(
            """
            WITH ranked AS (
                SELECT
                    c.symbol,
                    c.name AS company_name,
                    sp.close AS last_close,
                    sp.daily_return,
                    sp.volatility_score,
                    ROW_NUMBER() OVER (PARTITION BY c.symbol ORDER BY sp.date DESC) AS row_num
                FROM companies c
                JOIN stock_prices sp ON sp.symbol = c.symbol
            ),
            latest AS (
                SELECT symbol, company_name, last_close, daily_return, volatility_score
                FROM ranked
                WHERE row_num = 1
            ),
            stats AS (
                SELECT
                    symbol,
                    ROUND(AVG(close), 2) AS average_close,
                    ROUND(MAX(close), 2) AS week_52_high,
                    ROUND(MIN(close), 2) AS week_52_low
                FROM stock_prices
                GROUP BY symbol
            )
            SELECT
                latest.symbol,
                latest.company_name,
                latest.last_close,
                stats.average_close,
                stats.week_52_high,
                stats.week_52_low,
                ROUND(latest.daily_return, 6) AS average_daily_return,
                latest.volatility_score,
                ROUND(sentiment_stats.average_sentiment, 2) AS average_sentiment
            FROM latest
            JOIN stats ON stats.symbol = latest.symbol
            JOIN (
                SELECT symbol, AVG(sentiment_index) AS average_sentiment
                FROM stock_prices
                GROUP BY symbol
            ) AS sentiment_stats ON sentiment_stats.symbol = latest.symbol
            ORDER BY latest.daily_return DESC
            """
        ).fetchall()
        items = [dict(row) for row in rows]
        gainers = items[:limit]
        losers = list(reversed(items[-limit:]))
        return gainers, losers
    finally:
        connection.close()
