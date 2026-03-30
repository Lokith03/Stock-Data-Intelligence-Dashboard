from __future__ import annotations

from typing import List

from pydantic import BaseModel


class Company(BaseModel):
    symbol: str
    name: str
    sector: str
    base_price: float


class StockPoint(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    daily_return: float
    moving_average_7d: float
    rolling_52w_high: float
    rolling_52w_low: float
    volatility_score: float
    sentiment_index: float
    data_source: str


class Summary(BaseModel):
    symbol: str
    company_name: str
    last_close: float
    average_close: float
    week_52_high: float
    week_52_low: float
    average_daily_return: float
    volatility_score: float
    average_sentiment: float


class ComparisonSeries(BaseModel):
    symbol: str
    normalized_change_pct: float
    average_close: float
    average_daily_return: float


class ComparisonResponse(BaseModel):
    symbol1: ComparisonSeries
    symbol2: ComparisonSeries
    correlation: float
    winner: str
    compared_days: int
    aligned_dates: int


class DashboardPayload(BaseModel):
    companies: List[Company]
    top_gainers: List[Summary]
    top_losers: List[Summary]
