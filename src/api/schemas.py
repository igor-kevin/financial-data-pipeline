from pydantic import BaseModel
from datetime import date
from typing import Optional

class TickerMetric(BaseModel):
    date: date
    ticker: str
    close_price: float
    daily_return: Optional[float] = None
    retorno_30d: Optional[float] = None
    retorno_180d: Optional[float] = None
    retorno_365d: Optional[float] = None
    pct_do_cdi_30d: Optional[float] = None
    pct_do_cdi_180d: Optional[float] = None
    pct_do_cdi_365d: Optional[float] = None
    drawdown: Optional[float] = None


class TickerCompare(BaseModel):
    date: date
    ticker: str
    retorno_365d: Optional[float] = None
    cdi_365d: Optional[float] = None
    ibov_365d: Optional[float] = None


class TickerResume(BaseModel):
    ticker: str
    pct_do_cdi_365d: Optional[float] = None
    drawdown: Optional[float] = None
    retorno_30d: Optional[float] = None
    retorno_365d: Optional[float] = None
    close_price: Optional[float] = None